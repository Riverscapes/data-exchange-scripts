"""
Searches Data Exchange for RME projects matching criteria
Generates parquet files from metrics in the GeoPackages
Uploads to s3

Lorin Gaertner
Sept 2025
Enhances Philip's June 2025 rme_to_athena.py
"""
import argparse
import json
import logging
import os
from pathlib import Path
import re
import shutil
import warnings

import apsw
import boto3
import geopandas as gpd
import pandas as pd
from shapely import wkb
from semver import Version

from rsxml.util import safe_makedirs
from rsxml import dotenv, Logger, ProgressBar

from pydex import RiverscapesAPI, RiverscapesProject
from pydex.lib.athena import query_to_dataframe

# Environment-configurable buckets. These represent stable infrastructure and
# should not vary run-to-run, so we prefer environment variables over CLI args.
DATA_BUCKET_ENV_VAR = "RME_DATA_BUCKET"
OUTPUT_BUCKET_ENV_VAR = "RME_ATHENA_OUTPUT_BUCKET"
DEFAULT_DATA_BUCKET = "riverscapes-athena"

DATA_BUCKET = os.getenv(DATA_BUCKET_ENV_VAR, DEFAULT_DATA_BUCKET)
ATHENA_OUTPUT_BUCKET = os.getenv(OUTPUT_BUCKET_ENV_VAR, DATA_BUCKET)  # fallback to data bucket if not set

# Query to identify projects to add/replace. No semicolon allowed.
missing_projects_query = """
with huc_projects_dex as
         (select project_id,
                 huc,
                 created_on
          from vw_projects
          WHERE project_type_id = 'rs_metric_engine'
            and owner = 'a52b8094-7a1d-4171-955c-ad30ae935296'
            AND created_on >= 1735689600
            AND (contains(tags, '2025CONUS')
              OR contains(tags, 'conus_athena'))),
    huc_projects_scraped as
        (select substr(huc12, 1, 10) as huc10,
                raw_rme_pq2.rme_date_created_ts
         from raw_rme_pq2)
select distinct project_id, huc, created_on, rme_date_created_ts
from huc_projects_dex dex
    left join huc_projects_scraped scr on dex.huc = scr.huc10
where scr.huc10 is null
   or scr.rme_date_created_ts < truncate(dex.created_on/1000)*1000
"""


def semver_to_int(version: Version) -> int:
    """convert to integer for easier comparisons

    Args:
        version (Version): semver Version e.g. 3.1.5

    Returns:
        int: integerpresentation e.g. 3001005
    """
    MAJOR = 1000000
    MINOR = 1000
    return version.major * MAJOR + version.minor * MINOR + version.patch


def download_file(rs_api: RiverscapesAPI, project_id: str, download_dir: str, regex: str) -> str:
    """
    Download files from a project on Data Exchange that match the regex string
    Return the path to the downloaded file
    """
    # check if it has previously been downloaded
    log = Logger('download RS DEX file')
    gpkg_path = get_matching_file(download_dir, regex)
    if gpkg_path is not None and os.path.isfile(gpkg_path):
        log.debug(f'file for matching {regex} previously downloaded')
        return gpkg_path

    rs_api.download_files(project_id, download_dir, [regex])

    gpkg_path = get_matching_file(download_dir, regex)
    log.debug(f'file for {project_id} downloaded to {gpkg_path}')

    # Cannot proceed with this HUC if the output GeoPackage is missing
    if gpkg_path is None or not os.path.isfile(gpkg_path):
        raise FileNotFoundError(f'Could not find output GeoPackage in {download_dir}')

    return gpkg_path


def get_matching_file(parent_dir: str, regex_str: str) -> str | None:
    """
    Get the path to the *first* file in the parent directory that matches the regex.
    Returns None if no file is found.
    This is used to check if the output GeoPackage has already been downloaded and
    to avoid downloading it again.
    """

    regex = re.compile(regex_str)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def download_rme_geopackage(
    rs_api: RiverscapesAPI,
    project: RiverscapesProject,
    huc_dir: str | Path
) -> str:
    """
    Download the RME GeoPackage for a project and return its file path.
    """
    # RegEx string for finding RME output GeoPackages
    RME_SCRAPE_GPKG_REGEX = r'.*riverscapes_metrics.gpkg'
    # NOTE: will not overwrite existing files - which can be a problem.
    rme_gpkg = download_file(rs_api, project.id, huc_dir, RME_SCRAPE_GPKG_REGEX)  # pyright: ignore[reportArgumentType]
    return rme_gpkg


def get_layer_columns_dict(layer_definitions_path: Path, layer_id: str) -> dict[str, dict]:
    """
    Load the layer_definitions.json and return a dictionary of columns and their properties for the given layer_id.
    Args:
        layer_definitions_path (Path): the Path to the json file to use
        layer_id (str): The layer_id to look up.
    Returns:
        dict[str, dict]: Dictionary mapping column names to their property dicts.
    """
    with layer_definitions_path.open('r', encoding='utf-8') as f:
        data = json.load(f)
    for layer in data.get('layers', []):
        if layer.get('layer_id') == layer_id:
            return {col['name']: col for col in layer.get('columns', [])}
    raise ValueError(f"Layer ID '{layer_id}' not found in {layer_definitions_path}")


def extract_metrics_to_geodataframe(gpkg_path: str, spatialite_path: str) -> gpd.GeoDataFrame:
    """
    Connect to the GeoPackage, run the SQL, and return a GeoDataFrame.
    """
    conn = apsw.Connection(gpkg_path)
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)

    sql = '''
        SELECT
            dgos.level_path,
            dgos.seg_distance,
            dgos.centerline_length,
            dgos.segment_area,
            dgos.FCode as fcode,
            ST_X(ST_CENTROID(castautomagic(dgos.geom))) longitude,
            ST_Y(ST_CENTROID(castautomagic(dgos.geom))) latitude,
            dgo_desc.*,
            dgo_geomorph.*,
            dgo_veg.*,
            dgo_hydro.*,
            dgo_impacts.*,
            dgo_beaver.*,
            ST_AsBinary(CastAutomagic(dgos.geom)) dgo_geom
        FROM dgo_desc
            INNER JOIN dgo_geomorph ON dgo_desc.dgoid = dgo_geomorph.dgoid
            INNER JOIN dgo_veg ON dgo_desc.dgoid = dgo_veg.dgoid
            INNER JOIN dgo_hydro ON dgo_desc.dgoid = dgo_hydro.dgoid
            INNER JOIN dgo_impacts ON dgo_desc.dgoid = dgo_impacts.dgoid
            INNER JOIN dgo_beaver ON dgo_desc.dgoid = dgo_beaver.dgoid
            INNER JOIN dgos ON dgo_desc.dgoid = dgos.dgoid
    '''
    # we need apsw / spatialite . this seems to work despite pandas not supporting it
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
        df = pd.read_sql_query(sql, conn)

    # Use the data dictionary to set column types.
    # because there are nulls, the combination of sqlites dynamic typing and pandas' type inference mis-assigns data types
    # actually the problem is that it is sometimes a double, sometimes INT64. Needs to be consistent
    try:
        # NOTE - using this one definitions file to describe both INPUT AND OUTPUT structure
        # ideally we'd take the data types from the RME data dictionary and write them (along with any changes we're making) to the raw_rme version. But that doesn't exist yet
        # Possible enhancement: check the is_required property and if TRUE then we could use a nullable integer
        columns_dict = get_layer_columns_dict(Path(__file__).parent / 'layer_definitions.json', 'raw_rme')
        for field, props in columns_dict.items():
            if props.get('dtype') == 'INTEGER' and field in df.columns:
                df[field] = df[field].astype('Int64')  # pandas nullable integer
    except Exception as e:
        raise Exception(f"Could not apply data dictionary types: {e}") from e

    # Remove all columns named 'dgoid' (case-insensitive, even if duplicated)
    df = df.loc[:, [col for col in df.columns if col.lower() != 'dgoid']]
    # convert wkb geometry to shapely objects
    df['dgo_geom'] = df['dgo_geom'].apply(wkb.loads)  # pyright: ignore[reportCallIssue, reportArgumentType]
    gdf = gpd.GeoDataFrame(df, geometry='dgo_geom', crs='EPSG:4326')

    # Reproject to EPSG:5070 for simplification
    gdf_proj = gdf.to_crs(epsg=5070)

    # Use simplify_coverage for topology-preserving simplification
    gdf["geometry_simplified"] = gdf_proj.geometry.simplify_coverage(tolerance=11)  # 11 m seems to have worked well
    # Reproject simplified geometry back to EPSG:4326
    gdf["geometry_simplified"] = gpd.GeoSeries(gdf["geometry_simplified"], crs=5070).to_crs(epsg=4326)
    gdf = gdf.set_crs(epsg=4326)
    gdf = gdf.reset_index(drop=True)

    bbox_df = gdf.geometry.bounds.rename(columns={'minx': 'xmin', 'miny': 'ymin', 'maxx': 'xmax', 'maxy': 'ymax'})
    # Combine into a struct-like dict for each row
    gdf['dgo_geom_bbox'] = bbox_df.apply(
        lambda row: {'xmin': float(row.xmin), 'ymin': float(row.ymin), 'xmax': float(row.xmax), 'ymax': float(row.ymax)},
        axis=1
    )

    return gdf


def delete_folder(dirpath: Path) -> None:
    """delete a local folder and its contents"""
    log = Logger('delete downloads')
    if dirpath.is_dir():
        try:
            log.info(f'Deleting directory {dirpath}')
            shutil.rmtree(dirpath)
            log.debug(f'removed {dirpath}')
        except Exception as e:
            log.error(f'Error deleting download directory {dirpath}: {e}')


def upload_to_s3(
        file_path: str | Path,
        s3_bucket: str,
        s3_key: str
) -> None:
    """upload a file to s3

    Args:
        file_path (str): local file path
        s3_bucket (str): s3 bucket name
        s3_key (str): s3_key (including 'folders'?)
    """
    log = Logger('upload to s3')
    s3 = boto3.client('s3')
    s3.upload_file(file_path, s3_bucket, s3_key)
    log.debug(f'file uploaded to s3 {s3_bucket} {s3_key}')


def scrape_rme(
    rs_api: RiverscapesAPI,
    spatialite_path: str,
    download_dir: str | Path,
    data_bucket: str,
    delete_downloads_when_done: bool,
) -> None:
    """
    Orchestrate the scraping, processing, and uploading of RME projects.
    """
    # 1. Get list of projects to process
    # 2. For each project:
    #    - create a folder
    #    - Download and validate
    #    - Extract metrics, geometries as GeoDataFrame
    #    - Write GeoParquet
    #    - Upload to S3
    #    - Optionally clean up

    log = Logger('Scrape RME')
    download_dir = Path(download_dir)
    # NEW WAY
    # run Athena query to find all eligible projects that are newer than what is already scraped
    # projects_to_add_df = query_to_dataframe(missing_projects_query, 'identify new projects')
    # if projects_to_add_df.empty:
    #     log.info("Query to identify projects to scrape returned no results.")
    #     return
    projects_to_add_df = pd.DataFrame({'project_id': ['5aeff0f8-5a8e-4db8-8e6c-9e507b20eca0']})
    count = 0
    prg = ProgressBar(projects_to_add_df.shape[0], text="Scrape Progress")
    for project_id in projects_to_add_df['project_id']:
        project = rs_api.get_project_full(project_id)
        if project.huc is None or project.huc == '':
            log.warning(f'Project {project.id} does not have a HUC. Skipping.')
            continue

        # this truncates to nearest second, for whatever reason
        project_created_date_ts = int(project.created_date.timestamp()) * 1000  # pyright: ignore[reportOptionalMemberAccess] Projects always have a created_date

        if project.model_version is None:
            log.warning(f'Project {project.id} does not have a model version. Skipping.')
            continue

        model_version_int = semver_to_int(project.model_version)

        try:
            huc_dir = download_dir / project.huc
            safe_makedirs(str(huc_dir))
            gpkg_path = download_rme_geopackage(rs_api, project, huc_dir)
            data_gdf = extract_metrics_to_geodataframe(gpkg_path, spatialite_path)
            # add common project-level columns
            data_gdf['rme_project_id'] = project.id
            data_gdf['rme_date_created_ts'] = project_created_date_ts
            data_gdf['rme_version'] = str(project.model_version)
            data_gdf['rme_version_int'] = model_version_int

            log.debug(f"Dataframe prepared with shape {data_gdf.shape}")
            # until we have a more robust schema check this is something
            if len(data_gdf.columns) != 135:
                log.warning(f"Expected 135 columns, got {len(data_gdf.columns)}")
            rme_pq_filepath = huc_dir / f'rme_{project.huc}.parquet'
            data_gdf.to_parquet(rme_pq_filepath)
            # do not use os.path.join because this is aws os, not system os
            s3_key = f'data_exchange/riverscape_metrics/{rme_pq_filepath.name}'
            upload_to_s3(rme_pq_filepath, data_bucket, s3_key)

            if delete_downloads_when_done:
                delete_folder(download_dir)
            count += 1
            prg.update(count)
        except Exception as e:
            log.error(f'Error scraping HUC {project.huc}: {e}')
            raise
    prg.finish()


def main():
    """Process arguments, set up logs and orchestrate call to other functions"""
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages',  action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = Path(args.working_folder)
    download_folder = working_folder / 'downloads'

    safe_makedirs(str(working_folder))

    log = Logger('Setup')
    log.setup(log_path=os.path.join(working_folder, 'rme-athena.log'), log_level=logging.DEBUG)

    log.title("rme scrape to parquet to athena")

    # Log bucket resolution
    if ATHENA_OUTPUT_BUCKET == DATA_BUCKET:
        log.warning(f"Using single bucket for data & Athena output: {DATA_BUCKET} (override with {OUTPUT_BUCKET_ENV_VAR})")
    else:
        log.info(f"Data bucket: {DATA_BUCKET} (env {DATA_BUCKET_ENV_VAR}); Athena output bucket: {ATHENA_OUTPUT_BUCKET} (env {OUTPUT_BUCKET_ENV_VAR})")

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(
            api,
            args.spatialite_path,
            download_folder,
            DATA_BUCKET,
            args.delete
        )

    log.info('Process complete')


if __name__ == '__main__':
    main()
