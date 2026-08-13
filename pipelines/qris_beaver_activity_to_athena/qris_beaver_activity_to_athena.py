"""
Generic Pattern used for other scrapers we can now start to generalize:
  * Search Data Exchange for matching project
  * Get files from the project
  * Process files
  * Upload to Athena (considering Iceberg instead of S3)

Specific:
 * criteria for matching projects - in this case  type "Riverscapes Studio" and tag "beaver_activity", 3 specific orgs
 * the specific files and processing - from project.rs.xml, find the specific beaver_dam vector layer (expect named vw_beaver_dam_*), geopackage, table
 * the location of files of name of the table - rs_raw.qris_beaver_activity

Unlike RME, I do *not* assume there is only one valid project per HUC10

Layer extraction uses GeoPandas to read the beaver_dam vector layer directly from the GeoPackage.

Lorin Gaertner
August 2026
"""

import argparse
import logging
import re
import uuid
from pathlib import Path

import boto3
import geopandas as gpd
import lxml.etree as etree
import pandas as pd
from rsxml import Logger, ProgressBar, dotenv
from rsxml.util import safe_makedirs

from pydex import RiverscapesAPI
from pydex.lib.athena import athena_execute, query_to_dataframe

TARGET_TABLE: str = 'rs_raw.qris_beaver_activity'
ICEBERG_LOCATION: str = 's3://riverscapes-athena/rs_raw/qris_beaver_activity/'
SYNC_SOURCE_TABLE: str = 'dev_riverscapes.qris_beaver_activity_sync_source'
SYNC_SOURCE_PREFIX_BASE: str = 'dev-test/rs_raw_sync_source/qris_beaver_activity'


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse an S3 URI into (bucket, key_prefix)."""
    if not s3_uri.startswith('s3://'):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    path = s3_uri[5:]
    bucket, _, key = path.partition('/')
    return bucket, key


def stage_feature_gdf_to_sync_source(feature_gdf: gpd.GeoDataFrame, working_folder: Path) -> tuple[str, str, str]:
    """Write feature_gdf as Parquet with WKB geometry, upload to S3, and create Athena sync source table."""
    log = Logger('Sync Source QRiS Features')

    required_columns = [
        'project_id',
        'created_on',
        'updated_on',
        'dam_cer',
        'dam_type',
        'type_cer',
        'realization_name',
        'realization_description',
        'metadata_census_date',
        'parsed_survey_year',
    ]

    stage_df = pd.DataFrame(feature_gdf.drop(columns=['geometry'], errors='ignore')).copy()
    for column_name in required_columns:
        if column_name not in stage_df.columns:
            stage_df[column_name] = pd.NA

    if 'geometry' in feature_gdf.columns:
        stage_df['geom_wkb'] = feature_gdf.geometry.to_wkb()
    else:
        stage_df['geom_wkb'] = pd.NA

    stage_df = stage_df[[*required_columns, 'geom_wkb']]

    local_sync_source_path = working_folder / 'qris_beaver_activity_sync_source.parquet'
    stage_df.to_parquet(local_sync_source_path)

    iceberg_bucket, _ = parse_s3_uri(ICEBERG_LOCATION)
    run_id = uuid.uuid4()
    sync_source_prefix = f'{SYNC_SOURCE_PREFIX_BASE}/{run_id}/'
    sync_source_key = f'{sync_source_prefix}qris_beaver_activity_sync_source.parquet'

    s3 = boto3.client('s3')
    s3.upload_file(str(local_sync_source_path), iceberg_bucket, sync_source_key)
    log.info(f"Uploaded sync source parquet to s3://{iceberg_bucket}/{sync_source_key}")

    drop_sql = f"DROP TABLE IF EXISTS {SYNC_SOURCE_TABLE}"
    create_sync_source_sql = f"""
CREATE EXTERNAL TABLE {SYNC_SOURCE_TABLE} (
    project_id STRING,
    created_on BIGINT,
    updated_on BIGINT,
    dam_cer STRING,
    dam_type STRING,
    type_cer STRING,
    realization_name STRING,
    realization_description STRING,
    metadata_census_date STRING,
    parsed_survey_year STRING,
    geom_wkb BINARY
)
STORED AS PARQUET
LOCATION 's3://{iceberg_bucket}/{sync_source_prefix}'
"""

    if not athena_execute(iceberg_bucket, drop_sql):
        raise RuntimeError(f"Could not drop sync source table {SYNC_SOURCE_TABLE}")
    if not athena_execute(iceberg_bucket, create_sync_source_sql):
        raise RuntimeError(f"Could not create sync source table {SYNC_SOURCE_TABLE}")

    return SYNC_SOURCE_TABLE, iceberg_bucket, sync_source_prefix


def delete_s3_prefix(s3_bucket: str, prefix: str) -> int:
    """Delete all S3 objects under a prefix and return count deleted."""
    s3 = boto3.client('s3')
    deleted = 0
    continuation_token = None

    while True:
        kwargs = {'Bucket': s3_bucket, 'Prefix': prefix}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**kwargs)
        objects = response.get('Contents', [])
        if objects:
            for i in range(0, len(objects), 1000):
                chunk = objects[i : i + 1000]
                keys = [{'Key': obj['Key']} for obj in chunk]
                s3.delete_objects(Bucket=s3_bucket, Delete={'Objects': keys})
                deleted += len(keys)
        if not response.get('IsTruncated'):
            break
        continuation_token = response.get('NextContinuationToken')

    return deleted


def get_projects():
    """Return projects to be upserted into Athena"""
    # TODO: this is all projects matching criteria, makes no check for what is already there
    sql = """
SELECT project_id,
       huc,
       name,
       model_version,
       model_version_int,
       created_on,
       created_on_date,
       updated_on,
       updated_on_date,
       owner
FROM default.data_exchange_projects
WHERE project_type_id = 'riverscapesstudio'
  AND owner IN ('a52b8094-7a1d-4171-955c-ad30ae935296', -- USU RAM
                '4a49c97e-7ce2-4c66-8ffe-ed41a675a115', -- Bonneville Environmental Foundation
                'f0f6a9e7-f102-4066-9265-2d29ec1c467a' -- Defenders of Wildlife
    )
  AND (contains(tags, 'beaver_activity'))
  LIMIT 10
  """
    projects_to_add_df = query_to_dataframe(sql, 'identify new projects')
    return projects_to_add_df


def download_projectrsxml(rs_api: RiverscapesAPI, project_id: str, download_dir: Path) -> Path:
    """download project.rs.xml and return its file path (Error if not found)"""
    return rs_api.download_project_file(project_id, 'project.rs.xml', download_dir)


def beaver_dam_layers(projectxmlpath: Path) -> list[dict[str, str | dict[str, str]]]:
    """Parse project XML and return all Vector nodes with ``type='beaver_dam'``.

    Each returned item contains:
    - ``lyrName``
    - ``geopackage_path``
    - ``realization_id``
    - ``realization_name``
    - ``realization_description``
    - ``realization_metadata`` (name->value dictionary)
    """
    # Parse bytes so XML declarations like <?xml version="1.0" encoding="UTF-8"?> are valid.
    root = etree.fromstring(projectxmlpath.read_bytes())

    results: list[dict[str, str | dict[str, str]]] = []

    for realization in root.findall('.//Realizations/Realization'):
        realization_id = realization.get('id', '')
        realization_name = (realization.findtext('Name') or '').strip()
        realization_description = (realization.findtext('Description') or '').strip()

        realization_metadata: dict[str, str] = {}
        for meta in realization.findall('MetaData/Meta'):
            meta_name = meta.get('name')
            if meta_name:
                realization_metadata[meta_name] = (meta.text or '').strip()

        geopackages = realization.findall('.//Geopackage')
        for geopackage in geopackages:
            geopackage_path = (geopackage.findtext('Path') or '').strip()
            for vector in geopackage.findall('.//Vector'):
                if vector.get('type') != 'beaver_dam':
                    continue
                results.append(
                    {
                        'lyrName': vector.get('lyrName', ''),
                        'geopackage_path': geopackage_path,
                        'realization_id': realization_id,
                        'realization_name': realization_name,
                        'realization_description': realization_description,
                        'realization_metadata': realization_metadata,
                    }
                )

    return results


def extract_metrics_to_geodataframe(gpkg_path: Path, layer_name: str) -> gpd.GeoDataFrame:
    """Read a layer from the GeoPackage and return standardized beaver activity fields."""
    log = Logger('Extract Layer')
    gdf = gpd.read_file(gpkg_path, layer=layer_name)

    def _normalize(name: str) -> str:
        return ''.join(ch for ch in name.lower() if ch.isalnum())

    aliases = {
        'dam_cer': ['Dam CER', 'DAM CER', 'dam_cer', 'dam cer', 'damcer'],
        'dam_type': ['DAM Type', 'Dam Type', 'dam_type', 'dam type', 'damtype'],
        'type_cer': ['Type CER', 'TYPE CER', 'type_cer', 'type cer', 'typecer'],
    }
    required_fields = {'dam_cer', 'dam_type'}

    normalized_columns = {_normalize(col): col for col in gdf.columns}
    resolved_columns: dict[str, str] = {}

    for canonical_name, options in aliases.items():
        match = next((normalized_columns.get(_normalize(option)) for option in options), None)
        if not match:
            if canonical_name in required_fields:
                raise ValueError(f"Could not find expected field for '{canonical_name}' in layer '{layer_name}'. Available fields: {', '.join(map(str, gdf.columns))}")
            log.warning(f"Optional field '{canonical_name}' not found in layer '{layer_name}'. Writing nulls for this field.")
            continue
        resolved_columns[canonical_name] = match

    if 'geometry' not in gdf.columns:
        raise ValueError(f"Layer '{layer_name}' has no geometry column.")

    selected_columns = [resolved_columns['dam_cer'], resolved_columns['dam_type'], 'geometry']
    if 'type_cer' in resolved_columns:
        selected_columns.insert(2, resolved_columns['type_cer'])

    extracted = gdf[selected_columns].copy()
    rename_map = {
        resolved_columns['dam_cer']: 'dam_cer',
        resolved_columns['dam_type']: 'dam_type',
    }
    if 'type_cer' in resolved_columns:
        rename_map[resolved_columns['type_cer']] = 'type_cer'

    extracted = extracted.rename(columns=rename_map)
    if 'type_cer' not in extracted.columns:
        extracted['type_cer'] = pd.NA
    extracted = gpd.GeoDataFrame(extracted, geometry='geometry', crs=gdf.crs)
    log.debug(f"Extracted {len(extracted)} beaver dam rows from layer '{layer_name}' in {gpkg_path}")
    return extracted


def process_layer(rs_api: RiverscapesAPI, layer_info: dict, download_dir: Path) -> gpd.GeoDataFrame:
    """Download the geopackage, get the layer, build a dataframe"""
    log = Logger('Process Layer')
    geopackage_path = str(layer_info.get('geopackage_path', ''))
    project_id = str(layer_info.get('project_id', ''))
    layer_name = str(layer_info.get('lyrName', ''))
    log.debug(f"Processing {layer_name} in {geopackage_path}")
    gpkg_path = rs_api.download_project_file(project_id, geopackage_path, download_dir)
    layer_gdf = extract_metrics_to_geodataframe(gpkg_path, layer_name)
    return layer_gdf


def get_year_from_meta_or_realiz(layer_row: dict) -> str:
    """
    If the meta has CensusDate and it's numeric or a range such as 2016-2019, extract that
    If realization_name contains a year such NAIP_2024 extract that
    If only one matches, or they agree, return the value. Otherwise return 'unclear'
    TODO: Confirm this business logic with Jordan
    """

    def _parse_years_from_string(candidate: str) -> str:
        """Extract 4-digit year or pair of years such as 2013-2016 in string.
        Anything else returns empty string.
        Examples:
        '2024Census' -> '2024'
        'NAIP_2022' -> '2022'
        '2013-2016 ' -> '2013-2016'
        'no specified date range' -> ''
        '' -> ''
        '1706020502' -> ''
        """
        text = str(candidate or '').strip()
        if not text:
            return ''

        # Prefer explicit ranges first so "2013-2016" does not collapse to a single year.
        range_match = re.search(r'(?<!\d)(\d{4})\s*-\s*(\d{4})(?!\d)', text)
        if range_match:
            return f"{range_match.group(1)}-{range_match.group(2)}"

        # Match standalone 4-digit years, avoiding partial matches in longer digit runs.
        year_match = re.search(r'(?<!\d)(\d{4})(?!\d)', text)
        if year_match:
            return year_match.group(1)
        return ''

    metadata = layer_row.get('realization_metadata', {})
    census_date_raw = metadata.get('CensusDate', '') if isinstance(metadata, dict) else ''

    census_date_years = _parse_years_from_string(census_date_raw)
    realization_name_years = _parse_years_from_string(layer_row.get('realization_name', ''))
    if (census_date_years and not realization_name_years) or census_date_years == realization_name_years:
        return census_date_years
    if realization_name_years and not census_date_years:
        return realization_name_years
    return 'unclear'


def scrape_projects(rs_api: RiverscapesAPI, download_dir: Path, keep_sync_source: bool = False):
    """orchestrate scraping of projects"""
    log = Logger('Scrape Projects')
    projects = get_projects()
    if projects.empty:
        log.info("Query to identify projects to scrape returned no results.")
        return
    log.info(f"Query to identify projects to scrape returned {len(projects)} projects.")

    count = 0
    errors = 0
    all_rows: list[dict[str, object | None]] = []
    feature_frames: list[gpd.GeoDataFrame] = []
    prg = ProgressBar(projects.shape[0], text="Scrape Progress")
    for project_row in projects.itertuples(index=False):
        project_id = str(project_row.project_id)
        project_name = str(project_row.name)
        created_on_raw = project_row.created_on
        updated_on_raw = project_row.updated_on
        created_on = 0
        updated_on = 0
        if pd.notna(created_on_raw):
            try:
                created_on = int(float(str(created_on_raw)))
            except (TypeError, ValueError):
                log.warning(f"Could not parse created_on for {project_name} ({project_id}): {created_on_raw}")
        if pd.notna(updated_on_raw):
            try:
                updated_on = int(float(str(updated_on_raw)))
            except (TypeError, ValueError):
                log.warning(f"Could not parse updated_on for {project_name} ({project_id}): {updated_on_raw}")
        log.debug(f"Scraping {project_name} ({project_id})")
        try:
            # get the project.rs.xml and parse realizations
            project_download_dir = download_dir / project_id
            projectrsxml_path = download_projectrsxml(rs_api, project_id, project_download_dir)
            layers = beaver_dam_layers(projectrsxml_path)
            if len(layers) == 0:
                log.warning("No matching Vector layer found.")
            for layer in layers:
                row: dict[str, object | None] = {'project_id': project_id, 'project_name': project_name, 'created_on': created_on, 'updated_on': updated_on}
                row.update(layer)
                all_rows.append(row)
                layer_gdf = process_layer(rs_api, row, project_download_dir)
                if layer_gdf.empty:
                    continue
                layer_gdf = layer_gdf.copy()
                layer_gdf['project_id'] = project_id
                # layer_gdf['project_name'] = project_name # this can be derived from project_id joining to projects table in Athena, but could keep it for convenience
                # layer_gdf['lyrName'] = str(row.get('lyrName', '')) # values are vw_beaver_dam_1 or vw_beaver_dam_2 or vw_beaver_dam_event_1_event_layer_1
                # layer_gdf['geopackage_path'] = str(row.get('geopackage_path', '')) # this is internal info not useful, also ALL values are qris.gpkg
                # layer_gdf['realization_id'] = str(row.get('realization_id', '')) # all values are realization_qris_1 or realization_qris_2 . is this useful?
                layer_gdf['realization_name'] = str(row.get('realization_name', ''))
                layer_gdf['realization_description'] = str(row.get('realization_description', ''))
                # Python is good for string matching, logic etc, but we'll provide the raw data in case someone wants to extract it differently
                parsed_survey_year = get_year_from_meta_or_realiz(row)
                layer_gdf['parsed_survey_year'] = parsed_survey_year
                metadata_value = row.get('realization_metadata')
                # only care about the CensusDate metadatavalue, not the other two
                census_date_raw = metadata_value.get('CensusDate', '') if isinstance(metadata_value, dict) else ''
                layer_gdf['metadata_census_date'] = census_date_raw
                created_on_value = row.get('created_on', 0)
                updated_on_value = row.get('updated_on', 0)
                layer_gdf['created_on'] = int(created_on_value) if isinstance(created_on_value, (int, float, str)) else 0
                layer_gdf['updated_on'] = int(updated_on_value) if isinstance(updated_on_value, (int, float, str)) else 0
                feature_frames.append(layer_gdf)
            count += 1
            prg.update(count + errors)

        except Exception as e:
            errors += 1
            log.error(f'Error scraping {project_name} ({project_id}): {e}')
            prg.update(count + errors)
            # raise

    prg.finish()

    output_columns = [
        'project_id',
        'project_name',
        'created_on',
        'updated_on',
        'lyrName',
        'geopackage_path',
        'realization_id',
        'realization_name',
        'realization_description',
        'realization_metadata',
    ]
    results_df = pd.DataFrame(all_rows)
    if results_df.empty:
        results_df = pd.DataFrame(columns=output_columns)

    output_path = download_dir.parent / 'qris_beaver_dam_layers.parquet'
    results_df.to_parquet(output_path)

    feature_output_path = download_dir.parent / 'qris_beaver_activity.parquet'
    if feature_frames:
        feature_df = pd.concat(feature_frames, ignore_index=True)
        feature_gdf = gpd.GeoDataFrame(feature_df, geometry='geometry', crs=feature_frames[0].crs)
    else:
        feature_gdf = gpd.GeoDataFrame(columns=['dam_cer', 'dam_type', 'type_cer', 'parsed_survey_year', 'geometry'], geometry='geometry', crs='EPSG:4326')
    feature_gdf.to_parquet(feature_output_path)

    if not feature_gdf.empty:
        sync_source_relation = ''
        iceberg_bucket, _ = parse_s3_uri(ICEBERG_LOCATION)
        sync_source_prefix = ''
        load_succeeded = False
        try:
            sync_source_relation, iceberg_bucket, sync_source_prefix = stage_feature_gdf_to_sync_source(feature_gdf, download_dir.parent)
            load_succeeded = create_iceberg_table_and_initial_load(iceberg_bucket, sync_source_relation)
            if not load_succeeded:
                log.error("Iceberg initial load failed.")
        except Exception as e:
            log.error(f"Error loading QRiS beaver activity to Iceberg: {e}")
        finally:
            if sync_source_relation and keep_sync_source:
                log.info(f"Keeping sync source artifacts: table {SYNC_SOURCE_TABLE} and s3://{iceberg_bucket}/{sync_source_prefix}")
            elif sync_source_relation:
                if athena_execute(iceberg_bucket, f"DROP TABLE IF EXISTS {SYNC_SOURCE_TABLE}"):
                    log.info(f"Dropped sync source table {SYNC_SOURCE_TABLE}")
                else:
                    log.warning(f"Could not drop sync source table {SYNC_SOURCE_TABLE}")
                if sync_source_prefix:
                    deleted_count = delete_s3_prefix(iceberg_bucket, sync_source_prefix)
                    log.info(f"Deleted {deleted_count} sync source S3 objects from s3://{iceberg_bucket}/{sync_source_prefix}")
    else:
        log.info('Skipping Iceberg load because feature dataframe is empty.')

    log.info(f'Wrote {len(results_df)} beaver_dam layer rows to {output_path}')
    log.info(f'Wrote {len(feature_gdf)} beaver activity feature rows to {feature_output_path}')
    log.info(f'Processed {count} projects successfully and {errors} failed.')


def create_iceberg_table_and_initial_load(
    s3_bucket: str,
    source_table_or_view: str,
) -> bool:
    """Draft helper for first-time Iceberg load from an Athena source table/view.

    This creates the target Iceberg table if needed and inserts all rows from
    ``source_table_or_view``. The source relation is expected to expose these
    columns: project_id, created_on, updated_on, dam_cer, dam_type, type_cer,
    realization_name, realization_description, parsed_survey_year,
    metadata_census_date, geom_wkb.

    Geometry is persisted as WKB in ``geom_wkb`` (BINARY).
    """
    log = Logger('Iceberg Initial Load')

    create_sql = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    project_id STRING,
    project_created_on BIGINT,
    project_updated_on BIGINT,
    dam_cer STRING,
    dam_type STRING,
    type_cer STRING,
    realization_name STRING,
    realization_description STRING,
    metadata_census_date STRING,
    parsed_survey_year STRING,
    geom_wkb BINARY
)
LOCATION '{ICEBERG_LOCATION}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='PARQUET'
)
"""

    insert_sql = f"""
INSERT INTO {TARGET_TABLE}
SELECT
    CAST(project_id AS VARCHAR) AS project_id,
    CAST(created_on AS BIGINT) AS project_created_on,
    CAST(updated_on AS BIGINT) AS project_updated_on,
    CAST(dam_cer AS VARCHAR) AS dam_cer,
    CAST(dam_type AS VARCHAR) AS dam_type,
    CAST(type_cer AS VARCHAR) AS type_cer,
    CAST(realization_name AS VARCHAR) AS realization_name,
    CAST(realization_description AS VARCHAR) AS realization_description,
    CAST(metadata_census_date AS VARCHAR) AS metadata_census_date,
    CAST(parsed_survey_year AS VARCHAR) AS parsed_survey_year,
    geom_wkb
FROM {source_table_or_view}
"""

    log.info(f"Creating Iceberg table if needed: {TARGET_TABLE}")
    if not athena_execute(s3_bucket, create_sql):
        log.error(f"Failed creating Iceberg table {TARGET_TABLE}")
        return False

    log.info(f"Running initial load into {TARGET_TABLE} from {source_table_or_view}")
    if not athena_execute(s3_bucket, insert_sql):
        log.error(f"Failed initial insert into {TARGET_TABLE}")
        return False

    log.info(f"Initial Iceberg load completed for {TARGET_TABLE}")
    return True


def main():
    """Process arguments, set up logs and orchestrate call to other functions"""
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('--keep-sync-source', help='Keep sync source table and sync source S3 objects for debugging', action='store_true', default=False)
    # parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = Path(args.working_folder)
    download_folder = working_folder / 'downloads'

    safe_makedirs(str(working_folder))

    log = Logger('Setup')
    log.setup(log_path=working_folder / 'qris_beaver_activity_to_athena.log', log_level=logging.DEBUG)

    log.title("Scrape QRiS Projects Beaver Activity to Athena")

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_projects(api, download_folder, keep_sync_source=args.keep_sync_source)

    log.info('Process complete')


if __name__ == '__main__':
    main()
