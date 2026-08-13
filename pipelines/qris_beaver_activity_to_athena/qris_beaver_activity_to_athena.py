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
from pathlib import Path

import geopandas as gpd
import lxml.etree as etree
import pandas as pd
from rsxml import Logger, ProgressBar, dotenv
from rsxml.util import safe_makedirs

from pydex import RiverscapesAPI
from pydex.lib.athena import query_to_dataframe


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
       owner
FROM default.vw_projects
WHERE project_type_id = 'riverscapesstudio'
  AND owner IN ('a52b8094-7a1d-4171-955c-ad30ae935296', -- USU RAM
                '4a49c97e-7ce2-4c66-8ffe-ed41a675a115', -- Bonneville Environmental Foundation
                'f0f6a9e7-f102-4066-9265-2d29ec1c467a' -- Defenders of Wildlife
    )
  AND (contains(tags, 'beaver_activity'))"""
    projects_to_add_df = query_to_dataframe(sql, 'identify new projects')
    return projects_to_add_df


def download_projectrsxml(rs_api: RiverscapesAPI, project_id: str, download_dir: Path) -> Path:
    """download project.rs.xml and return its file path (Error if not found)"""
    return rs_api.download_project_file(project_id, 'project.rs.xml', download_dir)


def beaver_dam_layers(projectxmlpath: Path) -> list[dict[str, str | dict[str, str]]]:
    """Parse project XML and return all vectors with ``type='beaver_dam'``.

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
    raise NotImplementedError


def scrape_projects(rs_api: RiverscapesAPI, download_dir: Path):
    """orchestrate scraping of projects"""
    log = Logger('Scrape Projects')
    projects = get_projects()
    if projects.empty:
        log.info("Query to identify projects to scrape returned no results.")
        return
    log.info(f"Query to identify projects to scrape returned {len(projects)} projects.")
    # test a single project
    # projects_to_add_df = pd.DataFrame({'project_id': ['756cc4e5-47ae-41c3-bc34-e50d970f8b05']})
    count = 0
    errors = 0
    all_rows: list[dict[str, object | None]] = []
    feature_frames: list[gpd.GeoDataFrame] = []
    prg = ProgressBar(projects.shape[0], text="Scrape Progress")
    for project_row in projects.itertuples(index=False):
        project_id = str(project_row.project_id)
        project_name = str(project_row.name)
        try:
            # get the project.rs.xml and parse realizations
            project_download_dir = download_dir / project_id
            projectrsxml_path = download_projectrsxml(rs_api, project_id, project_download_dir)
            layers = beaver_dam_layers(projectrsxml_path)
            for layer in layers:
                row: dict[str, object | None] = {'project_id': project_id, 'project_name': project_name}
                row.update(layer)
                all_rows.append(row)
                layer_gdf = process_layer(rs_api, row, project_download_dir)
                if layer_gdf.empty:
                    continue
                layer_gdf = layer_gdf.copy()
                layer_gdf['project_id'] = project_id
                # layer_gdf['project_name'] = project_name # this can be derived from project_id in Athena
                # layer_gdf['lyrName'] = str(row.get('lyrName', ''))
                # layer_gdf['geopackage_path'] = str(row.get('geopackage_path', '')) # this is internal info not useful
                # layer_gdf['realization_id'] = str(row.get('realization_id', '')) # values are realization_qris_1 or realization_qris_2 . is this useful?
                layer_gdf['realization_name'] = str(row.get('realization_name', ''))
                layer_gdf['realization_description'] = str(row.get('realization_description', ''))
                # # this is one possibility. But maybe providing the raw data and doing the BL later, ie in Athena, is preferred?
                # survey_year = get_year_from_meta_or_realiz(row)
                # if so, we only really need the CensusDate metadatavalue, not the other two
                # metadata_value = row.get('realization_metadata')
                # if isinstance(metadata_value, dict):
                #     layer_gdf['realization_metadata'] = json.dumps(metadata_value, sort_keys=True)
                # else:
                #     layer_gdf['realization_metadata'] = ''
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
        feature_gdf = gpd.GeoDataFrame(columns=['dam_cer', 'dam_type', 'type_cer', 'geometry'], geometry='geometry', crs='EPSG:4326')
    feature_gdf.to_parquet(feature_output_path)

    log.info(f'Wrote {len(results_df)} beaver_dam layer rows to {output_path}')
    log.info(f'Wrote {len(feature_gdf)} beaver activity feature rows to {feature_output_path}')
    log.info(f'Processed {count} projects successfully and {errors} failed.')


def main():
    """Process arguments, set up logs and orchestrate call to other functions"""
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    # parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = Path(args.working_folder)
    download_folder = working_folder / 'downloads'

    safe_makedirs(str(working_folder))

    log = Logger('Setup')
    log.setup(log_path=working_folder / 'qris_beaver_activity_to_athena.log', log_level=logging.DEBUG)

    log.title("Scrape QRiS Projects Beaver Activity to Athena")

    log.info('Using GeoPandas to read GeoPackage layers (SpatiaLite extension not required in this module).')

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_projects(api, download_folder)

    log.info('Process complete')


if __name__ == '__main__':
    main()
