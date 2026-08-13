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

Lorin Gaertner
August 2026
"""

import argparse
import logging
from pathlib import Path

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


def scrape_projects(rs_api: RiverscapesAPI, download_dir: Path):
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
    log.info(f'Wrote {len(results_df)} beaver_dam layer rows to {output_path}')
    log.info(f'Processed {count} projects successfully and {errors} failed.')


def main():
    """Process arguments, set up logs and orchestrate call to other functions"""
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    # parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
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

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_projects(api, download_folder)

    log.info('Process complete')


if __name__ == '__main__':
    main()
