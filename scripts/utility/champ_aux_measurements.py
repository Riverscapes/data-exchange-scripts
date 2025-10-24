"""
1. Loop over CHaMP visits that are present in the SQLite workbench database.
2. Search the data exchange for a project matching the CHaMP visit ID.
3. Download the project.rs.xml file for each matching project.
4. Write the aux metrics as individual JSON files in the project folder.
5. Update the project.rs.xml file to reference the new JSON files.
6. Upload the modified project.rs.xml and JSON files back to the data exchange.

Philip Bailey
23 Oct 2025
"""

import os
import re
import sqlite3
import json
import argparse
from datetime import datetime
from rsxml import ProgressBar, dotenv, Logger
from rsxml.project_xml import Project, Dataset, Meta, MetaData
from rsxml.util import safe_makedirs
from new_project_upload import upload_project
from pydex import RiverscapesAPI
from pydex.classes.riverscapes_helpers import RiverscapesSearchParams
import psycopg2


def process_champ_visits(api: RiverscapesAPI, db_path: str, download_dir: str, delete_files: bool) -> None:
    """Process CHaMP visits from the workbench database and upload aux measurements to the data exchange"""

    log = Logger('CHaMP_Aux_Measurements')

    postgres_conn = psycopg2.connect('service=CHaMPGooglePostgres')
    postgres_cursor = postgres_conn.cursor()

    # Retrieve CHaMP visit IDs that have topo projects, but are missing aux measurements
    postgres_cursor.execute('''
        SELECT v.visit_id, s.name, w.name, v.visit_year, p.guid
        FROM visits v
            inner join sites s on v.site_id = s.site_id
            inner join watersheds w on s.watershed_id = w.watershed_id
                inner join projects p on v.visit_id = p.visit_id
        WHERE (program_id = 1)
        AND (project_type_id = 1)
        AND (guid IS NOT NULL)
        AND (aux_uploaded IS NULL)
        ORDER BY w.name, s.name, v.visit_year
    ''')

    visits = {
        row[0]: {
            'site_name': row[1],
            'watershed_name': row[2],
            'visit_year': row[3],
            'project_guid': row[4]
        } for row in postgres_cursor.fetchall()
    }

    sqlite_conn = sqlite3.connect(db_path)
    sqlite_curs = sqlite_conn.cursor()

    processed = 0
    errors = 0
    progbar = ProgressBar(len(visits), 50, 'CHaMP Aux', byte_format=True)
    for visit_id, visit_data in visits.items():
        site_name = visit_data['site_name']
        watershed_name = visit_data['watershed_name']
        visit_year = visit_data['visit_year']
        project_guid = visit_data['project_guid']
        log.info(f'Processing visit ID {visit_id} ({watershed_name} - {site_name} - {visit_year})')

        try:
            visit_dir = os.path.join(download_dir, f'visit_{visit_id}_{project_guid}')
            aux_dir = os.path.join(visit_dir, 'aux_measurements')
            safe_makedirs(visit_dir)
            safe_makedirs(aux_dir)
            api.download_files(project_guid, visit_dir, ['project\\.rs\\.xml'], force=True)
            project_xml_path = os.path.join(visit_dir, 'project.rs.xml')
            if not os.path.exists(project_xml_path):
                log.error(f'project.rs.xml not found for project ID {project_guid}')
                continue

            # retrieve all the aux measurements and save them to JSON files
            sqlite_curs.execute("""
                select l.title, m.Value
                from CHaMP_Measurements m
                    inner join LookupListItems l on m.MeasurementTypeID = l.ItemID
                where m.VisitID = ?""", (visit_id,))

            visit_aux_files = {}
            for row in sqlite_curs.fetchall():
                measurement_name = row[0]
                metric_value = json.loads(row[1])
                clean_name = re.sub(r'[_\s()]+', '_', measurement_name).strip('_')
                file_name = f'{clean_name.lower()}.json'
                aux_file_path = os.path.join(aux_dir, file_name)
                with open(aux_file_path, 'w', encoding='utf-8') as f:
                    json.dump({'value': metric_value}, f, indent=4)

                if aux_file_path in visit_aux_files:
                    log.error(f'Duplicate aux measurement file for {measurement_name} at {aux_file_path}, overwriting.')
                    continue

                visit_aux_files[aux_file_path] = (measurement_name, clean_name)

            if len(visit_aux_files) == 0:
                log.info(f'No aux measurement files found for visit ID {visit_id}, skipping upload.')
                continue

            log.info(f'Prepared {len(visit_aux_files)} aux measurement files for visit ID {visit_id}')

            # Load the project XML and update it to reference the new aux measurement files
            project = Project.load_project(project_xml_path)
            datasets = project.realizations[0].datasets

            for aux_file, (measurement_name, clean_name) in visit_aux_files.items():
                datasets.append(Dataset(
                    xml_id=f'CHAMP_Aux_{clean_name}'.upper(),
                    name=measurement_name,
                    path=os.path.relpath(aux_file, visit_dir),
                    ds_type='File',
                    meta_data=MetaData([Meta('measurementType', measurement_name)])
                ))

            # Write the project file
            project.write()

            # Upload the project found in this folder
            upload_project(api, project_xml_path)

            log.info(f'Uploaded aux measurements for visit ID {visit_id}')

            # Update the aux_uploaded flag in the Postgres database
            postgres_cursor.execute('UPDATE visits SET aux_uploaded = %s WHERE visit_id = %s', (datetime.now(), visit_id))

            if delete_files is True:
                try:
                    for root, _dirs, files in os.walk(visit_dir, topdown=False):
                        for name in files:
                            os.remove(os.path.join(root, name))
                        for name in _dirs:
                            os.rmdir(os.path.join(root, name))
                    os.rmdir(visit_dir)
                    log.info(f'Deleted files for visit ID {visit_id}')
                except Exception as e:
                    log.error(f'Error deleting files for visit ID {visit_id}: {e}')

            processed += 1
            progbar.update(processed)
        except Exception as e:
            print(f'Error processing visit ID {visit_id}: {e}')
            errors += 1

    progbar.finish()
    print(f'Process complete. {processed} visits processed. {errors} errors encountered.')


def main():
    """Main function to parse arguments and initiate processing of CHaMP visits"""
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Production or staging Data Exchange', type=str)
    parser.add_argument('db_path', help='Path to the workbench SQLite database', type=str)
    parser.add_argument('download_dir', help='Path to the download directory to temporarily store visit files', type=str)
    parser.add_argument('delete_files', help='Whether to delete downloaded files after upload', type=bool)
    args = dotenv.parse_args_env(parser)

    with RiverscapesAPI(stage=args.stage) as api:
        process_champ_visits(api, args.db_path, args.download_dir, args.delete_files)


if __name__ == "__main__":
    main()
