"""
Scrapes RME and RCAT outout GeoPackages from Data Exchange and extracts statistics for each HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.
Philip Bailey
"""
import sys
import os
import sqlite3
import logging
import argparse
from rsxml import dotenv, Logger
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI
from pydex.scrape_huc_statistics import create_output_db, scrape_hucs_batch


def main():
    """
    Scrape RME projects for multiple HUCs specified by a HUC filter.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('db_path', help='Path to the warehouse dump database', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages', type=bool, default=False)
    parser.add_argument('--huc_filter', help='HUC filter SQL prefix ("17%")', type=str, default='')
    args = dotenv.parse_args_env(parser)

    if not os.path.isfile(args.db_path):
        print(f'Data Exchange project dump database file not found: {args.db_path}')
        sys.exit(1)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder  # os.path.join(args.working_folder, output_name)
    download_folder = os.path.join(working_folder, 'downloads')
    scraped_folder = working_folder  # os.path.join(working_folder, 'scraped')

    safe_makedirs(scraped_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(scraped_folder, 'rme-scrape.log'), log_level=logging.DEBUG)

    huc_filter = f" AND (huc10 LIKE ('{args.huc_filter}')) " if args.huc_filter and args.huc_filter != '.' else ''

    # Determine projects in the dumped warehouse database that have both RCAT and RME available
    with sqlite3.connect(args.db_path) as conn:
        curs = conn.cursor()
        curs.execute(f'''
            SELECT huc10, min(rme_project_id), min(rcat_project_id)
            FROM
            (
                SELECT huc10,
                    CASE WHEN project_type_id = 'rs_metric_engine' THEN project_id ELSE NULL END rme_project_id,
                    CASE WHEN project_type_id = 'rcat' then project_id ELSE NULL END             rcat_project_id
                FROM vw_conus_projects
                WHERE project_type_id IN ('rs_metric_engine', 'rcat')
                    AND tags = '2024CONUS'
            )
            GROUP BY huc10
            HAVING min(rme_project_id) IS NOT NULL
                AND min(rcat_project_id) IS NOT NULL
                {huc_filter}
            ''')
        projects = {row[0]: {
            'rme': row[1],
            'rcat': row[2]
        } for row in curs.fetchall()}

    if len(projects) == 0:
        log.info('No projects found in Data Exchange dump with both RCAT and RME')
        sys.exit(0)

    log.info(f'Found {len(projects)} RME projects in Data Exchange dump with both RME and RCAT')

    output_db = os.path.join(scraped_folder, 'rme_scrape_output.sqlite')
    create_output_db(output_db)

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_hucs_batch(api, projects, download_folder, output_db, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
