"""
Scrapes RME GeoPackage from Data Exchange and extracts statistics for a single HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.
Philip Bailey
"""
import sys
import os
import argparse
from rsxml import Logger, dotenv
from rsapi.scrape_huc_statistics import scrape_huc_statistics, create_output_db


def main():
    """
    Scrape RME metrics for a single HUC
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('huc', help='HUC code for the scrape', type=str)
    parser.add_argument('rme_gpkg', help='RME output GeoPackage path', type=str)
    parser.add_argument('--delete', help='Delete the output database if it exists', action='store_true')
    parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true')
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger('RME Scrape')

    log_dir = os.path.join(os.path.dirname(args.rme_gpkg))
    log.setup(logPath=os.path.join(log_dir, 'rme_scrape.log'), verbose=args.verbose)
    log.title(f'RME scrape for HUC: {args.huc}')

    if not os.path.isfile(args.rme_gpkg):
        log.error(f'RME output GeoPackage cannot be found: {args.rme_gpkg}')
        sys.exit(1)

    # Place the output RME scrape database in the same directory as the RME GeoPackage
    output_db = os.path.join(os.path.dirname(args.rme_gpkg), 'rme_scrape.sqlite')
    log.info(f'Output database: {output_db}')

    try:
        create_output_db(output_db, args.delete)
        scrape_huc_statistics(args.huc, args.rme_gpkg, output_db)
    except Exception as e:
        log.error(f'Error scraping HUC {args.huc}: {e}')
        sys.exit(1)

    log.info('Process complete')


if __name__ == '__main__':
    main()
