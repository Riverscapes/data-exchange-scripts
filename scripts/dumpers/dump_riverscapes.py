"""
Dumps Riverscapes Data Exchange projects to a SQLite database
"""
import shutil
import sys
import os
import traceback
import argparse
from rsxml import Logger, dotenv
from rsapi import RiverscapesAPI
from rsapi.lib.dump.dump_riverscapes import dump_riverscapes

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_db_path', help='The final resting place of the SQLite DB', type=str)
    parser.add_argument('stage', help='URL to the cybercastor API', type=str, default='production')
    parser.add_argument('template', help='GeoPackage with HUC10 geometries on which to start the process', type=str)
    parser.add_argument('search_tags', help='Comma separated tags to search for projects. Combined with "AND". e.g. 2024CONUS', type=str)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    mainlog = Logger("SQLite Riverscapes Dump")
    mainlog.setup(log_path=os.path.join(os.path.dirname(args.output_db_path), "dump_riverscapes.log"), verbose=True)

    try:
        # If the output doesn't exist and HUC10 geometry does, then copy the HUC10 geometry to the output
        if not os.path.exists(args.output_db_path) and os.path.exists(args.template):
            shutil.copyfile(args.template, args.output_db_path)

        with RiverscapesAPI(args.stage) as api:
            dump_riverscapes(api, args.output_db_path, args.search_tags)

    except Exception as e:
        mainlog.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
