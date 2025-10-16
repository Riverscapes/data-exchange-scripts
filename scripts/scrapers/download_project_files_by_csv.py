"""
Take a CSV of project IDs and download all the associated project files to local disk.

The vision is to query Athena for the projects required and then save those GUIDs to a CSV file.
Then use this script to download all the files for those projects.

Philip Bailey
15 Oct 2025
"""
import os
import argparse
from rsxml import ProgressBar, dotenv
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI
from scripts.utility.load_project_guids_from_csv import load_project_guids_from_csv


def download_project_files_by_csv(rs_api: RiverscapesAPI, csv_folder: str, download_folder: str, regex_list: list[str], force: bool) -> None:
    """Delete projects from the Riverscapes API using a CSV file of project IDs"""

    project_ids = load_project_guids_from_csv(csv_folder)
    prg = ProgressBar(total=len(project_ids), text='Projects')
    downloaded = 0
    errors = 0
    for i, project_id in enumerate(project_ids):
        try:
            project_dir = os.path.join(download_folder, project_id)
            safe_makedirs(project_dir)
            rs_api.download_files(project_id, project_dir, regex_list, force)

            downloaded += 1
        except Exception as e:
            if e is not None and ('not found' in str(e) or 'already deleted' in str(e)):
                errors += 1
            else:
                raise e
        prg.update(i+1)

    prg.finish()
    print(f'Process complete. {downloaded} projects downloaded. {errors} projects errored out.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Production or staging Data Exchange', type=str, default='production')
    parser.add_argument('csv_folder', help='Folder containing CSV files with project IDs', type=str)
    parser.add_argument('download_folder', help='Folder to download project files', type=str)
    parser.add_argument('--regex_list', help='List of regex patterns to match files to download. Default is all files.', type=str, nargs='*', default=['.*'])
    parser.add_argument('--force', help='Force re-download of files even if they already exist', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    print(f'Downloading project files from {args.stage} environment')
    with RiverscapesAPI(stage=args.stage) as api:
        download_project_files_by_csv(api, args.csv_folder, args.download_folder, regex_list=args.regex_list, force=args.force)
