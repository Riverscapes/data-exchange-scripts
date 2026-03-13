"""
Change ownership for one or more projects in the Riverscapes Data Exchange using
a CSV of project IDs. No header row is required in the CSV file. It's
just one column of project IDs.

The idea to use Athena to identify project GUIDs and then save them to
a CSV file for changing ownership.

Philip Bailey
4 Mar 2026
"""
from typing import List
import os
import argparse
import inquirer
from rsxml import ProgressBar, dotenv
from pydex import RiverscapesAPI


def change_ownership_by_csv(rs_api: RiverscapesAPI, stage: str, csv_folder: str) -> None:
    """Change ownership of projects in the Riverscapes API using a CSV file of project IDs"""

    project_ids = load_project_guids_from_csv_file(csv_folder)
    if not project_ids or len(project_ids) == 0:
        return

    # Use inquirer to get the new organization GUID and confirm the action with the user before proceeding
    questions = [
        inquirer.Text('orgGuid', message="Type the organization GUID that will take ownership of the projects?"),
        inquirer.Confirm('confirm', message=f"Are you sure you want to change ownership of {len(project_ids)} projects in the {stage} environment?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers or not answers['confirm']:
        print('Aborting')
        return

    org_guid = answers['orgGuid']
    not_found = 0
    changed = 0
    change_owner_qry = rs_api.load_mutation('changeProjectOwner')
    prg = ProgressBar(total=len(project_ids), text='Changing ownership of projects')
    for i, project_id in enumerate(project_ids):
        try:
            result = rs_api.run_query(change_owner_qry, {'projectId': project_id, 'owner': {'id': org_guid, 'type': 'ORGANIZATION'}})
            if result is None:
                raise Exception('run query returned None')
            elif 'error' in result['data']['changeProjectOwner'] and result['data']['changeProjectOwner']['error'] is not None:
                raise Exception(result['data']['changeProjectOwner']['error'])
            else:
                changed += 1
        except Exception as e:
            raise e
        prg.update(i+1)

    prg.finish()

    print(f'Process complete. {changed} projects processed. {not_found} projects not found.')


def load_project_guids_from_csv_file(csv_folder: str) -> List[str]:
    """
    Prompt the user to select a CSV file from the specified folder and return a list of project GUIDs
    """

    if not os.path.exists(csv_folder):
        print(f'The folder {csv_folder} does not exist. Please provide a valid folder with CSV files.')
        return None

    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    if not csv_files:
        print(f'No CSV files found in {csv_folder}. Please provide a valid folder with CSV files.')
        return None

    answers = inquirer.prompt([inquirer.List("csv_path", message="Select a CSV file to use", choices=csv_files)])
    if not answers:
        print('Aborting')
        return None
    csv_path = os.path.join(csv_folder, answers['csv_path'])
    print(f'Selected CSV file: {csv_path}')

    project_ids = []
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        for line in csvfile:
            project_id = line.strip()
            if project_id:
                project_ids.append(project_id)

    return project_ids


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Production or staging Data Exchange', type=str, default='production')
    parser.add_argument('csv_folder', help='Folder containing CSV files with project IDs', type=str)
    args = dotenv.parse_args_env(parser)

    print(f'Changing ownership of projects in {args.stage} environment')
    with RiverscapesAPI(stage=args.stage) as api:
        change_ownership_by_csv(api, args.stage, args.csv_folder)
