"""
Delete one or more projects from the Riverscapes Data Exchange using
a CSV of project IDs. No header row is required in the CSV file. It's
just one column of project IDs.

The idea to use Athena to identify project GUIDs and then save them to
a CSV file for deletion.

Philip Bailey
15 July 2025
"""
import os
import argparse
import inquirer
from rsxml import ProgressBar, dotenv
from pydex import RiverscapesAPI


def delete_projects_by_csv(rs_api: RiverscapesAPI, stage: str, csv_folder: str) -> None:
    """Delete projects from the Riverscapes API using a CSV file of project IDs"""

    if not os.path.exists(csv_folder):
        print(f'The folder {csv_folder} does not exist. Please provide a valid folder with CSV files.')
        return

    # Get a list of all CSV files in the specified folder. Do not walk to subfolders.
    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    if not csv_files:
        print(f'No CSV files found in {csv_folder}. Please provide a valid folder with CSV files.')
        return

    answers = inquirer.prompt([inquirer.List("csv_path", message="Select a CSV file to use", choices=csv_files)])
    if not answers:
        print('Aborting')
        return
    csv_path = os.path.join(csv_folder, answers['csv_path'])
    print(f'Deleting projects from {stage} using CSV file: {csv_path}')

    project_ids = []
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        for line in csvfile:
            project_id = line.strip()
            if project_id:
                project_ids.append(project_id)

    confirm_delete = inquirer.prompt([inquirer.Text("confirm", message=f'Type the word DELETE to delete {len(project_ids)} projects')])
    if not confirm_delete or confirm_delete['confirm'] != 'DELETE':
        print('Aborting')
        return

    not_found = 0
    deleted = 0
    delete_qry = rs_api.load_mutation('deleteProject')
    prg = ProgressBar(total=len(project_ids), text='Deleting projects')
    for i, project_id in enumerate(project_ids):
        try:
            result = rs_api.run_query(delete_qry, {'projectId': project_id, 'options': {}})
            if result is None:
                raise Exception('run query returned None')
            elif result['data']['deleteProject']['error'] is not None:
                raise Exception(result['data']['deleteProject']['error'])
            else:
                deleted += 1
        except Exception as e:
            if e is not None and ('not found' in str(e) or 'already deleted' in str(e)):
                not_found += 1
            else:
                raise e
        prg.update(i+1)

    prg.finish()

    print(f'Process complete. {deleted} projects deleted. {not_found} projects not found.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Production or staging Data Exchange', type=str, default='production')
    parser.add_argument('csv_folder', help='Folder containing CSV files with project IDs', type=str)
    args = dotenv.parse_args_env(parser)

    print(f'Deleting projects from {args.stage} environment')
    with RiverscapesAPI(stage=args.stage) as api:
        delete_projects_by_csv(api, args.stage, args.csv_folder)
