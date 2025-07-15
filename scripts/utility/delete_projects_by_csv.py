"""
Delete one or more projects from the Riverscapes Data Exchange using
a CSV of project IDs. No header row is required in the CSV file. It's
just one column of project IDs.

The idea to use Athena to identify project GUIDs and then save them to
a CSV file for deletion.

Philip Bailey
15 July 2025
"""
import argparse
import inquirer
from pydex import RiverscapesAPI


def delete_projects_by_csv(rs_api: RiverscapesAPI, stage: str) -> None:
    """Delete projects from the Riverscapes API using a CSV file of project IDs"""

    answers = inquirer.prompt([inquirer.Text("csv_path", message="Path to CSV file with project IDs")])
    csv_path = answers['csv_path']
    print(f'Deleting projects from {stage} using CSV file: {csv_path}')
    project_ids = []
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        for line in csvfile:
            project_id = line.strip()
            if project_id:
                project_ids.append(project_id)

    confirm_delete = inquirer.prompt([inquirer.Text("confirm", message=f'Type the word DELETE to delete {len(project_ids)} projects')])
    if confirm_delete['confirm'] != 'DELETE':
        print('Aborting')
        return

    not_found = 0
    deleted = 0
    delete_qry = rs_api.load_mutation('deleteProject')
    for project_id in project_ids:
        try:
            result = rs_api.run_query(delete_qry, {'projectId': project_id, 'options': {}})
            if result is None or result['data']['deleteProject']['error'] is not None:
                raise Exception(result['data']['deleteProject']['error'])
            else:
                deleted += 1
        except Exception as e:
            if e is not None and ('not found' in str(e) or 'already deleted' in str(e)):
                not_found += 1
            else:
                raise e

    print(f'Process complete. {deleted} projects deleted. {not_found} projects not found.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Production or staging Data Exchange', type=str, default='production')
    args = parser.parse_args()

    print(f'Deleting projects from {args.stage} environment')
    with RiverscapesAPI(stage=args.stage) as api:
        delete_projects_by_csv(api, args.stage)
