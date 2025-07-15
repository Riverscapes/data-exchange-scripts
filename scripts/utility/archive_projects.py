"""
Set the archive flag one or more projects in the Riverscapes Data Exchange using
a CSV of project IDs. No header row is required in the CSV file. It's
just one column of project IDs.

The idea to use Athena to identify project GUIDs and then save them to
a CSV file for archiving.

Philip Bailey
15 July 2025
"""
import argparse
import inquirer
from pydex import RiverscapesAPI


def archive_projects_by_csv(rs_api: RiverscapesAPI, stage: str) -> None:
    """Archive projects from the Riverscapes API using a CSV file of project IDs"""

    answers = inquirer.prompt([inquirer.Text("csv_path", message="Path to CSV file with project IDs")])
    csv_path = answers['csv_path']
    print(f'Archiving projects from {stage} using CSV file: {csv_path}')
    project_ids = []
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        for line in csvfile:
            project_id = line.strip()
            if project_id:
                project_ids.append(project_id)

    confirm_archive = inquirer.prompt([inquirer.Confirm("confirm", message=f'Are you sure you want to archive {len(project_ids)} projects?', default=False)])
    if not confirm_archive['confirm']:
        print('Aborting')
        return

    not_found = 0
    archived = 0
    archive_qry = rs_api.load_mutation('archiveProject')
    for project_id in project_ids:
        try:
            result = rs_api.run_query(archive_qry, {'projectId': project_id, 'project': {'archived': True}})
            if result is None:
                raise Exception('')
            elif 'error' in result['data']['updateProject']:
                raise Exception(result['data']['updateProject']['error'])
            else:
                archived += 1
        except Exception as e:
            if e is not None and ('not found' in str(e) or 'already archived' in str(e)):
                not_found += 1
            else:
                raise e

    print(f'Process complete. {archived} projects archived. {not_found} projects not found.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Production or staging Data Exchange', type=str, default='production')
    args = parser.parse_args()

    print(f'Archiving projects from {args.stage} environment')
    with RiverscapesAPI(stage=args.stage) as api:
        archive_projects_by_csv(api, args.stage)
