"""
Scrapes RME and RCAT output GeoPackages from Data Exchange and extracts statistics for each HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.

This script assumes that the `scrape_huc_statistics.py` script has been run on each RME project.
The scrape_huc_statistics.py script extracts statistics from the RME and RCAT output GeoPackages
and generates a new 'rme_scrape.sqlite' file in the project. This is then uploaded into the
project on the Data Exchange.



CREATE
EXTERNAL TABLE rs_projects (
    id              INT,
    project_id      STRING,
    name            STRING,
    project_type_id STRING,
    tags            STRING,
    huc10           STRING,
    model_version   STRING,
    created_on      BIGINT,
    created_on_date STRING,
    owned_by_id     STRING,
    owned_by_name   STRING,
    owned_by_type   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION 's3://riverscapes-athena/data_exchange_projects'
TBLPROPERTIES ('skip.header.line.count'='1');


"""
import csv
from datetime import datetime, timezone
import tempfile
import sqlite3
import argparse
import boto3
import time
from pydex import RiverscapesAPI, RiverscapesSearchParams
from rsxml import dotenv


def scrape_projects_to_sqlite(rs_api: RiverscapesAPI, curs: sqlite3.Cursor, search_params: RiverscapesSearchParams) -> int:
    """
    Loop over all the projects, download the RME and RCAT output GeoPackages, and scrape the statistics
    """

    print('Scraping projects to SQLite...')

    for project, _stats, _searchtotal, _prg in rs_api.search(search_params, progress_bar=True, page_size=100):

        # Attempt to retrieve the huc10 and model version from the project metadata if it exists
        huc10 = next((project.project_meta[k] for k in ['HUC10', 'huc10', 'HUC', 'huc'] if k in project.project_meta), None)
        model_version = next((project.project_meta[k] for k in ['modelVersion', 'model_version', 'Model Version'] if k in project.project_meta), None)

        if huc10 is None:
            print(f'Project {project.id} does not have a HUC10. Skipping...')
            continue

        if model_version is None:
            print(f'Project {project.id} does not have a model version. Skipping...')
            continue

        # Insert project data
        curs.execute('''
            INSERT INTO rs_projects (
                project_id,
                name,
                tags,
                huc10,
                model_version,
                project_type_id,
                created_on,
                created_on_date,
                owned_by_id,
                owned_by_name,
                owned_by_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            project.id,
            project.name.replace(',', ' '),
            '|'.join(project.tags),
            huc10,
            model_version,
            project.project_type,
            int(project.created_date.timestamp() * 1000),
            project.created_date.strftime('%Y-%m-%d %H:%M:%S'),
            project.json['ownedBy']['id'],
            project.json['ownedBy']['name'].replace(',', ''),
            project.json['ownedBy']['__typename']
        ))

    curs.execute('SELECT COUNT(*) FROM rs_projects')
    total_projects = curs.fetchone()[0]
    print(f'Total projects scraped: {total_projects:,}')
    return total_projects


def upload_sqlite_to_s3(curs: sqlite3.Cursor, s3_bucket: str) -> None:
    """ Write the contents of the rs_projects table to a temporary CSV file and upload it to S3"""

    print('Uploading SQLite data to S3...')
    s3 = boto3.client('s3')

    # Get the columns of the rs_projects table
    curs.execute('PRAGMA table_info(rs_projects)')
    columns = [col[1] for col in curs.fetchall()]

    # Take the timestamp created_on column and return all the unique DAYS (86400000 milliseconds) as integers and dates
    curs.execute("SELECT DISTINCT cast(created_on / 86400000 as INT) * 86400000, date(created_on / 1000, 'unixepoch') from rs_projects")
    unique_dates = {row[0]: row[1] for row in curs.fetchall()}

    # Write the contents of the database to a temporary CSV file for each day
    for unique_stamp, unique_date in unique_dates.items():
        with tempfile.NamedTemporaryFile(delete=True, suffix='.csv', mode='w', newline='\n', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(columns)
            for row in curs.execute(f'SELECT {", ".join(columns)} FROM rs_projects WHERE created_on >= ?', [unique_stamp]):
                csvwriter.writerow(row)
            csvfile.flush()  # Ensure all data is written to the file

            file_name = f'{unique_date}-projects.csv'
            s3.upload_file(csvfile.name, s3_bucket, f'data_exchange_projects/{file_name}')
            print(f'Upload complete: {file_name} uploaded to S3 key: {s3_bucket}/data_exchange_projects/{file_name}')


def get_max_existing_athena_date(s3_bucket: str) -> datetime:
    """
    Get the maximum existing date in the Athena table from S3.
    This is used to determine if we need to delete the existing Athena table.
    """

    athena = boto3.client('athena', region_name='us-west-2')
    response = athena.start_query_execution(
        QueryString='SELECT MAX(created_on) FROM rs_projects',
        QueryExecutionContext={
            'Database': 'default',
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': f's3://{s3_bucket}/athena_query_results'
        }
    )

    query_execution_id = response['QueryExecutionId']

    # Poll for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)  # Wait before polling again

    if state != 'SUCCEEDED':
        print(f"Athena query failed or was cancelled: {state}")
        return None

    result = athena.get_query_results(QueryExecutionId=query_execution_id)

    # Athena returns header row as first row, data as second row
    rows = result['ResultSet']['Rows']
    if len(rows) > 1:
        data = rows[1]['Data'][0].get('VarCharValue')
        if data:
            try:
                date_timestamp = int(data)
                return datetime.fromtimestamp(date_timestamp / 1000, tz=timezone.utc)
            except ValueError:
                try:
                    return datetime.strptime(data, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except Exception:
                    pass
    return None


def main():
    """
    Search the Data Exchange for RME projects that have the RME scrape and then
    merge the contents into a single output database.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('s3_bucket', help='s3 bucket RME files will be placed', type=str)
    # parser.add_argument('tags', help='Data Exchange tags to search for projects', type=str)
    parser.add_argument('--full_scrape', help='Full scrape of all projects, or just new projects', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    search_params = RiverscapesSearchParams({})
    if args.full_scrape is False:
        # Get the maximum existing date in the Athena table and backup to midnight UTC
        existing_max_date = get_max_existing_athena_date(args.s3_bucket)
        if existing_max_date:
            search_start = existing_max_date.replace(hour=0, minute=0, second=0, microsecond=0)
            search_params.created_on = {'from':  datetime.strftime(search_start, '%Y-%m-%d %H:%M:%S')}
            print(f'Existing max date in Athena: {existing_max_date}')

    # Create an in memory SQLite database to store the project data
    with sqlite3.connect(":memory:") as conn:
        curs = conn.cursor()

        curs.execute('''CREATE TABLE rs_projects (
            id INTEGER      PRIMARY KEY AUTOINCREMENT,
            project_id      TEXT NOT NULL UNIQUE,
            name            TEXT NOT NULL,
            project_type_id TEXT NOT NULL,
            tags            TEXT,
            huc10           TEXT NOT NULL,
            model_version   TEXT NOT NULL,
            created_on      INTEGER NOT NULL,
            created_on_date TEXT NOT NULL,
            owned_by_id     TEXT NOT NULL,
            owned_by_name   TEXT NOT NULL,
            owned_by_type   TEXT NOT NULL
        )''')

        with RiverscapesAPI(stage=args.stage) as api:

            total_projects = scrape_projects_to_sqlite(api, curs, search_params)
            if total_projects == 0:
                print('No projects found with the specified tags. Exiting...')
                return
            upload_sqlite_to_s3(curs, args.s3_bucket)

        print('Process complete')


if __name__ == '__main__':
    main()
