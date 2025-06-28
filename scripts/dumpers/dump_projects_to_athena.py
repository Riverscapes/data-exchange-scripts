"""
Calls Data Exchange API to scrape projects into AWS Athena. The command line arguments allow
for either a full scrape of all projects or a scrape of only new projects since the last scrape into Athena.
This latter, partial scrape, is determined by checking the maximum `created_on` date in the existing Athena table
and then searching for all projects since midnight UTC of that date.

Philip Bailey
27 June 2025

The script creates an SQLite in-memory database to store the project data, which is then written to temporary CSV files
and uploaded to an S3 bucket. The Athena table is expected to be created beforehand with the following DDL:

```sql
-- Athena table creation DDL
-- This table is used to store project data scraped from the Data Exchange API.
-- The table is expected to be created in the 'default' database in Athena.
-- The S3 location is where the CSV files will be uploaded.
-- The table properties are set to skip the header line in the CSV files.
-- Make sure to adjust the S3 location to your specific bucket and path.
-- Example S3 location: s3://your-bucket-name/data_exchange_projects
-- The table is stored as TEXTFILE with tab-separated values.
-- Make sure to run this DDL before running the script.

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
```
"""
import csv
from datetime import datetime, timezone
import tempfile
import sqlite3
import argparse
import time
import boto3
from rsxml import dotenv
from pydex import RiverscapesAPI, RiverscapesSearchParams


def scrape_projects_to_sqlite(rs_api: RiverscapesAPI, curs: sqlite3.Cursor, search_params: RiverscapesSearchParams) -> int:
    """
    Loop over all the projects, download the RME and RCAT output GeoPackages, and scrape the statistics
    """

    print('Scraping projects to SQLite...')

    for project, _stats, _searchtotal, _prg in rs_api.search(search_params, progress_bar=True, page_size=500):

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

    # Store the unique days for each project type. We will create a separate CSV file for 
    # each day and each project type. This should help partition the data better in Athena.
    curs.execute("""
        CREATE TEMP TABLE temp_projects AS
        SELECT DISTINCT 
            project_type_id,
            CAST(created_on / 86400000 as INT) * 86400000 create_stamp,
            date(created_on / 1000, 'unixepoch') create_date,
            0 processed
        FROM rs_projects
        GROUP BY project_type_id, create_stamp, create_date""")

    curs.execute('SELECT COUNT(*) FROM temp_projects')
    total_temp_projects = curs.fetchone()[0]
    print(f'Total unique project types and dates: {total_temp_projects:,}')

    # curs.execute('SELECT * FROM temp_projects')
    # rows = curs.fetchall()
    # print(rows)

    while True:
        curs.execute("SELECT project_type_id, create_stamp, create_date FROM temp_projects WHERE processed = 0 LIMIT 1")
        row = curs.fetchone()
        if row is None:
            break
        
        project_type_id, unique_stamp, unique_date = row
        with tempfile.NamedTemporaryFile(delete=True, suffix='.csv', mode='w', newline='\n', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(columns)
            for row in curs.execute(f'SELECT {", ".join(columns)} FROM rs_projects WHERE project_type_id = ? AND CAST(created_on / 86400000 as INT) * 86400000 = ?', [project_type_id, unique_stamp]):
                csvwriter.writerow(row)
            csvfile.flush()  # Ensure all data is written to the file

            file_key = f'data_exchange_projects/{project_type_id}/{unique_date}-{project_type_id}.csv'
            s3.upload_file(csvfile.name, s3_bucket, file_key)
            # print(f'Upload complete to S3 key: {file_key}')
            curs.execute("UPDATE temp_projects SET processed = 1 WHERE project_type_id = ? AND create_stamp = ?", [project_type_id, unique_stamp])


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
    parser.add_argument('--tags', help='Data Exchange tags to search for projects', type=str, default='')
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

    if args.tags or args.tags != '' or args.tags != '.':
        search_params.tags = args.tags.split(',')

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
