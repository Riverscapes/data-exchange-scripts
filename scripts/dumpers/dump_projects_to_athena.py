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
    project_id      STRING,
    name            STRING,
    tags            ARRAY<STRING>,
    huc10           CHAR(10),
    model_version_int INT,
    created_on      BIGINT,
    created_on_date STRING,
    owned_by_id     STRING,
    owned_by_name   STRING,
    owned_by_type   STRING
)
PARTITIONED BY (project_type_id STRING, model_version STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION 's3://riverscapes-athena/data_exchange/projects'
TBLPROPERTIES ('skip.header.line.count'='1',
'collection.delim' = '|'
);

MSCK REPAIR TABLE rs_projects;
```
"""
import csv
from datetime import datetime, timezone
import os
import tempfile
import sqlite3
import argparse
import boto3
from rsxml import dotenv
from pydex import RiverscapesAPI, RiverscapesSearchParams
from pydex.lib.athena import athena_execute, athena_query_get_parsed


def scrape_projects_to_sqlite(rs_api: RiverscapesAPI, curs: sqlite3.Cursor, search_params: RiverscapesSearchParams) -> int:
    """
    Loop over all the projects that match the search params. Store them in a temporary SQLite database.
    Then query this database by project type and (UNIQUE) project creation date and create CSV files that
    are then uploaded to S3 for Athena.
    """

    print('Scraping projects to temporary, in-memory SQLite...')

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

        model_version_int = int(model_version.split('.')[0]) * 1000000 + int(model_version.split('.')[1]) * 1000 + int(model_version.split('.')[2])

        # Insert project data
        # The pipe separating tags is vital. It must correspond wtith the Athena table definition.
        curs.execute('''
            INSERT INTO rs_projects (
                project_id,
                name,
                tags,
                huc10,
                model_version,
                model_version_int,
                archived,
                project_type_id,
                created_on,
                created_on_date,
                owned_by_id,
                owned_by_name,
                owned_by_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            project.id,
            project.name.replace(',', ' '),
            '|'.join(project.tags) if project.tags else None,
            huc10,
            model_version,
            model_version_int,
            1 if project.archived else 0,
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

    # Remove the columns that are not needed for the CSV file because they are partition keys
    columns.remove('project_type_id')
    columns.remove('model_version')

    # Store the unique days for each project type. We will create a separate CSV file for
    # each day and each project type. This should help partition the data better in Athena.
    curs.execute("""
        CREATE TEMP TABLE temp_projects AS
        SELECT DISTINCT 
            project_type_id,
            CAST(created_on / 86400000 as INT) * 86400000 create_stamp,
            date(created_on / 1000, 'unixepoch') create_date,
            model_version,
            0 processed
        FROM rs_projects
        GROUP BY project_type_id, create_stamp, create_date, model_version""")

    curs.execute('SELECT COUNT(*) FROM temp_projects')
    total_temp_projects = curs.fetchone()[0]
    print(f'Total unique project types and dates: {total_temp_projects:,}')

    # curs.execute('SELECT * FROM temp_projects')
    # rows = curs.fetchall()
    # print(rows)

    while True:
        curs.execute("SELECT project_type_id, create_stamp, create_date, model_version FROM temp_projects WHERE processed = 0 LIMIT 1")
        row = curs.fetchone()
        if row is None:
            break

        project_type_id, unique_stamp, unique_date, model_version = row
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tsv', mode='w', newline='\n', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter='\t', escapechar='\\', quoting=csv.QUOTE_NONE)
            csvwriter.writerow(columns)
            for row in curs.execute(f'SELECT {", ".join(columns)} FROM rs_projects WHERE project_type_id = ? AND CAST(created_on / 86400000 as INT) * 86400000 = ?', [project_type_id, unique_stamp]):
                csvwriter.writerow(row)
            csvfile.flush()  # Ensure all data is written to the file
            temp_filename = csvfile.name

        file_key = f'data_exchange/projects/project_type_id={project_type_id}/model_version={model_version}/{unique_date}-{project_type_id}.tsv'
        s3.upload_file(csvfile.name, s3_bucket, file_key)
        os.remove(temp_filename)
        print(f'Upload complete to S3 key: {file_key}')
        curs.execute("UPDATE temp_projects SET processed = 1 WHERE project_type_id = ? AND create_stamp = ?", [project_type_id, unique_stamp])


def get_max_existing_athena_date(s3_bucket: str) -> datetime | None:
    """
    Get the maximum existing date in the Athena table from S3.
    This is used to determine if we need to delete the existing Athena table.
    """
    rows = athena_query_get_parsed(s3_bucket, 'SELECT MAX(created_on) AS max_created_on FROM vw_projects')
    if rows and rows[0].get('max_created_on'):
        data = rows[0]['max_created_on']
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
    Search the Data Exchange for projects and upload them to an S3 bucket for Athena.
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
            search_params.createdOnFrom = search_start
            print(f'Existing max date in Athena: {existing_max_date}')
            print(f'Searching Data Exchange from: {search_start}')
    else:
        print('Full scrape of all projects requested. Ignoring existing Athena dates.')

    if args.tags and args.tags != '' and args.tags != '.':
        search_params.tags = args.tags.split(',')

    # Create an in memory SQLite database to store the project data
    with sqlite3.connect(":memory:") as conn:
        curs = conn.cursor()

        curs.execute('''CREATE TABLE rs_projects (
            project_id      TEXT NOT NULL PRIMARY KEY,
            name            TEXT NOT NULL,
            project_type_id TEXT NOT NULL,
            tags            TEXT,
            huc10           TEXT NOT NULL,
            model_version   TEXT NOT NULL,
            model_version_int INTEGER NOT NULL,
            archived        INTEGER NOT NULL DEFAULT 0,
            created_on      INTEGER NOT NULL,
            created_on_date TEXT NOT NULL,
            owned_by_id     TEXT NOT NULL,
            owned_by_name   TEXT NOT NULL,
            owned_by_type   TEXT NOT NULL
        ) WITHOUT ROWID''')

        curs.execute('CREATE INDEX idx_created_on ON rs_projects (created_on)')

        with RiverscapesAPI(stage=args.stage) as api:

            total_projects = scrape_projects_to_sqlite(api, curs, search_params)
            if total_projects == 0:
                print('No projects found with the specified tags. Exiting...')
                return
            upload_sqlite_to_s3(curs, args.s3_bucket)

            # Need to refresh partitions after uploading new data to S3
            print('Refreshing Athena partitions...')
            athena_execute(args.s3_bucket, 'MSCK REPAIR TABLE rs_projects')

        print('Process complete')


if __name__ == '__main__':
    main()
