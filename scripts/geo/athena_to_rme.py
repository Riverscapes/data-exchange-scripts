"""
THIS WAS COPIED TO rs-report-gen repository where it was enhanced. PROBABLY GET RID OF IT HERE. 
Reads a CSV file from S3 and generates a GeoPackage (SQLite) file with tables
such as dgos, dgos_veg, dgo_hydro, etc., based on a column-to-table-and-type mapping
from rme_table_column_defs.csv. All tables will include a sequentially generated dgoid column.

Lorin Gaertner
August 2025

IMPLEMENTED: Sequential dgoid (integer), geometry handling for dgo_geom (SRID 4326, WKT conversion), foreign key syntax in table creation, error handling (throws on missing/malformed required columns), debug output for skipped/invalid rows.
INCOMPLETE: Actual column names/types must be supplied in rme_table_column_defs.csv. Geometry column creation and spatialite extension loading are stubbed (see TODOs). No batching/optimization for very large CSVs. No advanced validation or transformation beyond geometry and required columns.
ASSUMPTION: All columns are TEXT unless otherwise specified in rme_table_column_defs.csv. Foreign key constraints are defined but not enforced unless PRAGMA foreign_keys=ON is set.
"""


import os
import csv
import logging
import argparse
import boto3
import apsw
import uuid
from rsxml import dotenv, Logger, ProgressBar


def parse_table_defs(defs_csv_path):
    """
    Parse rme_table_column_defs.csv and return a dict of table schemas and column order.
    Returns: {table_name: {col_name: col_type, ...}, ...}, {table_name: [col_order]}
    Adds sequential integer dgoid to all tables.
    """
    table_schema_map = {}
    table_col_order = {}
    fk_tables = set()
    with open(defs_csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row['table_name']
            col = row['name']
            col_type = row['type']
            if table not in table_schema_map:
                table_schema_map[table] = {}
                table_col_order[table] = []
            table_schema_map[table][col] = col_type
            table_col_order[table].append(col)
            if table != 'dgos':
                fk_tables.add(table)
        # Ensure dgoid is present in all tables
        for table in table_schema_map:
            table_schema_map[table]['dgoid'] = 'INTEGER'
            if 'dgoid' not in table_col_order[table]:
                table_col_order[table].insert(0, 'dgoid')
    return table_schema_map, table_col_order, fk_tables


def download_csv_from_s3(s3_bucket: str, s3_key: str, local_path: str) -> None:
    """
    Download a CSV file from S3 to a local path.
    """
    log = Logger('Download CSV')
    s3 = boto3.client('s3')
    log.info(f"Downloading {s3_key} from bucket {s3_bucket} to {local_path}")
    s3.download_file(s3_bucket, s3_key, local_path)
    log.info("Download complete.")


def create_geopackage(gpkg_path: str, table_schema_map: dict, table_col_order: dict, fk_tables: set) -> apsw.Connection:
    """
    Create a GeoPackage (SQLite) file and tables as specified in table_schema_map.
    Returns the APSW connection.

    IMPLEMENTED: Geometry column for dgo_geom in dgos table (as TEXT for now, TODO: convert to spatialite geometry).
    IMPLEMENTED: Foreign key syntax for dgoid in child tables.
    TODO: Enable spatialite extension and create geometry columns properly.
    """
    log = Logger('Create GeoPackage')
    log.info(f"Creating GeoPackage at {gpkg_path}")
    conn = apsw.Connection(gpkg_path)
    curs = conn.cursor()
    # TODO: Enable spatialite extension and initialize spatial metadata
    for table, columns in table_schema_map.items():
        col_defs = []
        for col, coltype in columns.items():
            if table == 'dgos' and col == 'dgo_geom':
                # TODO: Use geometry type and spatialite
                col_defs.append(f"{col} TEXT")  # Placeholder
            else:
                col_defs.append(f"{col} {coltype}")
        # Add FK syntax for child tables
        fk = ''
        if table in fk_tables:
            fk = ', FOREIGN KEY(dgoid) REFERENCES dgos(dgoid)'
        curs.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)}{fk})")
        log.info(f"Created table {table} with columns: {list(columns.keys())}")
    return conn


def wkt_from_csv(csv_geom: str) -> str:
    """
    Convert geometry string from CSV (with | instead of ,) back to WKT.
    """
    if not csv_geom:
        return None
    return csv_geom.replace('|', ',')


def populate_tables_from_csv(csv_path: str, conn: apsw.Connection, table_schema_map: dict, table_col_order: dict) -> None:
    """
    Read the CSV and insert rows into the appropriate tables based on column mapping.

    IMPLEMENTED: Sequential dgoid, geometry handling for dgo_geom, error handling, debug output for skipped/invalid rows.
    TODO: Use spatialite GeomFromText for geometry column, batch inserts for large CSVs, advanced validation.
    """
    log = Logger('Populate Tables')
    log.info(f"Populating tables from {csv_path}")
    curs = conn.cursor()
    dgoid_counter = 1
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        conn.execute('BEGIN')
        for idx, row in enumerate(reader):
            try:
                # Generate sequential dgoid
                dgoid = dgoid_counter
                dgoid_counter += 1
                for table, columns in table_schema_map.items():
                    values = []
                    for col in table_col_order[table]:
                        if col == 'dgoid':
                            values.append(dgoid)
                        elif col == 'dgo_geom' and table == 'dgos':
                            wkt = wkt_from_csv(row.get('dgo_geom', ''))
                            if not wkt:
                                raise ValueError(f"Missing or malformed geometry in row {idx}")
                            # TODO: Use spatialite GeomFromText(wkt, 4326) for geometry column
                            values.append(wkt)
                        else:
                            val = row.get(col, None)
                            # Check for required columns (notnull)
                            if val is None:
                                raise ValueError(f"Missing required column '{col}' in row {idx}")
                            values.append(val)
                    placeholders = ', '.join(['?'] * len(table_col_order[table]))
                    curs.execute(f"INSERT INTO {table} ({', '.join(table_col_order[table])}) VALUES ({placeholders})", values)
                if idx % 10000 == 0:
                    log.info(f"Inserted {idx} rows...")
            except Exception as e:
                log.error(f"Row {idx} skipped: {e}\nRow data: {row}")
        conn.execute('COMMIT')
    log.info("Table population complete.")


def main():
    """
    Main entry point: parses arguments, downloads CSV, parses table defs, creates GeoPackage, and populates tables.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('s3_bucket', help='S3 bucket containing the CSV file', type=str)
    parser.add_argument('s3_key', help='S3 key/path to the CSV file', type=str)
    parser.add_argument('output_gpkg', help='Path to output GeoPackage file', type=str)
    parser.add_argument('table_defs_csv', help='Path to rme_table_column_defs.csv', type=str)
    args = dotenv.parse_args_env(parser)

    log = Logger('Setup')
    log.setup(log_level=logging.INFO)

    temp_csv = 'temp_input.csv'
    download_csv_from_s3(args.s3_bucket, args.s3_key, temp_csv)
    table_schema_map, table_col_order, fk_tables = parse_table_defs(args.table_defs_csv)
    conn = create_geopackage(args.output_gpkg, table_schema_map, table_col_order, fk_tables)
    populate_tables_from_csv(temp_csv, conn, table_schema_map, table_col_order)
    log.info('Process complete.')


if __name__ == '__main__':
    main()
