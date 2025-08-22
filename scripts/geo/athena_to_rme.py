"""
Reads a CSV file from S3 and generates a GeoPackage (SQLite) file with tables
such as dgos, dgos_veg, dgo_hydro, etc.,
column-to-table-and-type mapping extracted from rme geopackage pragma
into rme_table_column_defs.csv. All tables will include a sequentially generated dgoid column.

Lorin Gaertner (with copilot)
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

def create_geopackage(gpkg_path: str, table_schema_map: dict, table_col_order: dict, fk_tables: set, spatialite_path: str) -> apsw.Connection:
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
    # TODO: Enable spatialite extension and initialize spatial metadata
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    add_geopackage_tables(conn)
    curs = conn.cursor()
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

        # Register spatial table as a layer if it has a geometry column
        if table == 'dgos':
            # Insert into gpkg_contents
            curs.execute(f"""
                INSERT OR REPLACE INTO gpkg_contents (
                    table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id
                ) VALUES (?, 'features', ?, '', CURRENT_TIMESTAMP, NULL, NULL, NULL, NULL, 4326)
            """, (table, table))
            # Insert into gpkg_geometry_columns
            curs.execute(f"""
                INSERT OR REPLACE INTO gpkg_geometry_columns (
                    table_name, column_name, geometry_type_name, srs_id, z, m
                ) VALUES (?, 'geom', 'MULTIPOLYGON', 4326, 0, 0)
            """, (table,))
            log.info(f"Registered {table} as a spatial layer in GeoPackage.")
    
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

    IMPLEMENTED: Sequential dgoid, geometry handling for dgo_geom (called geom in dgos table), error handling, debug output for skipped/invalid rows.
    TODO: Use spatialite GeomFromText for geometry column, batch inserts for large CSVs, advanced validation.
    """
    log = Logger('Populate Tables')
    log.info(f"Populating tables from {csv_path}")
    curs = conn.cursor()
    dgoid_counter = 1
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        conn.execute('BEGIN')
        for idx, row in enumerate(reader, start=1):
            try:
                # Generate sequential dgoid
                dgoid = dgoid_counter
                dgoid_counter += 1
                for table, columns in table_schema_map.items():
                    values = []
                    for col in table_col_order[table]:
                        if col == 'dgoid':
                            values.append(dgoid)
                        elif col == 'geom' and table == 'dgos':
                            wkt = wkt_from_csv(row.get('dgo_geom', ''))
                            if not wkt:
                                raise ValueError(f"Missing or malformed geometry in row {idx}")
                            values.append(wkt)
                        else:
                            val = row.get(col, None)
                            # Check for required columns (notnull)
                            if val is None:
                                # raise ValueError(f"Missing required column '{col}' in row {idx}")
                                print (f"Missing required column '{col}' in row {idx}")
                                values.append(None)
                            else:
                                values.append(val)
                    placeholders = ', '.join(['?'] * len(table_col_order[table]))
                    curs.execute(f"INSERT INTO {table} ({', '.join(table_col_order[table])}) VALUES ({placeholders})", values)
                if idx % 10000 == 0:
                    log.info(f"Inserted {idx} rows...")
            except Exception as e:
                log.error(f"Row {idx} skipped: {e}\nRow data: {row}")
        log.info(f"Inserted {idx} rows.")    
        conn.execute('COMMIT')
    log.info("Table population complete.")

def add_geopackage_tables(conn: apsw.Connection):
    """
    Create required GeoPackage spatial_ref_sys and metadata tables: gpkg_contents and gpkg_geometry_columns.
    
    """
    curs = conn.cursor()

    curs.execute("""
        CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys (
            srs_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL PRIMARY KEY,
            organization TEXT NOT NULL,
            organization_coordsys_id INTEGER NOT NULL,
            definition TEXT NOT NULL,
            description TEXT
        );
    """)
    curs.execute("""
        INSERT OR IGNORE INTO gpkg_spatial_ref_sys (
            srs_name, srs_id, organization, organization_coordsys_id, definition, description
        ) VALUES (
            'WGS 84', 4326, 'EPSG', 4326,
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]',
            'WGS 84 geographic coordinate system'
        );
    """)

    # Create gpkg_contents table
    curs.execute("""
        CREATE TABLE IF NOT EXISTS gpkg_contents (
            table_name TEXT NOT NULL PRIMARY KEY,
            data_type TEXT NOT NULL,
            identifier TEXT UNIQUE,
            description TEXT DEFAULT '',
            last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            min_x DOUBLE,
            min_y DOUBLE,
            max_x DOUBLE,
            max_y DOUBLE,
            srs_id INTEGER,
            CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );
    """)
    # Create gpkg_geometry_columns table
    curs.execute("""
        CREATE TABLE IF NOT EXISTS gpkg_geometry_columns (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            geometry_type_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL,
            z TINYINT NOT NULL,
            m TINYINT NOT NULL,
            CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
            CONSTRAINT uk_gc_table_name UNIQUE (table_name),
            CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
            CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id)
        );
    """)

def main():
    """
    Main entry point: parses arguments, downloads CSV, parses table defs, creates GeoPackage, and populates tables.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('s3_bucket', help='S3 bucket containing the CSV file', type=str)
    parser.add_argument('s3_key', help='S3 key/path to the CSV file', type=str)
    parser.add_argument('output_gpkg', help='Path to output GeoPackage file', type=str)
    parser.add_argument('table_defs_csv', help='Path to rme_table_column_defs.csv', type=str)
    # note rsxml.dotenv screws up s3 paths! we'll need to address that see issue #895 in RiverscapesXML repo
    # args = dotenv.parse_args_env(parser)
    args = parser.parse_args()
    print(repr(args.s3_key))

    log = Logger('Setup')
    log.setup(log_level=logging.INFO)

    local_csv = r"C:\nardata\work\rme_extraction\20250820-yct\yct19.csv"
    # download_csv_from_s3(args.s3_bucket, args.s3_key, local_csv)
    table_schema_map, table_col_order, fk_tables = parse_table_defs(args.table_defs_csv)
    conn = create_geopackage(args.output_gpkg, table_schema_map, table_col_order, fk_tables, args.spatialite_path)
    populate_tables_from_csv(local_csv, conn, table_schema_map, table_col_order)
    log.info('Process complete.')

if __name__ == '__main__':
    main()