"""
Reads a CSV file (optionally, first copies it from S3) and generates a GeoPackage (SQLite) file with tables
such as dgos, dgos_veg, dgo_hydro, etc.,
column-to-table-and-type mapping extracted from rme geopackage pragma
into rme_table_column_defs.csv. All tables will include a sequentially generated dgoid column.

Lorin Gaertner (with copilot)
August 2025

IMPLEMENTED: Sequential dgoid (integer), geometry handling for dgo_geom (SRID 4326, WKT conversion), foreign key syntax in table creation, error handling (throws on missing/malformed required columns), debug output for skipped/invalid rows.
Actual column names/types must be supplied in rme_table_column_defs.csv. 
Geometry column creation and spatialite extension loading are stubbed (see TODOs). 
No batching/optimization for very large CSVs - but it handled 1.5 M records okay as is.
No advanced validation or transformation beyond geometry and required columns.
Foreign key constraints are defined but not enforced unless PRAGMA foreign_keys=ON is set.
"""

from datetime import datetime
import os
import csv
import logging
import argparse
import boto3
import apsw
import json
import tempfile
from pydex.lib.rs_geo_helpers import get_bounds
from rsxml import dotenv, Logger, ProgressBar
from rsxml.util import safe_makedirs
from rsxml.project_xml import (
    Project,
    MetaData,
    Meta,
    ProjectBounds,
    Coords,
    BoundingBox,
    Realization,
    Geopackage,
    GeopackageLayer,
    GeoPackageDatasetTypes,
    Dataset
)

GEOMETRY_COL_TYPES = ('MULTIPOLYGON','POINT')

def parse_table_defs(defs_csv_path):
    """
    Parse rme_table_column_defs.csv and return a dict of table schemas; a list of column order; list of foreign key tables
    Returns: {table_name: {col_name: col_type, ...}, ...}, {table_name: [col_order]}, set(table_names)
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

def download_file_from_s3(s3_uri: str, local_path: str) -> None:
    """
    Download a file from S3 to a local path.

    Args:
        s3_uri (str): S3 URI of the file, e.g. 's3://bucket/key'.
        local_path (str): Local filesystem path to save the downloaded file.

    Raises:
        ValueError: If s3_uri is not a valid S3 URI.

    Example:
        download_file_from_s3('s3://riverscapes-athena/adhoc/yct_sample4.csv', '/tmp/yct_sample4.csv')
    """
    log = Logger('Download File')
    # Validate and parse S3 URI
    if not isinstance(s3_uri, str) or not s3_uri.startswith('s3://'):
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with 's3://'")
    parts = s3_uri[5:].split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must be in format 's3://bucket/key'")
    s3_bucket, s3_key = parts

    log.info(f"Downloading {s3_key} from bucket {s3_bucket} to {local_path}")
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=s3_bucket, Key=s3_key)
    size_bytes = response['ContentLength']
    log.info(f"ContentType: {response['ContentType']}\t File size: {size_bytes} bytes")
    # TODO if very large, add progress bar
    s3.download_file(s3_bucket, s3_key, local_path)
    log.info("Download complete.")

def create_geopackage(gpkg_path: str, table_schema_map: dict, table_col_order: dict, fk_tables: set, spatialite_path: str) -> apsw.Connection:
    """
    Create a GeoPackage (SQLite) file and tables as specified in table_schema_map. 
    Returns the APSW connection.
    """
    log = Logger('Create GeoPackage')
    log.info(f"Creating GeoPackage at {gpkg_path}")
    # apsw.Connection = The database is opened for reading and writing, and is created if it does not already exist. 
    # this can be a problem if this script is run more than once
    # Delete the file if it exists
    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)
        log.info(f"Deleted existing file {gpkg_path}")

    conn = apsw.Connection(gpkg_path)
    # Enable spatialite extension and initialize spatial metadata
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    add_geopackage_tables(conn)
    curs = conn.cursor()
    # create tables
    for table, columns in table_schema_map.items():
        col_defs = []
        for col in table_col_order[table]:
            coltype = columns[col]
            # treat dgoid specially
            if col == 'dgoid' and table == 'dgos':
                col_defs.append(f"{col} {coltype} PRIMARY KEY")
            # don't add geometry cols here - we'll do it 
            elif coltype not in GEOMETRY_COL_TYPES:
                col_defs.append(f"{col} {coltype}")

        # Add FK syntax for child tables
        fk = ''
        # if table in fk_tables:
        #     fk = ', FOREIGN KEY(dgoid) REFERENCES dgos(dgoid)'
        curs.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)}{fk})")
        log.info(f"Created table {table}")       

    # register relations 
    # for table in fk_tables:
    #     curs.execute("""
    #         INSERT INTO gpkg_relations (
    #             name, type, base_table_name, base_primary_column, related_table_name, related_primary_column
    #         ) VALUES (?, 'association', ?, ?, ?, ?)
    #     """, (
    #         f'dgos_{table}',
    #         'dgos', 'dgoid',
    #         table, 'dgoid'
    #     ))

    # add and register geometry columns
    for table, columns in table_schema_map.items():
        for col, coltype in columns.items():
            if coltype in GEOMETRY_COL_TYPES:
                curs.execute(
                    "SELECT gpkgAddGeometryColumn (?, ?, ?, 0, 0, 4326)",
                    (table, col, coltype)
                )
                curs.execute(
                    "SELECT gpkgAddSpatialIndex(?, ?);", 
                    (table, col)
                )
                log.info(f"Registered {table} as a spatial layer in GeoPackage with {col} as {coltype}.")

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
    """
    log = Logger('Populate Tables')
    log.info(f"Populating tables from {csv_path}")
    curs = conn.cursor()
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        conn.execute('BEGIN')
        try:
            for idx, row in enumerate(reader, start=1):
                # Use index as sequential dgoid -- assumes there is no existing data
                dgoid = idx
                for table, columns in table_schema_map.items():
                    insert_cols = []
                    insert_values = []
                    geom_col_idx = None
                    for col in table_col_order[table]:
                        coltype = columns[col]                        
                        if col == 'dgoid':
                            insert_cols.append(col)
                            insert_values.append(dgoid)
                        elif coltype in GEOMETRY_COL_TYPES:
                            insert_cols.append(col)
                            # assume the only col of this coltype is geom, which we will build from 
                            geom_wkt = f"POINT({row.get('longitude')} {row.get('latitude')})"
                            # assume the only col of this coltype is geom, which we want to pop with dgo_geom
                            # geom_wkt = wkt_from_csv(row.get('dgo_geom', ''))
                            # if not geom_wkt:
                            #     raise ValueError(f"Missing or malformed geometry in row {idx}")
                            insert_values.append(geom_wkt)
                        else:
                            val = row.get(col, None)
                            # Check for required columns (notnull)
                            if val is None:
                                raise ValueError(f"Missing required column '{col}' in row {idx}")
                                # alternatively, warn and keep going
                                # print (f"Missing required column '{col}' in row {idx}")
                                # values.append(None)
                            else:
                                insert_cols.append(col)
                                insert_values.append(val)
                    sql_placeholders = [] 
                    for i, col in enumerate (insert_cols):
                        coltype = columns[col]
                        if coltype in GEOMETRY_COL_TYPES:
                            sql_placeholders.append("AsGPB(GeomFromText(?, 4326))")
                        else:
                            sql_placeholders.append("?")
                    sqlstatement = f"INSERT INTO {table} ({', '.join(insert_cols)}) VALUES ({', '.join(sql_placeholders)})"
                    # uncomment for debug printing first SQL statement - noisy
                    # if idx == 1: 
                    #     log.debug(sqlstatement)
                    #     log.debug(insert_values)
                    curs.execute(sqlstatement, insert_values)
                # TODO (enhance) use progress bar instead of this status message
                # thought to use file size and tell() but that doesn't work - would have to read the number of rows first
                # need to check if that slows things down much - probably not
                if idx % 10000 == 0:
                    log.info(f"Inserted {idx} rows.")
            conn.execute('COMMIT')
            log.info(f"Inserted {idx} rows.")    
        except Exception as e:
            log.error(f"Error at row {idx}: {e}\nRow data: {row}\nSQL: {sqlstatement}")
            conn.execute('ROLLBACK')
            raise             
    log.info("Table population complete.")

def add_geopackage_tables(conn: apsw.Connection):
    """
    Create required GeoPackage spatial_ref_sys and metadata tables: gpkg_contents and gpkg_geometry_columns.
    # initially I inserted with CREATE TABLE statements but the single spatialite function `gpkgCreateBaseTables` does all this 
    the Spatialite function gpkgInsertEpsgSRID(4326) is not needed because CreateBaseTables inserts that one already 
    """
    curs = conn.cursor()
    curs.execute("SELECT gpkgCreateBaseTables();")
    
    # commented out because defined relationships not required and setting them up is more work
    # copilot says the GeoPackage specification does not include the gpkg_relations table by default, and the Spatialite function gpkgCreateBaseTables() does not create it
    # so we create it to avoid warning in qgis 
    # curs.execute("""
    #     CREATE TABLE IF NOT EXISTS gpkg_relations (
    #         id INTEGER PRIMARY KEY AUTOINCREMENT,
    #         name TEXT NOT NULL,
    #         type TEXT NOT NULL,
    #         mapping_table_name TEXT,
    #         base_table_name TEXT NOT NULL,
    #         base_primary_column TEXT NOT NULL,
    #         related_table_name TEXT NOT NULL,
    #         related_primary_column TEXT NOT NULL
    #     );
    # """)

def get_datasets(output_gpkg: str) -> list[GeopackageLayer]:
    """
    Returns a list of the datasets from the output GeoPackage.
    These are the spatial views that are created from the igos and dgos tables.
    ***COPIED FROM scrape_rme2.py***
    """

    conn = apsw.Connection(output_gpkg)
    conn.enable_load_extension(True)
    curs = conn.cursor()

    # Get the names of all the tables in the database
    curs.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features'")
    datasets = [GeopackageLayer(
        lyr_name=row[0],
        ds_type=GeoPackageDatasetTypes.VECTOR,
        name=row[0]
    ) for row in curs.fetchall()]
    return datasets

def create_igos_project(project_dir: str, project_name: str, spatialite_path: str, gpkg_path: str, log_path: str):
    """
    Create a Riverscapes project of type 
    Modelled after scrape_rme2.py 

    
    """
    log = Logger ('Create IGOS project')
    # Build the bounds for the new RME scrape project
    # dgos table has point geom but buffering them works
    # Alternate approach : we could use the AOI that was used to select the dgos to begin with
    bounds, centroid, bounding_rect = get_bounds(gpkg_path, spatialite_path, bounds_layer='dgos')
    output_bounds_path = os.path.join(project_dir, 'project_bounds.geojson')
    with open(output_bounds_path, "w", encoding='utf8') as f:
        json.dump(bounds, f, indent=2)
    print(f"centroid = {centroid}")
    rs_project = Project(
        project_name,
        project_type='igos',
        description=f"""This project was generated as an extract from raw_rme which is itself an extract of Riverscapes Metric Engine projects in the Riverscapes Data Exchange produced as part of the 2025 CONUS run of Riverscapes tools. See https://docs.riverscapes.net/initiatives/CONUS-runs for more about this initiative. 
        At the time of extraction this dataset has *not* yet been thoroughly quality controlled and may contain errors or gaps. 
        """,
        bounds=ProjectBounds(
            Coords(centroid[0], centroid[1]),
            BoundingBox(bounding_rect[0], bounding_rect[1], bounding_rect[2], bounding_rect[3]),
            os.path.basename(gpkg_path)),
            realizations=[Realization(
                name='Realization1',
                xml_id='REALIZATION1',
                date_created=datetime.now(),
                product_version='1.0.0',
                datasets=[
                    Geopackage(
                        name='Riverscapes Metrics',
                        xml_id='RME',
                        path=os.path.relpath(gpkg_path, project_dir),
                        layers=get_datasets(gpkg_path)
                    ),
                    Dataset(
                        xml_id='LOG',
                        ds_type='LogFile',
                        name='Lof File',
                        description='Processing log file',
                        path=os.path.relpath(log_path, project_dir),
                    ),
                ]
            )]
    )
    merged_project_xml = os.path.join(project_dir, 'project.rs.xml')
    rs_project.write(merged_project_xml)
    log.info(f'Project XML file written to {merged_project_xml}')

def create_views(conn: apsw.Connection, table_col_order: dict):
    """
    create spatial views for each attribute table joined with the dgos (spatial) table 
    also creates a combined view 'vw_dgo_metrics' joining all attribute tables and `dgos`
    Registers each view in gpkg_contents and gpkg_geometry_columns so GIS tools recognize them as spatial layers.
    modeled after clean_up_gpkg in scrape_rme2 but using the dgo geom (which is a point)
    """
    log = Logger ('Create views')
    curs = conn.cursor()
    
    dgo_tables = [t for t in table_col_order.keys() if t != 'dgos']
    log.debug(f'Attribute tables to create views for: {dgo_tables}')

    # Get the columns from the dgos table
    curs.execute('PRAGMA table_info(dgos)')
    dgo_cols = [f"dgos.{col[1]}" for col in curs.fetchall() if col[1] != 'dgoid' and col[1] != 'DGOID']

    new_views = ['vw_dgo_metrics']
    # we don't have IGO table but we put the point geom in the DGO geom - same idea
    # I'm a little confused about whether we have DGO metrics or IGO metrics or some mix in raw_rme
    curs.execute('''
        CREATE VIEW vw_dgo_metrics AS
        SELECT dgo_desc.*, dgo_geomorph.*, dgo_veg.*, dgo_hydro.*, dgo_impacts.*, dgo_beaver.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
        FROM dgo_desc
        INNER JOIN dgo_geomorph ON dgo_desc.dgoid = dgo_geomorph.dgoid
        INNER JOIN dgo_veg ON dgo_desc.dgoid = dgo_veg.dgoid
        INNER JOIN dgo_hydro ON dgo_desc.dgoid = dgo_hydro.dgoid
        INNER JOIN dgo_impacts ON dgo_desc.dgoid = dgo_impacts.dgoid
        INNER JOIN dgo_beaver ON dgo_desc.dgoid = dgo_beaver.dgoid
        INNER JOIN dgos ON dgo_desc.dgoid = dgos.dgoid
        '''
        )

    for dgo_table in dgo_tables:
        # Get the columns of the dgo side table
        curs.execute(f'PRAGMA table_info({dgo_table})')
        dgo_table_cols = ['t.' + col[1] for col in curs.fetchall() if col[1] != 'dgoid' and col[1] != 'DGOID']
 
        view_name = f'vw_{dgo_table}_metrics'
        new_views.append(view_name)
        curs.execute(f'''
            CREATE VIEW {view_name} AS
            SELECT dgos.dgoid,
            dgos.geom,
            {",".join(dgo_table_cols)},
            {",".join(dgo_cols)}
            FROM {dgo_table} t INNER JOIN dgos ON t.dgoid=dgos.dgoid
        ''')
    # Register each view as a spatial layer in the GeoPackage metadata tables
    for view_name in new_views:
        curs.execute('''
            INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y)
            SELECT ?, 'features', ?, min_x, min_y, max_x, max_y FROM gpkg_contents WHERE table_name='dgos'
        ''', [view_name, view_name])

        curs.execute('''
            INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT ?, 'geom', 'POINT', 4326, 0, 0 FROM gpkg_geometry_columns WHERE table_name='dgos'
        ''', [view_name])
    log.info(f"{len(new_views)} views created and registered as spatial layers.")

def fix_s3_uri(argstr: str) -> str:
    """the parser is messing up s3 paths this should fix them
    launch.json value (a valid s3 string): "s3://riverscapes-athena/athena_query_results/d40eac38-0d04-4249-8d55-ad34901fee82.csv" 
    agrstr (input to this function) 's3:\\\\riverscapes-athena\\\\athena_query_results\\\\d40eac38-0d04-4249-8d55-ad34901fee82.csv'
    ouput: back to a valid s3 string 
    """
    import re
    # Replace all backslashes with slashes
    uri = argstr.replace("\\", "/")
    # Ensure exactly two slashes after 's3:'
    uri = re.sub(r'^s3:/+', 's3://', uri)
    return uri

def main():
    """
    Main entry point: parses arguments, downloads CSV, parses table defs, creates GeoPackage, and populates tables.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('raw_rme_csv_path', help='full path to csv file containing the raw_rme extract (can be s3 URI e.g. s3://riverscapes-athena/adhoc/yct_sample4.csv)')
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('table_defs_csv', help='Path to rme_table_column_defs.csv', type=str)
    # note rsxml.dotenv screws up s3 paths! we'll need to address that see issue #895 in RiverscapesXML repo - meanwhile we'll fix it 
    args = dotenv.parse_args_env(parser)
    # instead, can use standard parser. However this doesn't handle {env:DATA_ROOT} the way rsxml does
    # args = parser.parse_args()

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    download_folder = os.path.join(working_folder, 'downloads')
    project_dir = os.path.join(working_folder, 'project')  # , 'outputs', 'riverscapes_metrics.gpkg')
    safe_makedirs(project_dir)
    project_name = os.path.basename(args.raw_rme_csv_path)

    log = Logger('Setup')
    log_path = os.path.join(project_dir, 'rme-scrape.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)

    # parser.add_argument('output_gpkg', help='Path to output GeoPackage file', type=str)
    gpkg_path = os.path.join(project_dir, 'outputs', 'riverscape_metrics.gpkg') 
    safe_makedirs(os.path.join(project_dir,'outputs'))

    # csv can be either a local path or an s3 path. parse and handle accordingly
    # TODO (enhancement) - stream and process file without storing the tempfile
    if args.raw_rme_csv_path.startswith('s3:'):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
            local_csv = tmpfile.name
        s3_uri = fix_s3_uri(args.raw_rme_csv_path)
        download_file_from_s3(s3_uri, local_csv)
    else:
        local_csv = args.raw_rme_csv_path   

    table_schema_map, table_col_order, fk_tables = parse_table_defs(args.table_defs_csv)
    conn = create_geopackage(gpkg_path, table_schema_map, table_col_order, fk_tables, args.spatialite_path)
    populate_tables_from_csv(local_csv, conn, table_schema_map, table_col_order)
    create_views(conn, table_col_order)
    create_igos_project(project_dir, project_name, args.spatialite_path, gpkg_path, log_path)
    log.info('Process complete.')

if __name__ == '__main__':
    main()