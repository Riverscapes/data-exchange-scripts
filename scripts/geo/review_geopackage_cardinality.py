"""purpose:  traverse a directory with geopackages,
run something on each one, and report what we find
"""
import os
import sqlite3
import pandas as pd
from rsxml import ProgressBar

# Configuration
ROOT_DIR = r"F:\nardata\work\rme_extraction\rme-athena\downloads"
GPKG_NAME = "riverscapes_metrics.gpkg"
TABLES = [
    "dgo_geomorph",
    "dgo_veg",
    "dgo_hydro",
    "dgo_impacts",
    "dgo_beaver",
    "dgos",
    "dgo_desc"
]
OUTPUT_CSV = "gpkg_cardinality_report.csv"


def find_files_matching_name(root_dir, file_name):
    """return list of paths for all files matching file_name in root_dir and all subdirectories"""
    gpkg_files = []
    for dirpath, _, files in os.walk(root_dir):
        if file_name in files:
            gpkg_files.append(os.path.join(dirpath, file_name))
    return gpkg_files


def get_grandparent_id(gpkg_path):
    """get the name of the path's grandparent folder"""
    # e.g. `...\1002000101\outputs\riverscapes_metrics.gpkg` -> 1002000101
    return os.path.basename(os.path.dirname(os.path.dirname(gpkg_path)))


def get_spatialite_path():
    return os.environ.get("SPATIALITE_LIB")


def connect_with_spatialite(gpkg_path):
    """return sqlite connection with spatialite enabled"""
    conn = sqlite3.connect(gpkg_path)
    spatialite_path = get_spatialite_path()
    if spatialite_path:
        conn.enable_load_extension(True)
        conn.load_extension(spatialite_path)
    else:
        raise FileNotFoundError(f'Could not find {spatialite_path}')
    return conn


def measure_table_counts(gpkg_path, tables):
    counts = {}
    try:
        conn = connect_with_spatialite(gpkg_path)
        cur = conn.cursor()
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
            except Exception:
                counts[table] = None
        conn.close()
    except Exception:
        counts = {table: None for table in tables}
    return counts


def count_dgos_polygons(gpkg_path):
    """count of records for our dgos selection query"""
    try:
        conn = connect_with_spatialite(gpkg_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM (
                    SELECT
                    dgoid,
                    st_union(CastAutomagic(dgos.geom)) dgo_geom,
                    level_path,
                    seg_distance,
                    centerline_length,
                    segment_area,
                    FCode
                FROM dgos
                GROUP BY level_path, seg_distance
            )
        """)
        result = cur.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        print(f"Error counting dgos polygons in '{gpkg_path}': {e}")
        return None


def measure_table_cardinality(gpkg_path: str) -> dict:
    counts = measure_table_counts(gpkg_path, TABLES)
    dgos_poly_count = count_dgos_polygons(gpkg_path)
    count_values = [v for v in counts.values() if v is not None]
    all_equal = len(set(count_values)) == 1 and len(count_values) == len(TABLES)
    return {
        **counts,
        "dgos_poly_count": dgos_poly_count,
        "all_equal": all_equal
    }


def find_duplicate_lp_segdist(gpkg_path: str) -> dict:
    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()
    cur.execute("""
        WITH dupes AS (
            SELECT level_path, seg_distance
            FROM dgos
            WHERE level_path IS NOT NULL AND seg_distance IS NOT NULL
            GROUP BY level_path, seg_distance
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) FROM dupes;
    """)
    num_dupes = cur.fetchone()[0]
    conn.close()
    has_dupes = num_dupes > 0
    return {'hasduplicate': has_dupes}


def main():
    gpkg_files = find_files_matching_name(ROOT_DIR, GPKG_NAME)
    results = []
    _prg = ProgressBar(len(gpkg_files), 50, 'File progress')
    for i, gpkg_path in enumerate(gpkg_files):
        _prg.update(i)
        id_ = get_grandparent_id(gpkg_path)
        result = measure_table_cardinality(gpkg_path)
        # result = find_duplicate_lp_segdist(gpkg_path)
        results.append({"id": id_, **result})
        if i == 100:  # just sample
            break
    _prg.finish()
    # Write to CSV
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"results written to {OUTPUT_CSV}")
    # df2 = df.groupby('hasduplicate').count()
    # print(df2)


if __name__ == "__main__":
    main()
