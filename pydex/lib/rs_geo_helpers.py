import apsw
import json
from rsxml import Logger

def get_bounds(
        bounds_gpkg: str, 
        spatialite_path: str, 
        bounds_layer: str = 'project_bounds'
        ) -> tuple[dict, tuple[float, float], tuple[float, float, float, float]]:
    """
    Union all the polygons in the bounds_gpkg layer named project_bounds
    Simplifies as well. 
    Using spatialite 

    Returns: GeoJSOn dictionary, the centroid, the bounding box

    This was copied from scrape_rme2.py and then modified
    There is also code in riverscapes tools that does similar work:
      https://github.com/Riverscapes/riverscapes-tools/blob/master/lib/commons/rscommons/project_bounds.py
    And in QRAVE https://github.com/Riverscapes/QRAVEPlugin/blob/master/src/frm_project_bounds.py 
    Todo: consolidate or at least annotate the other versions, so we don't repeat yourself (DRY)
    """
    log = Logger('get bounds')
    conn = apsw.Connection(bounds_gpkg)
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    curs = conn.cursor()
    log.debug (f'Connected with spatialite to {bounds_gpkg}')
    simplification = '0.00001' # 0.01 was what PB used but for my test it returned null result
    bounds_query = f'''
        SELECT AsGeoJSON(union_geom) AS geojson,
            ST_X(ST_Centroid(union_geom)),
            ST_Y(ST_Centroid(union_geom)),
            ST_MinX(union_geom),
            ST_MinY(union_geom),
            ST_MaxX(union_geom),
            ST_MaxY(union_geom) FROM (
                SELECT ST_Simplify(ST_Buffer(ST_Union(ST_Buffer(CastAutomagic(geom), 0.001)), -0.001), {simplification}) union_geom 
                 FROM {bounds_layer}
            )'''
    curs.execute(bounds_query)
    
    bounds_row = curs.fetchone()
    if bounds_row is None:
        raise ValueError(f"No rows returned from the database query. Check if '{bounds_layer}' table has data.")

    geojson_geom = json.loads(bounds_row[0])
    centroid = (float(bounds_row[1]), float(bounds_row[2]))
    bounding_box = (
        float(bounds_row[3]),
        float(bounds_row[4]),
        float(bounds_row[5]),
        float(bounds_row[6])
    )

    geojson_output = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": geojson_geom,
            "properties": {}
        }]
    }

    return geojson_output, centroid, bounding_box
