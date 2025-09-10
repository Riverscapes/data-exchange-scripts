"""
Extract raw_rme data from athena for an area of interest (shape)
Because athena doesn't have spatial indexes we do a pre-selection on bounding box

Lorin Gaertner 
August 2025

1. get bounding box of aoi
2. buffer it 
3. sql prequery on lat lon within buffered bounding box
4. get the shape in queryable format
    option 1 (chosen) - produce and use wkt or wkb string for query
    option 2 - upload the shape to s3/create athena table
5. query the dgos for the actual aoi
    option 1 (chosen) - (most accurate way) query st_intersects aoi and dgo geometry 
    option 2 - (faster way) query dgo point within aoi 

Output can then be further processed with athena_to_rme.py

Assumption: 
* provided a geojson in epsg 4326

Future enhancements: 
* attach attributes from aoi to result - especially useful for multi-polygon aoi 
* provide ability to simplify the shape if it is not simple enough to handle in query 
* check for gaps in raw_rme coverage (query huc10_geom and vw_projects)
"""

import geopandas as gpd
# from shapely.geometry import box
from pydex.lib.athena import athena_query, parse_athena_results
from rsxml import Logger

# buffer, in decimal degrees, on centroids to capture DGO. 
# value of 0.47 is based on an analysis of distance between centroid and corners of bounding boxes of raw_rme 2025-09-08
BUFFER_CENTROID_TO_BB_DD = 0.47 

def simplify_aoi_gdf(gdf: gpd.GeoDataFrame, tolerance_meters: float = 11) -> gpd.GeoDataFrame:
    # Reproject to a projected CRS (EPSG:5070 is good for CONUS)
    gdf_proj = gdf.to_crs(epsg=5070)
    # Simplify geometries in projected CRS (tolerance in meters)
    gdf_proj["geometry"] = gdf_proj.geometry.simplify(tolerance=tolerance_meters, preserve_topology=True)
    # Reproject back to EPSG:4326 for Athena
    gdf_simplified = gdf_proj.to_crs(epsg=4326)
    return gdf_simplified

def get_aoi_geom_sql_expression(gdf: gpd.GeoDataFrame, max_size_bytes=261000) -> str | None: 
    """check the geometry, union 
    maximum size of athena query is 256 kb or 262144 bytes
    the default max_size assumes we need just under 5000 bytes for the rest of the query so this part needs to be under
    if it is too big returns None 
    """
    log = Logger('check aoi')
    # Get the union of all geometries in the GeoDataFrame
    aoi_geom = gdf.union_all()

    # Convert to WKT
    aoi_wkt = aoi_geom.wkt
    wkt_length = len(aoi_wkt.encode('utf-8')) + 23 # this is to account for the text ST_GeometryFromText('')

    # WKB representation (as hex string)
    aoi_wkb = aoi_geom.wkb.hex()
    wkb_length = len(aoi_wkb) + 31 # this is to account for the text ST_GeomFromBinary(from_hex(''))

    log.debug(f"WKT length: {wkt_length} bytes")
    log.debug(f"WKB hex length: {wkb_length} bytes")

    if wkt_length > max_size_bytes and wkb_length > max_size_bytes:
        log.error(f"aoi geometry results in too big a query (greater than {max_size_bytes} bytes) - simplify it and try again")
        return
    
    # return the smaller representation as an SQL expression for Athena 
    if wkt_length <= wkb_length:
        log.info("returning WKT as it is smaller ")
        return f"ST_GeometryFromText('{aoi_wkt}')"
    else:
        return f"ST_GeomFromBinary(from_hex('{aoi_wkb}'))"

def generate_sql_where_clause_for_bounds(gdf: gpd.GeoDataFrame) -> str:
    """
    Get the total bounds (minx, miny, maxx, maxy)
    'buffer' by BUFFER_CENTROID_TO_BB_DD and 
    return SQL where clause for latitude and longitude within this expanded box
    we round to 6 decimal places (<10cm) to control size of the SQL string 

    Assumes the geojson is in decimal degrees 
    Todo: check the assumption 
      
    Note tried buffering the bounding box with buffer but 
    this produces rounded corners and smooth edges - no longer a straightforward box
    # from shapely.geometry import box
    # Create a bounding box geometry
    # bbox = box(minx, miny, maxx, maxy)

    # print("Unbuffered bounding box WKT:", bbox.wkt)
  
    # buffered_bbox = bbox.buffer(0.01)
    # print("Buffered bounding box WKT:", buffered_bbox.wkt)
    """

    # these are fun variable names :-)
    minx, miny, maxx, maxy = gdf.total_bounds
    bufminx = round(float(minx) - BUFFER_CENTROID_TO_BB_DD,6)
    bufminy = round(float(miny) - BUFFER_CENTROID_TO_BB_DD,6)
    bufmaxx = round(float(maxx) + BUFFER_CENTROID_TO_BB_DD,6)
    bufmaxy = round(float(maxy) + BUFFER_CENTROID_TO_BB_DD,6)

    bounds_where_clause = f'WHERE (latitude between {bufminy} AND {bufmaxy}) AND (longitude between {bufminx} AND {bufmaxx})'
    return bounds_where_clause

def run_aoi_athena_query(path_to_shape: str, s3_bucket: str) -> str | None:
    """Run Athena query on supplied AOI geojson and return path to results
    returns None if the shape can't be converted to suitably sized geometry sql expression
    """
    log=Logger('Run AOI Query on Athena')
    # Read the GeoJSON
    gdf = gpd.read_file(path_to_shape)

    # we are going to filter the centroids, but clip to polygon
    # So we need to buffer by the maximum distance between a centroid and its bounding polygon 
    prefilter_where_clause = generate_sql_where_clause_for_bounds(gdf)

    # count the prefiltered records - uncomment for debugging only
    query_str = f'SELECT count(*) AS record_count FROM raw_rme {prefilter_where_clause}'
    raw_results = (athena_query(s3_bucket, query_str))
    log.debug (f'Prefiltered records: {parse_athena_results(raw_results)}')

    # Try with original geometry
    aoi_geom_str = get_aoi_geom_sql_expression(gdf)
    simplified = False

    # If too large, try with simplified geometry
    if not aoi_geom_str:
        log.info("AOI geometry too large, simplifying and retrying...")
        gdf = simplify_aoi_gdf(gdf, tolerance_meters=11)  # Adjust tolerance as needed
        aoi_geom_str = get_aoi_geom_sql_expression(gdf)
        simplified = True

    if not aoi_geom_str:
        log.error('Could not create suitable geometry from supplied value, even after simplification.')
        return

    log.debug(f'aoi_geo_str is {len(aoi_geom_str)} long ({"simplified" if simplified else "original"})')

    # For a query to pull just the lat/lon of DGOs that intersect, use this instead
    # fields_str = "longitude, latitude"
    # query to get all the details _except_ the dgo geometry
    fields_str = "rme_version, rme_version_int, rme_date_created_ts, level_path, seg_distance, centerline_length, segment_area, fcode, longitude, latitude, ownership, state, county, drainage_area, watershed_id, stream_name, stream_order, headwater, stream_length, waterbody_type, waterbody_extent, ecoregion3, ecoregion4, elevation, geology, huc12, prim_channel_gradient, valleybottom_gradient, rel_flow_length, confluences, diffluences, tributaries, tribs_per_km, planform_sinuosity, lowlying_area, elevated_area, channel_area, floodplain_area, integrated_width, active_channel_ratio, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, confinement_ratio, constriction_ratio, confining_margins, constricting_margins, lf_evt, lf_bps, lf_agriculture_prop, lf_agriculture, lf_conifer_prop, lf_conifer, lf_conifer_hardwood_prop, lf_conifer_hardwood, lf_developed_prop, lf_developed, lf_exotic_herbaceous_prop, lf_exotic_herbaceous, lf_exotic_tree_shrub_prop, lf_exotic_tree_shrub, lf_grassland_prop, lf_grassland, lf_hardwood_prop, lf_hardwood, lf_riparian_prop, lf_riparian, lf_shrubland_prop, lf_shrubland, lf_sparsely_vegetated_prop, lf_sparsely_vegetated, lf_hist_conifer_prop, lf_hist_conifer, lf_hist_conifer_hardwood_prop, lf_hist_conifer_hardwood, lf_hist_grassland_prop, lf_hist_grassland, lf_hist_hardwood_prop, lf_hist_hardwood, lf_hist_hardwood_conifer_prop, lf_hist_hardwood_conifer, lf_hist_peatland_forest_prop, lf_hist_peatland_forest, lf_hist_peatland_nonforest_prop, lf_hist_peatland_nonforest, lf_hist_riparian_prop, lf_hist_riparian, lf_hist_savanna_prop, lf_hist_savanna, lf_hist_shrubland_prop, lf_hist_shrubland, lf_hist_sparsely_vegetated_prop, lf_hist_sparsely_vegetated, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, riparian_veg_departure, ag_conversion, develop, grass_shrub_conversion, conifer_encroachment, invasive_conversion, riparian_condition, qlow, q2, splow, sphigh, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, brat_capacity, brat_hist_capacity, brat_risk, brat_opportunity, brat_limitation, brat_complex_size, brat_hist_complex_size, dam_setting"
    query_str = f"""
    WITH pre_filtered_rme AS (
        SELECT
            {fields_str}
            , ST_GeometryFromText(REPLACE(raw_rme.dgo_geom, '|', ',')) AS dgo_geom_obj
        FROM
            raw_rme
        {prefilter_where_clause}
        )
    SELECT
        {fields_str}
    FROM 
        pre_filtered_rme AS t1
    WHERE 
        ST_Intersects(
            t1.dgo_geom_obj,
            {aoi_geom_str}
        );
    """

    # Debugging output
    log.debug(f'Query is {len(query_str.encode('utf-8'))} bytes')
    log.debug(f"Query starts with: {query_str[:50]}")
    log.debug(f"Query ends with: {query_str[-30:]}")
    # print("Full query:")
    # print(query_str)
    # with open("athena_query.sql", "w", encoding="utf-8") as f:
    #     f.write(query_str)

    results = athena_query(s3_bucket, query_str, return_output_path=True)
    return results

def main():
    """get an AOI geometry and query athena raw_rme for data within"""
    # path_to_shape = r"C:\nardata\work\rme_extraction\20250827-rkymtn\physio_rky_mtn_system.geojson"
    path_to_shape = r"C:\nardata\work\rme_extraction\Price-riv\pricehuc10s.geojson"
    s3_bucket = "riverscapes-athena"

    path_to_results = run_aoi_athena_query(path_to_shape, s3_bucket)
    print(path_to_results)

if __name__ == '__main__':
    main()
