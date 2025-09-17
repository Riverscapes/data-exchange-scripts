# https://geopandas.org/en/latest/docs/reference/api/geopandas.GeoSeries.simplify_coverage.html

# import geopandas as gpd

# # Load layer
# gdf = gpd.read_file("/Users/philipbailey/GISData/watershed_boundaries/simplification_experiment/huc10_conus_single_5070.gpkg")

# # Simplify geometries (preserve topology)
# # gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.simplify(tolerance=5000, preserve_topology=True))

# # Simplify while preserving shared boundaries
# simplified = simplify_coverage(gdf, tolerance=10)

# # Save result
# simplified.to_file("simplified_polygons.gpkg", driver="GPKG")

# # Save result
# gdf.to_file("/Users/philipbailey/GISData/watershed_boundaries/simplification_experiment/simplified_layer2.gpkg", driver="GPKG")


import shapely
import geopandas as gpd

print ("'simplify_coverage' requires shapely>=2.1 and GEOS>=3.12.")
print(f"Shapely: {shapely.__version__}")
print(f"Geopandas: {gpd.__version__}")

# Read the GeoPackage
in_file = r"C:\nardata\work\huc_wbd_nhd_align\final\lsg_processed_hu10.gpkg"
in_layer = 'wbdhu10_conus_rs'
gdf = gpd.read_file(in_file, layer=in_layer)
print(f'Read {in_layer} from {in_file}.')

# Reproject to EPSG:5070
gdf = gdf.to_crs(epsg=5070)

# Simplify all geometries using simplify_coverage on the GeoSeries
gdf["geometry"] = gdf.geometry.simplify_coverage(tolerance=8000)

# Select only the desired columns (geometry is always included if present)
gdf_out = gdf[["TNMID", "HUC10", "geometry"]]
# change (back?) to 4326 for Athena
gdf_out.to_crs(epsg=4326)

# Save the result
out_file=r"C:\nardata\work\huc_wbd_nhd_align\final\simplified_hu10.gpkg"
out_layer = 'wbdhu10_conus_rs_simplified_8km'
gdf_out.to_file(out_file, layer=out_layer, driver="GPKG")
print(f"wrote {out_layer} to {out_file}.")

