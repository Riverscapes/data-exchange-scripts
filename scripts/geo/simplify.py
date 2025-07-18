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

print(gpd.__version__)
print(shapely.__version__)

# Read the GeoPackage
gdf = gpd.read_file("/Users/philipbailey/GISData/watershed_boundaries/simplification_experiment/huc10_conus_single_5070.gpkg")

# Simplify all geometries using simplify_coverage on the GeoSeries
gdf["geometry"] = gdf.geometry.simplify_coverage(tolerance=10000)

# Save the result
gdf.to_file("/Users/philipbailey/GISData/watershed_boundaries/simplification_experiment/simplified_layer2.gpkg", driver="GPKG")
