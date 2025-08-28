"""
Extract raw_rme data from athena for an area of interest (shape)
Because athena doesn't have spatial indexes we do a pre-selection on bounding box

Lorin Gaertner 
August 2025

1. get bounding box of aoi
2. buffer it 
3. sql query on lat lon within buffered bounding box
4. upload the shape to s3/create athena table
5 (more accurate way) query st_intersects aoi and dgo geometry 
5 (faster way) query dgo point within aoi 

Output can then be further processed with athena_to_rme.py

Assumption: 
* provided a geojson in epsg 4326

Future enhancements: 
* attach attributes from aoi to result - especially useful for multi-polygon aoi 
* check if shape is simple enough to handle in query without uploading it - and if not simplify it
* check for gaps in raw_rme coverage (query huc10_geom and vw_projects)
"""

def main():
    """get an AOI geometry and query athena raw_rme for data within"""
    path_to_shape = r"C:\"

if __name__ == '__main__':
    main()
