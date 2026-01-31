# RME to Athena Pipeline Changelog

## 1.1

* Added: New Parquet files generated from geopackage now includes geometry_simplified column
* Changed: Metadata layer_definitions.json updated to 0.8 schema

## 1.0

* Used for CONUS run scrape
* the parquet results were later augmented with a simplified geometry version using add_simplified_geom_pq.py (without going back to source data in data exchange)
