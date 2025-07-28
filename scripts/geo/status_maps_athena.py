"""
Generates a series of status maps for different project types
from a GeoPackage containing project data.

This script connects to an Athena database, retrieves project data,

Philip Bailey
12 July 2025
"""
import os
import argparse
from typing import List
from datetime import datetime
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import boto3
from pyathena import connect
from shapely import wkt


def generate_status_maps(athena_output_dir: str, output_image_dir: str, output_gpkgs: bool) -> List[str]:
    """
    Generate status maps for different project types and save them as PNG images.
    """

    # Ensure output directory exists
    os.makedirs(output_image_dir, exist_ok=True)
    os.makedirs(output_image_dir, exist_ok=True)

    conn = connect(s3_staging_dir=athena_output_dir, region_name='us-west-2')

    # Get the number of CONUS HUCs. Use the official HUC table for this
    huc10_count_df = pd.read_sql('SELECT COUNT(*) FROM vw_conus_hucs', conn)
    huc10_count = huc10_count_df.iloc[0, 0]

    # Read the HUC10 outline polygons from the database
    # Convert the 'geom' column (WKT) into actual Shapely geometry objects
    # Create a GeoDataFrame
    df = pd.read_sql('SELECT huc10, geom FROM huc10_cartography_polygons', conn)
    df['geometry'] = df['geom'].apply(safe_load_wkt)
    gdf_outline = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')  # WGS84 assumed

    # Loop over each 2025CONUS project type and generate a status map image for each
    image_paths = []
    project_types = pd.read_sql('select distinct project_type_id from conus_projects', conn)
    for project_type in project_types['project_type_id']:

        project_count = pd.read_sql(f"SELECT count(*) FROM conus_projects WHERE project_type_id = '{project_type}'", con=conn)
        project_count = project_count.iloc[0, 0]

        # Read the project polygons for this type
        projects_df = pd.read_sql(
            f"""SELECT h.huc10, h.geom
                FROM conus_projects p INNER JOIN huc10_cartography_polygons h ON p.huc = h.huc10
                WHERE p.project_type_id = '{project_type}'""", con=conn)
        projects_df['geometry'] = projects_df['geom'].apply(safe_load_wkt)
        projects_gdf = gpd.GeoDataFrame(projects_df, geometry='geometry', crs='EPSG:4326')

        # Skip plotting if no data
        if projects_gdf.empty:
            print(f"Skipping {project_type}: no matching features.")
            continue

        # Optionally output the filled polygons to a new GeoPackage layer
        if output_gpkgs is True:
            projects_gdf.drop(columns=['geom'], inplace=True)  # Remove the original WKT column
            projects_gdf.to_file(os.path.join(output_image_dir, 'rs_complete.gpkg'), layer=project_type, driver="GPKG")

        # Plot the fillpremed and unfilled polygons
        __fig, ax = plt.subplots(figsize=(10, 10))
        gdf_outline.plot(ax=ax, facecolor="#BBCD3F", edgecolor="#828F2C", linewidth=0.15, alpha=0.5)
        projects_gdf.plot(ax=ax, facecolor="#004793", edgecolor="none", alpha=1.0)
        gdf_outline.plot(ax=ax, facecolor="none", edgecolor="#4DCBDC", linewidth=0.15, alpha=0.5)

        # Main title (centered at top of figure)
        percent_complete = project_count / huc10_count * 100 if huc10_count > 0 else 0
        ax.set_title(f'2025 CONUS Projects for {project_type}\nProjects: {project_count:,} ({percent_complete:.1f}% complete)\n{datetime.now().strftime("%d %b %Y %H:%M")}', fontsize=12)
        ax.set_axis_off()

        # Save to PNG
        img_path = os.path.join(output_image_dir, f"status_map_{project_type}.png")
        plt.savefig(img_path, bbox_inches='tight', pad_inches=0.1, dpi=300)
        plt.close()
        image_paths.append(img_path)

        print(f"Saved image to {img_path}")

    return image_paths


def safe_load_wkt(wkt_str):
    """Convert a WKT string to a Shapely geometry object, handling empty or invalid strings gracefully."""

    if isinstance(wkt_str, str) and wkt_str.strip():
        try:
            return wkt.loads(wkt_str)
        except Exception:
            return None  # or raise/log if you want to debug specific bad values
    return None


def upload_to_s3(image_paths: List[str], s3_bucket: str) -> None:
    """
    Upload generated images to an S3 bucket.: param image_paths: List of paths to images to upload.: param s3_bucket: Name of the S3 bucket.: param s3_key_prefix: Optional prefix for the S3 keys.
    """
    if s3_bucket is None or len(image_paths) < 1:
        return

    s3_parts = s3_bucket.split('/')
    s3_bucket = s3_parts[2]
    s3_key = '/'.join(s3_parts[3:]).strip('/')

    s3 = boto3.client('s3')
    for image_path in image_paths:
        filename = os.path.basename(image_path)
        s3_key_full = f'{s3_key}/{filename}'
        s3.upload_file(image_path, s3_bucket, s3_key_full, ExtraArgs={'ACL': 'public-read'})
        print(f"Uploaded {filename} to S3 bucket {s3_bucket} at {s3_key_full}")


def main():
    """Main function to parse arguments and call the status map generation function."""

    parser = argparse.ArgumentParser(description="Generate status maps for project types.")
    parser.add_argument('athena_output_dir', type=str, help='s3 path where Athena output is stored')
    parser.add_argument('output_image_dir', type=str, help='Directory to save output images')
    parser.add_argument('--s3_bucket', type=str, default=None, help='Optional S3 bucket to upload images.')
    parser.add_argument('--output_gpkgs', action='store_true', default=False, help='Output filled polygons to GeoPackage layers')
    args = parser.parse_args()

    image_paths = generate_status_maps(args.athena_output_dir, args.output_image_dir, args.output_gpkgs)
    upload_to_s3(image_paths, args.s3_bucket)

    print(f'Status maps generation complete. {len(image_paths)} images saved to {args.output_image_dir}.')


if __name__ == "__main__":
    main()
