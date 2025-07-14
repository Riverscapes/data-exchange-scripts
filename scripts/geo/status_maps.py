"""
Generates a series of status maps for different project types
from a GeoPackage containing project data.

Note that for now, this script is designed to work with the SQLite GeoPackage
that Philip developed to track project status. Utlimately, this should be
replaced with a more generic solution that can work with Athena.

Philip Bailey
12 July 2025
"""
import os
import sqlite3
import argparse
from typing import List
from datetime import datetime
import geopandas as gpd
import matplotlib.pyplot as plt
import boto3


def generate_status_maps(gpkg_path: str, output_image_dir: str) -> List[str]:
    """
    Generate status maps for different project types and save them as PNG images.
    """

    # Ensure output directory exists
    os.makedirs(output_image_dir, exist_ok=True)

    # Connect to the GeoPackage and fetch project types
    image_paths = []
    try:
        os.makedirs(output_image_dir, exist_ok=True)
    except OSError as e:
        print(f"Error creating output directory {output_image_dir}: {e}")
        return image_paths

    with sqlite3.connect(gpkg_path) as conn:
        curs = conn.cursor()
        curs.execute('SELECT distinct project_Type_id FROM vw_projects')
        project_types = [row[0] for row in curs.fetchall()]

        gdf_outline = gpd.read_file(gpkg_path, layer="vw_conus_hucs")

        curs.execute('SELECT count(*) FROM vw_conus_hucs')
        outline_count = curs.fetchone()[0]

        for project_type in project_types:

            curs.execute("SELECT count(*) FROM vw_projects WHERE tags LIKE '%2025CONUS%' AND project_type_id = ?", (project_type,))
            project_count = curs.fetchone()[0]

            # --- Load layers ---
            gdf_filled = gpd.read_file(gpkg_path, layer="vw_projects")

            # --- Optional filter (WHERE clause) ---
            gdf_filled = gdf_filled[
                (gdf_filled["project_type_id"] == project_type) &
                (gdf_filled['tags'].str.contains('2025CONUS', na=False))
            ]

            # Skip plotting if no data
            if gdf_filled.empty:
                print(f"Skipping {project_type}: no matching features.")
                continue

            # --- Reproject outline if needed ---
            if gdf_filled.crs != gdf_outline.crs:
                gdf_outline = gdf_outline.to_crs(gdf_filled.crs)

            # --- Plot ---
            __fig, ax = plt.subplots(figsize=(10, 10))

            # Plot the filled polygons (filtered)
            gdf_filled.plot(ax=ax, facecolor="#004793", edgecolor="none", alpha=1.0)

            # Plot the hollow outlines
            gdf_outline.plot(ax=ax, facecolor="none", edgecolor="#C8D765", linewidth=0.15, alpha=0.5)

            # Main title (centered at top of figure)
            percent_complete = project_count / outline_count * 100 if outline_count > 0 else 0
            ax.set_title(f'2025 CONUS Projects for {project_type}\nProjects: {project_count:,} ({percent_complete:.1f}% complete)\n{datetime.now().strftime("%d %b %Y %H:%M")}', fontsize=12)

            # Clean up plot
            ax.set_axis_off()

            # Save to PNG
            img_path = os.path.join(output_image_dir, f"status_map_{project_type}.png")
            plt.savefig(img_path, bbox_inches='tight', pad_inches=0.1, dpi=300)
            plt.close()
            image_paths.append(img_path)

            print(f"Saved image to {img_path}")

    return image_paths


def upload_to_s3(image_paths: List[str], s3_bucket: str) -> None:
    """
    Upload generated images to an S3 bucket.

    :param image_paths: List of paths to images to upload.
    :param s3_bucket: Name of the S3 bucket.
    :param s3_key_prefix: Optional prefix for the S3 keys.
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
        s3.upload_file(image_path, s3_bucket, s3_key_full)
        print(f"Uploaded {filename} to S3 bucket {s3_bucket} at {s3_key_full}")


def main():
    """Main function to parse arguments and call the status map generation function."""

    parser = argparse.ArgumentParser(description="Generate status maps for project types.")
    parser.add_argument('gpk_path', type=str, help='GeoPackage path containing project data and HUC10 geometries')
    parser.add_argument('output_image_dir', type=str, help='Directory to save output images')
    parser.add_argument('--s3_bucket', type=str, default=None, help='Optional S3 bucket to upload images.')
    args = parser.parse_args()

    image_paths = generate_status_maps(args.gpk_path, args.output_image_dir)
    upload_to_s3(image_paths, args.s3_bucket)

    print(f'Status maps generation complete. {len(image_paths)} images saved to {args.output_image_dir}.')


if __name__ == "__main__":
    main()
