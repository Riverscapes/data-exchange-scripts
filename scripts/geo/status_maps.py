"""
Generates a series of status maps for different project types
from a GeoPackage containing project data.

Note that for now, this script is designed to work with the SQLite GeoPackage
that Philip developed to track project status. Utlimately, this should be
replaced with a more generic solution that can work with Athena.

Philip Bailey
12 July 2025
"""
import argparse
import os
from datetime import datetime
import sqlite3
import geopandas as gpd
import matplotlib.pyplot as plt


def generate_status_maps(gpkg_path: str, output_image_dir: str) -> None:
    """
    Generate status maps for different project types and save them as PNG images.
    """

    # Ensure output directory exists
    os.makedirs(output_image_dir, exist_ok=True)

    # Connect to the GeoPackage and fetch project types
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

            print(f"Saved image to {img_path}")


def main():
    """Main function to parse arguments and call the status map generation function."""

    parser = argparse.ArgumentParser(description="Generate status maps for project types.")
    parser.add_argument('gpk_path', type=str, help='GeoPackage path containing project data and HUC10 geometries')
    parser.add_argument('output_image_dir', type=str, help='Directory to save output images')
    args = parser.parse_args()

    generate_status_maps(args.gpk_path, args.output_image_dir)


if __name__ == "__main__":
    main()
