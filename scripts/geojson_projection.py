"""
Crappy little script to reproject geojson files
"""
import os
import json
import subprocess
import inquirer
from rsxml.util import safe_makedirs


def main():
    """
    Use inquirer to ask for a local folder then open every geojson file inside it and convert it from whatever projection it uses to 4326. then output it in a new folder.
    """

    folder = inquirer.prompt(
        [
            inquirer.Path(
                "folder",
                message="Select folder",
                path_type=inquirer.Path.DIRECTORY,
            )
        ]
    )["folder"]

    # Get all the files in the folder
    files = os.listdir(folder)
    # Filter out only the geojson files
    files = [f for f in files if f.endswith(".geojson")]

    # Create a new folder to store the output files
    out_folder = os.path.join(folder, "output")
    safe_makedirs(out_folder)

    # Loop through all the files
    for f in files:
        # Get the full path to the file
        f_path = os.path.join(folder, f)
        # Get the name of the file
        f_name = os.path.splitext(f)[0]

        # the name comes in as "Project_ID_Couse Creek - 642.geojson" but I need the output name to just be "642.geojson"
        out_name = f_name.split(" - ")[-1]
        # Get the output path
        out_path = os.path.join(out_folder, out_name + ".geojson")

        # Convert the projection
        print(f"Converting {f_path} to {out_path}")
        cmd = f"ogr2ogr -f GeoJSON \"{out_path}\" \"{f_path}\" -t_srs EPSG:4326"
        subprocess.run(cmd, shell=True, check=True)

        # Now open the file, parse the json, remove the "crs" property and save it back to the file
        with open(out_path, "r", encoding='utf8') as f:
            data = f.read()
            json_data = json.loads(data)
            json_data.pop("crs", None)
            json_data.pop("name", None)
            json_data.pop("xy_coordinate_resolution", None)
            for feature in json_data["features"]:
                feature['properties'] = {}

        with open(out_path, "w", encoding='utf8') as f:
            json.dump(json_data, f, indent=2)


if __name__ == '__main__':
    main()
