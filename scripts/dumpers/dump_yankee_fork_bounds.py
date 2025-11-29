"""
One time script to download Yankee Fork bounds from data exchange for use 
in migration of post-CHaMP Yankee Fork topo data.

Basically this searches for all topo projects in the Yankee Fork 
watershed and downloads their project.rs.xml and project_bounds.geojson files
to a local directory structure organized by site name. It just does
one download pass and skips any sites that already have data downloaded.

It then builds a SQLite database containing the bounds information for each
site. This SQLite is an attempt to recreate the database that was provided to
Tyler Kunz when he was migrating projects from the old Warehouse to the new 
Data Exchange. It will be used in the RiverscapesXML python scripts to build
bounds for the migrating the post-champ Yankee Fork topo projects.

Philip Bailey
27 Nov 2025
"""
import os
import sqlite3
import json
import xml.etree.ElementTree as ET
from pydex.classes.riverscapes_helpers import RiverscapesSearchParams
from pydex import RiverscapesAPI

parent_output_dir = "/Users/philipbailey/GISData/champ/yankee_fork_bounds"
workbench_db_path = "/Users/philipbailey/GISData/riverscapes/champ/workbench.db"

with RiverscapesAPI(stage="production") as api:
    with sqlite3.connect(workbench_db_path) as sqlite_conn:
        curs = sqlite_conn.cursor()
        curs.execute('''
            CREATE TABLE IF NOT EXISTS CHaMP_Bounds
            (
                VisitID INTEGER PRIMARY KEY REFERENCES Visits(VisitID),
                bounds TEXT,
                polygon TEXT
            )
        ''')
    for x, _stats, _total, _prg in api.search(RiverscapesSearchParams({
        "projectTypeId": "topo",
        "tags": ["CHAMP_Watershed_Yankee_Fork"],
    }), page_size=500):

        site = x.project_meta["Site"]
        visit_id = int(x.project_meta["Visit"])
        output_dir = os.path.join(parent_output_dir, site)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            api.download_files(x.id, output_dir, ['project\\.rs\\.xml$', 'project_bounds\\.geojson$'], force=True)

        bounds_file_path = os.path.join(output_dir, 'project_bounds.geojson')
        bounds_file_json = json.load(open(bounds_file_path, 'r', encoding='utf-8')) if os.path.exists(bounds_file_path) else None

        # Load the project.rs.xml to verify it loads correctly.
        rscontext_path = os.path.join(output_dir, 'project.rs.xml')
        project = ET.parse(rscontext_path)
        nod_project = project.find('ProjectBounds')
        if nod_project is None:
            continue

        bounds = {
            'centroid': {
                'lat': float(nod_project.find('Centroid/Lat').text),
                'lng': float(nod_project.find('Centroid/Lng').text),
            },
            'boundingBox': {
                'MinLat': float(nod_project.find('BoundingBox/MinLat').text),
                'MinLng': float(nod_project.find('BoundingBox/MinLng').text),
                'MaxLat': float(nod_project.find('BoundingBox/MaxLat').text),
                'MaxLng': float(nod_project.find('BoundingBox/MaxLng').text),
            },
        }

        curs.execute('INSERT INTO CHaMP_Bounds (VisitID, bounds, polygon) VALUES (?, ?, ?) ON CONFLICT DO NOTHING', [visit_id, json.dumps(bounds), json.dumps(bounds_file_json)])
    sqlite_conn.commit()
