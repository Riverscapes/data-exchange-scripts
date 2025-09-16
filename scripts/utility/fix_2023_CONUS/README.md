This folder of scripts was created to tackle this issue

https://github.com/Riverscapes/data-exchange-scripts/issues/17


The overall solution was to:

Download all the projects, This is done by the fix_RME.py script but modified slightly for new paths and different search parameters

Find the mismatched bounds id's from the 2023 and 2025 project. The mismatching of the bounds id done by the find_mismatches_boundsid.py and only keep all the projects that had matching HUCs to the 2025 projects but also have differing bounds id's, and match all 2025 projects HUCS to the 2023 projects that had inconsistent project bounds. 

Then we filter out the 0 huc matches out of the main folder using filter_out_0_huc_matches.py

Build a directory of the projects by HUC-ProjectType-ProjectId with 2023 conus being the project to fix and its rscontext being the source of truth. Having the huc_folder_downloading.py create a 2023 folder with its project.rs.xml and the 2025 projectbounds.geojson

Modify the 2023 incorrect xml using xml_editing.py

And the reupload the updated project to upload_fixed_conus_to_de.py