This folder of scripts was created to tackle this issue

https://github.com/Riverscapes/data-exchange-scripts/issues/16


The overall solution was to download all the projects and only keep all the projects that were missing boundsId's, and match all 2025 projects HUCS to the 2024 projects that were missing projects bounds. This is done by the fix_RME.py script

We will then use the create_huc_folder.py script to create folders by each HUC name and populate the RME sub folder with the new project_bounds.geojson and its project.rs.xml and also have a rscontext sub folder with the desired project.rs.xml folder

Then for all the projects we have downloaded we will add to the HUC folders to allow the upcoming upload script to have enough context to update itself using the upload function provided by RiverscapesAPI using add_projects_to_HUC_folders.py

Last step before the upload we will update the RME project.rs.xml with the correct project.rs.xml from the rscontext folder

We then upload the fixed rme projects using upload_rme_fixed_projects_to_de.py

