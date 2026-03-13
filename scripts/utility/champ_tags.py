"""
One time script to loop over CHaMP topo projects in the Data Exchange and ensure they
have the correct tags and ownership. Also ensure that these projects are correctly
linked to the CHaMP Postgres database by verifying GUIDs.

Philip Bailey
27 Nov 2025
"""
import time

from rsxml import Logger
import psycopg2
import psycopg2.extras
from pydex import RiverscapesAPI
from pydex.classes.riverscapes_helpers import RiverscapesSearchParams

postgres_service = "CHaMPGooglePostgres"
stage = "production"
download_dir = "/tmp/champ_downloads"
champ_org = "c7d8c487-c377-42b0-a5b6-4c16db18fb41"
watershed_solutions_org = "9a619d52-6b3c-4e26-8854-eb7ff9b77c2a"

VISIT = 1
YEAR = 2
SITE = 3
WATERSHED = 4

project_types = {
    "champphotos": [YEAR, WATERSHED],
    "cad_export": [SITE, VISIT, YEAR, WATERSHED],
    "topo": [VISIT, YEAR, SITE, WATERSHED],
    "fhm": [SITE, WATERSHED],
    "hydro": [SITE, VISIT, YEAR, WATERSHED],
}


def champ_tags(api: RiverscapesAPI, curs: psycopg2.extensions.cursor, project_type: str) -> None:
    """Loop over CHaMP topo projects and ensure correct tags and ownership."""

    log = Logger(project_type)
    log.info(f"Processing CHaMP {project_type} projects to ensure correct tags and ownership.")

    # Get the list of required tags for this project type
    required_tags = project_types.get(project_type, [])

    missing_tags_count = 0
    adding_guid_count = 0
    missing_postgres_row = 0
    wrong_owner_count = 0

    watershed_tag_count = 0
    site_tag_count = 0
    year_tag_count = 0
    visit_tag_count = 0

    update_project_mutation = api.load_mutation('updateProject')

    # Simplest search just for Topo projects.
    # Script assumes all topo projects should be owned by CHaMP organization.
    for x, _stats, _total, _prg in api.search(RiverscapesSearchParams({
        "projectTypeId": project_type,
    }), page_size=100):

        # if x.id != 'cffbf214-8927-4e0d-98cc-a77f012f4204':
        #     continue

        meta_watershed = x.project_meta.get("Watershed", None)
        meta_site = x.project_meta.get("Site") or x.project_meta.get("SiteName")
        meta_year = int(x.project_meta.get("Year") or x.project_meta.get("FieldSeason") or 0)
        meta_visit = int(x.project_meta.get("Visit", 0) or x.project_meta.get("VisitNumber", 0) or x.project_meta.get("VisitID", 0))

        if meta_watershed == 'SouthForkSalmon':
            meta_watershed = 'South Fork Salmon'
        elif meta_watershed == 'JohnDay':
            meta_watershed = 'John Day'
        elif meta_watershed == 'UpperGrandeRonde':
            meta_watershed = 'Upper Grande Ronde'
        elif meta_watershed == 'YankeeFork':
            meta_watershed = 'Yankee Fork'
        elif meta_watershed == 'WallaWalla':
            meta_watershed = 'Walla Walla'

        # Check for required metadata
        missing_metadata = False
        if WATERSHED in required_tags and meta_watershed is None:
            log.error(f"  No Watershed found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            missing_metadata = True
        if SITE in required_tags and meta_site is None:
            log.error(f"  No Site found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            missing_metadata = True
        if YEAR in required_tags and meta_year == 0:
            log.error(f"  No Year found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            missing_metadata = True
        if VISIT in required_tags and meta_visit == 0:
            log.error(f"  No Visit found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            missing_metadata = True

        if missing_metadata:
            continue

        # Cleanup the metadata into the format required for tags.
        tags_map = {}
        if WATERSHED in required_tags:
            tags_map[WATERSHED] = f"CHAMP_Watershed_{meta_watershed.replace(' ', '_').replace('(', '').replace(')', '')}"
        if SITE in required_tags:
            tags_map[SITE] = f"CHAMP_Site_{meta_site.replace(' ', '_')}"
        if YEAR in required_tags:
            tags_map[YEAR] = f"CHAMP_Year_{meta_year}"
        if VISIT in required_tags:
            tags_map[VISIT] = f"CHAMP_Visit_{str(meta_visit).zfill(4)}"

        # Start a new list of final tags to ensure no duplicates.
        # Add any existing tags that are not CHaMP tags.
        final_tags = [t for t in x.tags if not t.startswith("CHAMP_") and not t.startswith("CHaMP_")]

        # Add required tags
        missing_tags = 0
        for tag_type, tag_str in tags_map.items():
            final_tags.append(tag_str)
            if tag_str not in x.tags:
                missing_tags += 1
                if tag_type == WATERSHED:
                    watershed_tag_count += 1
                elif tag_type == SITE:
                    site_tag_count += 1
                elif tag_type == YEAR:
                    year_tag_count += 1
                elif tag_type == VISIT:
                    visit_tag_count += 1

        if missing_tags > 0:
            log.info(f"Updating project {x.id} - {x.name} with new tags.")
            print_visit(meta_watershed, meta_site, meta_year, meta_visit, x)
            missing_tags_count += 1

            __update_project_result = api.run_query(update_project_mutation, {
                'projectId': x.id,
                'project': {
                    'tags': final_tags
                }
            })
            # sleep for 3 seconds to avoid hitting rate limits
            time.sleep(3)
            print(f"Updated project {x.id} - {x.name} with new tags.")

        # Project should be owned by either CHaMP or Watershed Solutions organization.
        # if x.ownedBy['id'] != champ_org and x.ownedBy['id'] != watershed_solutions_org:
        #     log.warning(f"Project {x.id} - {x.name} is not owned by CHaMP or Watershed Solutions organization. Skipping tag upload.")
        #     print_visit(meta_watershed, meta_site, meta_year, meta_visit, x)
        #     wrong_owner_count += 1

        #     change_owner_mutation = api.load_mutation('changeProjectOwner')
        #     __change_owner_result = api.run_query(change_owner_mutation, {
        #         'projectId': x.id,
        #         'owner': {
        #             'id': champ_org,
        #             'type': 'ORGANIZATION'
        #         }
        #     })
        #     print(f"Updated project {x.id} - {x.name} with new ownership.")

        ############################################################################################################################################
        # Check if Postgres has a matching project row
        # if project_type == 'topo' and VISIT in required_tags:
        #     curs.execute("SELECT project_id, status_id, guid FROM projects WHERE visit_id = %s AND project_type_id = 1", (meta_visit,))
        #     project_row = curs.fetchone()
        #     if not project_row:
        #         log.error(f"No project found in CHaMP database for visit ID {meta_visit}. Skipping tag upload.")
        #         missing_postgres_row += 1
        #         continue
        #     else:
        #         # Ensure that the project row has GUID and that it matches that in Data Exchange
        #         if project_row['guid']:
        #             # log.debug(f"  CHaMP DB GUID: {project_row['guid']}")
        #             if project_row['guid'] != x.id:
        #                 log.error(f"  GUID mismatch for visit ID {meta_visit}. CHaMP DB GUID: {project_row['guid']}, Dex GUID: {x.id}. Skipping tag upload.")
        #                 continue
        #         else:
        #             log.warning(f"Postgres project record has no GUID for corresponding visit {meta_visit}.")

        #             curs.execute("UPDATE projects SET guid = %s WHERE visit_id = %s AND project_type_id = 1", (x.id, meta_visit))
        #             log.info(f"  Added GUID {x.id} to CHaMP DB for visit ID {meta_visit}.")
        #             curs.connection.commit()
        #             adding_guid_count += 1

    log.info(f"Watershed tags added: {watershed_tag_count}")
    log.info(f"Site tags added: {site_tag_count}")
    log.info(f"Year tags added: {year_tag_count}")
    log.info(f"Visit tags added: {visit_tag_count}")

    log.info(f"Tag updates: {missing_tags_count}")
    log.info(f"GUIDs added: {adding_guid_count}")
    log.info(f"Missing Postgres rows: {missing_postgres_row}")
    log.info(f"Projects with wrong owner: {wrong_owner_count}")


def print_visit(watershed: str, site: str, year: int, visit: int, x) -> None:
    log = Logger('CHaMP')
    log.debug(f"Processing project {x.id} - {x.name}")
    log.debug(f"Watershed: {watershed}")
    log.debug(f"Site: {site}")
    log.debug(f"Year: {year}")
    log.debug(f"Visit: {visit}")


def main():

    conn = psycopg2.connect(service="CHaMPGooglePostgres")
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    with RiverscapesAPI(stage=stage) as api:
        for project_type in project_types.keys():
            champ_tags(api, curs, project_type)


if __name__ == "__main__":
    main()
