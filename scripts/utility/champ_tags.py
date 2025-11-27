"""
One time script to loop over CHaMP topo projects in the Data Exchange and ensure they 
have the correct tags and ownership. Also ensure that these projects are correctly
linked to the CHaMP Postgres database by verifying GUIDs.

Philip Bailey
27 Nov 2025
"""
from rsxml import Logger
import psycopg2
import psycopg2.extras
from pydex import RiverscapesAPI
from pydex.classes.riverscapes_helpers import RiverscapesSearchParams

postgres_service = "CHaMPGooglePostgres"
stage = "production"
download_dir = "/tmp/champ_downloads"
champ_org = "c7d8c487-c377-42b0-a5b6-4c16db18fb41"


def champ_tags(api: RiverscapesAPI, curs: psycopg2.extensions.cursor) -> None:
    """Loop over CHaMP topo projects and ensure correct tags and ownership."""

    log = Logger('CHaMP Tags')

    missing_tags_count = 0
    adding_guid_count = 0
    missing_postgres_row = 0
    wrong_owner_count = 0

    watershed_tag_count = 0
    site_tag_count = 0
    year_tag_count = 0
    visit_tag_count = 0

    # Simplest search just for Topo projects.
    # Script assumes all topo projects should be owned by CHaMP organization.
    for x, _stats, _total, _prg in api.search(RiverscapesSearchParams({
        "projectTypeId": "topo",
    })):

        meta_watershed = x.project_meta.get("Watershed", None)
        meta_site = x.project_meta.get("Site", None)
        meta_year = int(x.project_meta.get("Year", 0))
        meta_visit = int(x.project_meta.get("Visit", 0))

        # Cannot proceed without these metadata fields.
        if meta_watershed is None:
            log.error(f"  No Watershed found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            continue
        if meta_site is None:
            log.error(f"  No Site found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            continue
        if meta_year == 0:
            log.error(f"  No Year found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            continue
        if meta_visit == 0:
            log.error(f"  No Visit found in project metadata for project {x.id} - {x.name}. Skipping tag upload.")
            continue

        if x.ownedBy['id'] != champ_org:
            log.warning(f"Project {x.id} - {x.name} is not owned by CHaMP organization. Skipping tag upload.")
            wrong_owner_count += 1
            continue

        log.debug(f"Processing project {x.id} - {x.name}")
        log.debug(f"Watershed: {meta_watershed}")
        log.debug(f"Site: {meta_site}")
        log.debug(f"Year: {meta_year}")
        log.debug(f"Visit: {meta_visit}")

        # Cleanup the metadata into the format required for tags.
        watershed_tag = f"CHAMP_Watershed_{meta_watershed.replace(' ', '_')}"
        site_tag = f"CHAMP_Site_{meta_site.replace(' ', '_')}"
        year_tag = f"CHAMP_Year_{meta_year}"
        visit_tag = f"CHAMP_Visit_{str(meta_visit).zfill(4)}"

        # Start a new list of final tags to ensure no duplicates.
        final_tags = [
            watershed_tag,
            site_tag,
            year_tag,
            visit_tag
        ]

        # Add any existing tags that are not CHaMP tags.
        for tag in x.tags:
            if not tag.startswith("CHAMP_"):
                final_tags.append(tag)

        missing_tags = 0
        if watershed_tag not in x.tags:
            missing_tags += 1
            watershed_tag_count += 1
            # log.warning(f"  Added tag: {watershed_tag}")

        if site_tag not in x.tags:
            missing_tags += 1
            site_tag_count += 1
            # log.warning(f"  Added tag: {site_tag}")

        if year_tag not in x.tags:
            missing_tags += 1
            year_tag_count += 1
            # log.warning(f"  Added tag: {year_tag}")

        if visit_tag not in x.tags:
            missing_tags += 1
            visit_tag_count += 1
            # log.warning(f"  Added tag: {visit_tag}")

        if missing_tags_count > 0 or x.ownedBy['id'] != champ_org:
            log.info(f"Updating project {x.id} - {x.name} with new tags and/or ownership.")

            update_project_mutation = api.load_mutation('updataProject')
            __update_project_result = api.run_query(update_project_mutation, {
                'projectId': x.id,
                'owner': {
                    'id': champ_org,
                    'type': 'ORGANIZATION'
                },
                'tags': final_tags
            })
            print(f"Updated project {x.id} - {x.name} with new tags and/or ownership.")

        ############################################################################################################################################
        # Check if Postgres has a matching project row
        curs.execute("SELECT project_id, status_id, guid FROM projects WHERE visit_id = %s AND project_type_id = 1", (meta_visit,))
        project_row = curs.fetchone()
        if not project_row:
            log.error(f"  No project found in CHaMP database for visit ID {meta_visit}. Skipping tag upload.")
            missing_postgres_row += 1
            continue
        else:
            # Ensure that the project row has GUID and that it matches that in Data Exchange
            if project_row['guid']:
                log.debug(f"  CHaMP DB GUID: {project_row['guid']}")
                if project_row['guid'] != x.id:
                    log.error(f"  GUID mismatch for visit ID {meta_visit}. CHaMP DB GUID: {project_row['guid']}, Dex GUID: {x.id}. Skipping tag upload.")
                    continue
            else:
                log.warning(f"Postgres project record has no GUID for corresponding visit {meta_visit}.")

                curs.execute("UPDATE projects SET guid = %s WHERE visit_id = %s AND project_type_id = 1", (x.id, meta_visit))
                log.info(f"  Added GUID {x.id} to CHaMP DB for visit ID {meta_visit}.")
                curs.connection.commit()
                adding_guid_count += 1

    log.info(f"Watershed tags added: {watershed_tag_count}")
    log.info(f"Site tags added: {site_tag_count}")
    log.info(f"Year tags added: {year_tag_count}")
    log.info(f"Visit tags added: {visit_tag_count}")

    log.info(f"Tag updates: {missing_tags_count}")
    log.info(f"GUIDs added: {adding_guid_count}")
    log.info(f"Missing Postgres rows: {missing_postgres_row}")
    log.info(f"Projects with wrong owner: {wrong_owner_count}")


def main():

    conn = psycopg2.connect(service="CHaMPGooglePostgres")
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    with RiverscapesAPI(stage=stage) as api:
        champ_tags(api, curs)


if __name__ == "__main__":
    main()
