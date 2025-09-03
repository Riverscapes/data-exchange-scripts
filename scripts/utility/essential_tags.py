"""[summary]
"""
import os
from typing import List
import json
import inquirer
from rsxml import Logger
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


def add_tag(riverscapes_api: RiverscapesAPI):
    """Add essential tags to projects from the server.

    """

    log = Logger('AddTag')
    log.title('Add Essential Tag to Projects from the server')

    # -------------------------------------------------------------------------
    # 1) Resolve orgId for "North Arrow Research"
    # -------------------------------------------------------------------------
    find_org_qry = """
    query ($limit:Int!, $offset:Int!, $params: SearchParamsInput!) {
      searchOrganizations(limit: $limit, offset: $offset, params: $params, sort: [NAME_ASC]) {
        total
        results { item { id name } }
      }
    }"""
    org_res = riverscapes_api.run_query(find_org_qry, {
        "limit": 10,
        "offset": 0,
        "params": {"name": "North Arrow Research"}
    })
    org_matches = [
        r["item"] for r in org_res["data"]["searchOrganizations"]["results"]
        if r["item"]["name"] == "North Arrow Research"
    ]
    if not org_matches:
        raise RuntimeError("Could not find organization named 'North Arrow Research'.")
    org_id = org_matches[0]["id"]
    log.info(f"Target organization: North Arrow Research (id={org_id})")

    # -------------------------------------------------------------------------
    # 2) Build search params in memory (ownedBy the org)
    #    NOTE: RiverscapesSearchParams expects GraphQL-ish shape for ownedBy
    # -------------------------------------------------------------------------
    try:
        # If the class accepts kwargs in constructor:
        search_params = RiverscapesSearchParams(
            input_obj={
                "ownedBy": {"id": org_id, "type": "ORGANIZATION"},
                # "excludeArchived": False
            }
        )
    except TypeError:
        # Fallback in case it needs attribute assignment:
        print("Error finding orgId, using RiverscapesSearchParams with kwargs failed.")

    # -------------------------------------------------------------------------
    # 3) Fixed tag list for this script
    # -------------------------------------------------------------------------
    tags = ["ESSENTIAL"]

    # Instead of command-line arguments, we'll use inquirer to ask the user for the stage and tags
    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    questions = [
        inquirer.Text('logdir', message="Where do you want to save the log files?", default=default_dir),
        inquirer.Text('tags', message="Comma-separated tags", default=tags)
    ]
    answers = inquirer.prompt(questions)

    tags = [x.strip() for x in answers['tags'].split(',')]
    logdir = answers['logdir']
    safe_makedirs(logdir)

    # logdir = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs"
    # safe_makedirs(logdir)

    # # Still ask for tags interactively if you like
    # questions = [
    #     inquirer.Text('tags', message="Comma-separated tags", default=[tags])
    # ]
    # answers = inquirer.prompt(questions)

    # tags = [x.strip() for x in answers['tags'].split(',')]

    # -------------------------------------------------------------------------
    # 4) Search & collect candidates (projects missing at least one of the tags)
    # -------------------------------------------------------------------------
    changeable_projects: List[RiverscapesProject] = []
    total = 0
    for project, _stats, search_total, _prg in riverscapes_api.search(
        search_params,
        progress_bar=True,
        # keep default sort (DATE_CREATED_DESC) to preserve wrapper pagination
    ):
        total = search_total
        if any(tag not in project.tags for tag in tags):
            changeable_projects.append(project)

    # Now write all projects to a log file as json
    logpath = os.path.join(logdir, f'add_tag_{riverscapes_api.stage}_{"-".join(tags)}.json')
    with open(logpath, 'w', encoding='utf8') as fobj:
        fobj.write(json.dumps([x.json for x in changeable_projects], indent=2))

    # Now ask if we're sure and then run mutations on all these projects one at a time
    # ================================================================================================================

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to add tag")
    if len(changeable_projects) == 0:
        log.info("No projects to add tag to. Exiting.")
        return
    log.warning(f"Please review the summary of the affected projects in the log file at {logpath} before proceeding!")
    questions = [
        inquirer.Confirm('confirm1', message=f"Are you sure you want to add the tag {tags} to all these projects?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        return

    # -------------------------------------------------------------------------
    # 5) Mutate: updateProject with full tag array (idempotent add)
    # -------------------------------------------------------------------------
    mutation_script = riverscapes_api.load_mutation('updateProject')
    for project in changeable_projects:
        log.debug(f"Add Tag to project: {project.name} ({project.id})")
        for tag in tags:
            if tag not in project.tags:
                project.tags.append(tag)
                log.debug(f" - Adding tag: {tag} to project {project.name} with current tags {project.tags}")

        try:
            resp = riverscapes_api.run_query(mutation_script, {
                "projectId": project.id,
                "project": {"tags": project.tags},
            })
            log.info(f"✅ Updated {project.name} ({project.id}) with tags {project.tags}")
            if resp and "errors" in resp:
                log.error(f"GraphQL error for {project.id}: {resp['errors']}")
        except Exception as e:
            log.error(f"❌ Failed to update {project.name} ({project.id}): {e}")

    log.info("Done!")


if __name__ == '__main__':
    with RiverscapesAPI() as api:
        add_tag(api)
