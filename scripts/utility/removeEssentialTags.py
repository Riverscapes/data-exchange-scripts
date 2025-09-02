"""
Remove the ESSENTIAL tag from all projects owned by 'North Arrow Research'.
- Searches by organization ownership.
- Filters client-side for projects containing 'ESSENTIAL'.
- Logs a JSON backup of affected projects (including original tags) before mutation.
"""

import os
import json
from typing import List, Tuple
from rsxml import Logger
from rsxml.util import safe_makedirs
import inquirer
from pydex import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


ORG_NAME = "North Arrow Research"
TAG_TO_REMOVE = "ESSENTIAL"
LOG_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs"


def _resolve_org_id(api: RiverscapesAPI, org_name: str) -> str:
    find_org_qry = """
    query ($limit:Int!, $offset:Int!, $params: SearchParamsInput!) {
      searchOrganizations(limit: $limit, offset: $offset, params: $params, sort: [NAME_ASC]) {
        total
        results { item { id name } }
      }
    }"""
    res = api.run_query(find_org_qry, {
        "limit": 10,
        "offset": 0,
        "params": {"name": org_name}
    })
    matches = [
        r["item"] for r in res["data"]["searchOrganizations"]["results"]
        if r["item"]["name"] == org_name
    ]
    if not matches:
        raise RuntimeError(f"Could not find organization named '{org_name}'.")
    return matches[0]["id"]


def build_search_params(org_id: str) -> RiverscapesSearchParams:
    # Prefer the same pattern you used earlier; fall back if the class signature differs
    try:
        return RiverscapesSearchParams(
            input_obj={
                "ownedBy": {"id": org_id, "type": "ORGANIZATION"},
                # "excludeArchived": False,  # include if you want archived included
            }
        )
    except TypeError:
        # Some versions use direct kwargs; adapt as needed
        sp = RiverscapesSearchParams()
        # If your class uses attributes instead, uncomment/adjust:
        # sp.ownedBy = {"id": org_id, "type": "ORGANIZATION"}
        return sp


def get_projects(api: RiverscapesAPI, search_params: RiverscapesSearchParams, tag: str
                     ) -> Tuple[List[RiverscapesProject], int]:
    """
    Iterate search results and collect projects that currently contain the tag.
    Returns (targets, total_found_in_search_scope)
    """
    targets: List[RiverscapesProject] = []
    total_in_scope = 0
    for proj, _stats, search_total, _prg in api.search(
        search_params,
        progress_bar=True,  # keeps your existing UX
        # keep default sort (DATE_CREATED_DESC) to preserve wrapper pagination
    ):
        total_in_scope = search_total
        if tag in proj.tags:
            targets.append(proj)
    return targets, total_in_scope


def remove_tag_from_projects(api: RiverscapesAPI):
    log = Logger('RemoveTag')
    log.title(f"Remove '{TAG_TO_REMOVE}' Tag from Projects (owned by {ORG_NAME})")

    # 1) Resolve org
    org_id = _resolve_org_id(api, ORG_NAME)
    log.info(f"Target organization: {ORG_NAME} (id={org_id})")

    # 2) Build search params (ownedBy org)
    search_params = build_search_params(org_id)

    # 3) Find candidate projects (those that currently have TAG_TO_REMOVE)
    log.info(f"Searching for projects owned by {ORG_NAME} that contain tag '{TAG_TO_REMOVE}'...")
    targets, total = get_projects(api, search_params, TAG_TO_REMOVE)

    # 4) Write backup log (original tags preserved)
    safe_makedirs(LOG_DIR)
    backup_path = os.path.join(
        LOG_DIR, f"remove_tag_backup_{api.stage}_{TAG_TO_REMOVE}.json"
    )
    with open(backup_path, "w", encoding="utf8") as f:
        f.write(json.dumps([p.json for p in targets], indent=2))
    log.warning(f"Backup of {len(targets)} candidate projects written to: {backup_path}")

    log.info(f"In-scope projects (owned by org): {total}")
    log.info(f"Projects that will have '{TAG_TO_REMOVE}' removed: {len(targets)}")
    if len(targets) == 0:
        log.info("Nothing to do. Exiting.")
        return

    # 5) Confirm before mutating
    answers = inquirer.prompt([
        inquirer.Confirm(
            'confirm',
            message=f"Remove '{TAG_TO_REMOVE}' from {len(targets)} project(s)?",
            default=False
        )
    ])
    if not answers or not answers.get('confirm'):
        log.info("Aborted by user.")
        return

    # 6) Mutate (updateProject) with tag removed
    mutation_script = api.load_mutation('updateProject')
    changed = 0
    errors = 0
    for p in targets:
        if TAG_TO_REMOVE not in p.tags:
            continue  # should not happen; defensive

        new_tags = [t for t in p.tags if t != TAG_TO_REMOVE]
        log.debug(f"Removing '{TAG_TO_REMOVE}' from: {p.name} ({p.id})")

        try:
            resp = api.run_query(mutation_script, {
                "projectId": p.id,
                "project": {"tags": new_tags},
            })
            # Optional: check for GraphQL errors shape if your client surfaces it
            if resp and isinstance(resp, dict) and "errors" in resp and resp["errors"]:
                errors += 1
                log.error(f"GraphQL error updating {p.id}: {resp['errors']}")
            else:
                changed += 1
        except Exception as e:
            errors += 1
            log.error(f"Exception updating {p.id}: {e}")

    log.info(f"Done. Updated {changed} project(s). Errors: {errors}")


if __name__ == "__main__":
    with RiverscapesAPI() as api:
        remove_tag_from_projects(api)
