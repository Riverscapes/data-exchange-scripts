"""Bulk apply project attribution to projects in Data Exchange

The workhorse function is `apply_attribution`. The inputs are a list of projects, an organization ID and a list of roles.
The main function will help user select a csv file (from a specified folder) containing the list of projects.

A project's attribution consists of a list of attribution objects,
each of which is an Organization and a list of Roles from the AttributionRoleEnum

There are three MODES for attribution change:
1. ADD (do not change existing, apply new on top of it)
2. REPLACE (remove any existing attribution and apply new)
3. REMOVE (remove specific attribution but leave all others in place)

* This currently implements all modes
* in REMOVE mode, it removes _all_ attribution for that organization (leaving other organizations in place)
* It only updates project if there is a change.
* When a project is updated by the script, the project will show as having been UPDATED BY the user running the script (logging into Data Exchange)

## Example usage:
"Add BLM as funder to all the 2025 CONUS projects."
* Run Athena query to get the IDs of all projects tagged 2025CONUS. `SELECT project_id FROM conus_projects WHERE contains(tags,'2025CONUS')`
* download results as csv e.g. `conusprojects.csv`
* look up the BLM organization ID in Data Exchange: 876d3961-08f2-4db5-aff2-7ccfa391b984
* Run `apply_attribution --stage production --csv-file conusprojects.csv --organization 876d3961-08f2-4db5-aff2-7ccfa391b984 --role FUNDER --mode ADD`

Lorin Gaertner
January 2026

These classes objects originally came from pydex.generated_types (and could be imported):
 AttributionRoleEnum, ProjectAttributionInput, ProjectInput

Possible enhancements:
* if Organization or roles not provided in command line, use inquirer option to get from user (with multi-select for roles)
* more selective removal option - to remove specific role for an organization
"""

import argparse
import logging
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, TypedDict

import inquirer
from rsxml import Logger, ProgressBar, dotenv

from pydex import RiverscapesAPI


class AttributionRoleEnum(str, Enum):
    ANALYST = "ANALYST"
    CONTRIBUTOR = "CONTRIBUTOR"
    CO_FUNDER = "CO_FUNDER"
    DESIGNER = "DESIGNER"
    FUNDER = "FUNDER"
    OWNER = "OWNER"
    QA_QC = "QA_QC"
    SUPPORTER = "SUPPORTER"


class ProjectAttributionInput(TypedDict, total=True):
    organizationId: str
    roles: list["AttributionRoleEnum"]


class ProjectInput(TypedDict, total=False):
    archived: bool
    attribution: list["ProjectAttributionInput"]
    description: str
    name: str
    summary: str
    tags: list[str]


# ============================================================================================


class ProjectAttributionOutput(TypedDict):
    """Model for what we get back from the API"""

    organization: dict[str, Any]  # e.g. {'id': '...', 'name': '...'}
    roles: list[str]


class UpdateMode(str, Enum):
    """Allowed options for attribution changes"""

    ADD = "ADD"
    REPLACE = "REPLACE"
    REMOVE = "REMOVE"


def build_attribution_params() -> tuple[list[str], str, list[str]]:
    """Assemble:
    * list of projects IDs to apply attribution to
    * ProjectAttribution Object Organization ID
    * ProjectAttribution Object list of AttributionRoleEnum
    """
    return (["73cc1ada-c82b-499e-b3b2-5dc70393e340"], "c3addb86-a96d-4831-99eb-3899764924da", ["ANALYST", "DESIGNER"])


def normalize_api_data(current_data: list[Any]) -> list[ProjectAttributionInput]:
    """Helper: Convert raw API Output (Nested Dicts) to Input Format (TypedDict)"""
    normalized_list: list[ProjectAttributionInput] = []

    if not current_data:
        return normalized_list

    for item in current_data:
        # Safety check for malformed data
        if not item.get("organization") or not item["organization"].get("id"):
            continue

        normalized_list.append(
            {
                "organizationId": item["organization"]["id"],
                # Convert string roles back to proper Enums
                "roles": [AttributionRoleEnum(r) for r in item.get("roles", [])],
            }
        )
    return normalized_list


def is_attribution_equal(list_a: list[ProjectAttributionInput], list_b: list[ProjectAttributionInput]) -> bool:
    """Compare two attribution lists.
    * Checks length
    * Checks Organization ID Match
    * Checks Roles (Order agnostic using Sets)
    """
    if len(list_a) != len(list_b):
        return False

    # We assume the order of organizations matters (e.g. Primary first)
    for a, b in zip(list_a, list_b):
        if a["organizationId"] != b["organizationId"]:
            return False

        # Compare roles as sets to ignore order (['A', 'B'] == ['B', 'A'])
        if set(a["roles"]) != set(b["roles"]):
            return False

    return True


def resolve_attribution_list(current_data: list[ProjectAttributionInput], target_attrib_item: ProjectAttributionInput, mode: UpdateMode) -> list[ProjectAttributionInput]:
    """
    Takes the normalized input list, applies logic, returns new list:
    * for ADD - adds the specific attribution in target to existing
    * for REPLACE - all existing attributions ignored, target returned
    * For REMOVE - removes all attribution for the organization specified in target
    # TODO: Allow for more targetted removal of a specific role
    """

    # 2. Logic
    if mode == UpdateMode.REPLACE:
        # Override everything
        return [target_attrib_item]

    target_org_id = target_attrib_item["organizationId"]
    if mode == UpdateMode.REMOVE:
        # Return list without this org
        return [x for x in current_data if x["organizationId"] != target_org_id]

    working_list = [x.copy() for x in current_data]
    if mode == UpdateMode.ADD:
        # check if org exists
        existing_index = next((i for i, x in enumerate(working_list) if x["organizationId"] == target_org_id), -1)
        if existing_index > -1:
            # MERGE: Combine existing roles with new roles (using set to avoid duplicates)
            existing_roles = set(working_list[existing_index]["roles"])
            new_roles = set(target_attrib_item["roles"])

            # Convert back to list and cast to Enum to satisfy TypedDict
            merged_roles = [AttributionRoleEnum(r) for r in existing_roles.union(new_roles)]
            working_list[existing_index]["roles"] = merged_roles
        else:
            # APPEND: Add new entry to list
            working_list.append(target_attrib_item)

    return working_list


def apply_attribution(rs_api: RiverscapesAPI, mode: UpdateMode, project_ids: list[str], org_id: str, roles: list[str]):
    """Apply attribution to a project"""
    # Project.attribution is an array of [ProjectAttribution!]!
    # ProjectAttribution is organization: Organization! , role [AttributionRoleEnum!]
    log = Logger("Apply attribution")
    log.title("Apply attribution")
    mutation_file = Path(__file__).parent / "updateProjectAttribution.graphql"
    mutation = rs_api.load_mutation(mutation_file)
    get_current_attrib_query_file = Path(__file__).parent / "getProjectAttribution.graphql"
    get_current_attrib_query = rs_api.load_mutation(get_current_attrib_query_file)

    target_attrib_item: ProjectAttributionInput = {"organizationId": org_id, "roles": [AttributionRoleEnum(role) for role in roles]}

    updated = 0
    prg = ProgressBar(total=len(project_ids), text="Attributing projects")
    for i, project_id in enumerate(project_ids):
        log.debug(f"Processing Project ID {project_id}")
        # Step 1 .Fetch Current attribution
        current_attribution = []
        try:
            resp = rs_api.run_query(get_current_attrib_query, {"id": project_id})
            if resp and "data" in resp:
                raw_data = resp["data"]["project"].get("attribution", [])
                current_attribution = normalize_api_data(raw_data)
            log.debug(f"Current attribution: {current_attribution}")
        except Exception as e:
            log.error(f"Failed to fetch current attribution for {project_id}: {e}")
            prg.update(i + 1)
            continue

        # Step 2: Calculate desired new attribution state
        final_list = resolve_attribution_list(current_attribution, target_attrib_item, mode)
        if is_attribution_equal(current_attribution, final_list):
            log.debug("No change needed")
        else:
            project_update: ProjectInput = {"attribution": final_list}
            variables = {"projectId": project_id, "project": project_update}
            try:
                result = rs_api.run_query(mutation, variables)
                if result is None:
                    raise Exception(f"Failed to update project {project_id}. Query returned: {result}")
                updated += 1
                log.debug(f"New attribution: {final_list}")
            except Exception as e:
                log.error(f"Error executing mutation on {project_id}: {e}")
        prg.update(i + 1)
    prg.finish()
    log.info(f"Process complete. {updated} projects updated.")


def get_file_from_folder(folder: Path, ext: str = ".csv") -> Path | None:
    """prompt user for csv file from within specified folder
    returns: path to the chosen file or None otherwise
    This could easily be adjusted to get files of
    """
    log = Logger("get file from folder")
    if not (folder.exists() and folder.is_dir()):
        log.error(f"The path {folder} does not exist or is not a directory. Please provide a valid folder with CSV files.")
        return

    # Get a list of all CSV files in the specified folder. Do not walk to subfolders.
    matching_files = [f for f in folder.iterdir() if f.suffix == ext]
    if not matching_files:
        log.error(f"No `.{ext}` files found in {folder}. Please provide a valid folder with {ext} files.")
        return

    answers = inquirer.prompt([inquirer.List("file_path", message=f"Select a {ext} file to use", choices=matching_files)])
    if not answers:
        log.error("No file selected.")
        return
    csv_path = folder / answers["file_path"]
    return csv_path


def load_ids_from_csv(csvfile: Path) -> list[str]:
    """
    Load a list of GUIDs from a CSV file.
    Assumes the file exists and is a valid path.
    Ignores the first row if it looks like a header (non-GUID).
    Strips whitespace from each entry.
    Logs a warning if any non-GUID values are found.
    Returns a list of GUID strings only.
    """
    log = Logger("load IDs from file")
    project_ids = []
    non_guids = []
    lines = csvfile.read_text().splitlines()
    for i, line in enumerate(lines):
        value = line.strip().strip(",")
        # Remove surrounding single or double quotes if present
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        value = value.strip()
        if not value:
            continue
        try:
            # Try to parse as UUID
            uuid_obj = uuid.UUID(value)
            project_ids.append(str(uuid_obj))
        except (ValueError, AttributeError):
            # Ignore first row if it looks like a header
            if i == 0:
                continue
            non_guids.append(value)
    if non_guids:
        log.warning(f"Found {len(non_guids)} non-GUID values in CSV e.g. {non_guids[0]}. These will not be processed.")
    return project_ids


def get_organization_name(rs_api: RiverscapesAPI, organization_id: str) -> str | None:
    """Look up organization by ID and return its name or None if not found."""
    get_org_qry = """
query getOrganization($id: ID!) {
  organization(id: $id) {
    name
  }
}
"""
    log = Logger("Get organization name")
    try:
        resp = rs_api.run_query(get_org_qry, {"id": organization_id})
        if resp and "data" in resp and resp["data"].get("organization"):
            return resp["data"]["organization"]["name"]
        return None
    except Exception as e:
        log.error(f"No organization found for id {organization_id}: {e}")
        return None


def main():
    """Main entry point - process arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", help="Production or staging Data Exchange", type=str, choices=["production", "staging"], default="staging")
    parser.add_argument("--mode", type=str, choices=[m.value for m in UpdateMode], default="ADD", help="ADD: Append/Merge, REPLACE: Overwrite, REMOVE: Delete specific org")
    # because we use dotenv.parse_args_env we need to parser to get strings rather than path objects
    parser.add_argument("--csv-file", help="path to specific csv file with projectIDs to process", type=str)
    parser.add_argument("--csv-folder", help="Folder containing CSV files with project IDs, from which a file can be chosen interactively", type=str)
    parser.add_argument("--organization", help="GUID for the organization whose attribution will be added or removed", type=str)
    parser.add_argument(
        "--roles",
        nargs="+",
        choices=[role.value for role in AttributionRoleEnum],
        help="one or more roles to add or replace for the supplied organization and projects e.g. FUNDER OWNER. Ignored for REMOVE mode (all attributions are removed)",
        type=str,
    )
    parser.add_argument("--yes", "-y", help="Assume yes to all prompts and run without confirmation.", action="store_true")
    parser.add_argument("--verbose", "-v", help="Verbose logging output", action="store_true")
    # Parse arguments and inquire from user, as needed
    args = dotenv.parse_args_env(parser)
    log = Logger("Setup")

    datestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    log_path = Path.cwd() / f"apply_attribution_{datestamp}.log"
    log.setup(log_path=log_path, log_level=log_level)
    mode_enum = UpdateMode(args.mode)
    # get csv_file of projects
    csv_file = None
    if args.csv_file:
        csv_path = Path(args.csv_file)
        csv_file = csv_path if csv_path.exists() else None
    elif args.csv_folder:
        folder_path = Path(args.csv_folder)
        csv_file = get_file_from_folder(folder_path)
    if not csv_file:
        log.error("No file of projects to process provided. Exiting.")
        return
    project_id_list = load_ids_from_csv(csv_file)

    log.info(f"Connecting to {args.stage} environment")
    with RiverscapesAPI(stage=args.stage) as api:
        organization_id = args.organization
        org_name = get_organization_name(api, organization_id)
        if not org_name:
            log.error(f"Invalid Organization ID: {organization_id}")
        roles = args.roles
        log.info(f"Ready to alter attribution using {mode_enum} for {organization_id} ({org_name}) (ROLES {roles}) to {len(project_id_list)} projects from {csv_file}.")
        # final review for user
        if not args.yes:
            proceed = inquirer.prompt([inquirer.Confirm("proceed", message="Proceed?", default=True)])
            if not proceed or not proceed.get("proceed", False):
                return
        apply_attribution(api, mode_enum, project_id_list, organization_id, roles)


if __name__ == "__main__":
    main()
