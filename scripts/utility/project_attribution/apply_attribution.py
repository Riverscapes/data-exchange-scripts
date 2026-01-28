"""Bulk apply project attribution to projects in Data Exchange
issue https://github.com/Riverscapes/rs-web-monorepo/issues/861

A project's attribution consists of a list of attribution objects,
each of which is an Organization and a list of Roles from the AttributionRoleEnum

There are three ways someone might want to change attribution:
1. add (do not change existing, apply new on top of it)
2. replace (remove any existing attribution and apply new)
3. remove (remove specific attribution but leave all others in place)

* This currently implements all modes. 
* The project will show as having been UPDATED BY the user running the script

Lorin Gaertner
January 2026

examples - all tagged 2025conus - blm funder
huc metadata is in certain huc2
pb really likes point to folder of csv files with projects
inquirer - get org GUID and multi-select roles
"""
from typing import Any
from pathlib import Path
import argparse

from rsxml import ProgressBar, dotenv, Logger
from pydex import RiverscapesAPI
# from pydex.generated_types import AttributionRoleEnum, ProjectAttributionInput, ProjectInput
# ============================================================================================
from typing import TypedDict
from enum import Enum


class AttributionRoleEnum(str, Enum):
    ANALYST = 'ANALYST'
    CONTRIBUTOR = 'CONTRIBUTOR'
    CO_FUNDER = 'CO_FUNDER'
    DESIGNER = 'DESIGNER'
    FUNDER = 'FUNDER'
    OWNER = 'OWNER'
    QA_QC = 'QA_QC'
    SUPPORTER = 'SUPPORTER'


class ProjectAttributionInput(TypedDict, total=True):
    organizationId: str
    roles: list['AttributionRoleEnum']


class ProjectInput(TypedDict, total=False):
    archived: bool
    attribution: list['ProjectAttributionInput']
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
    ADD = 'ADD'
    REPLACE = 'REPLACE'
    REMOVE = 'REMOVE'


def build_attribution_params() -> tuple[list[str], str, list[str]]:
    """Assemble:
    * list of projects IDs to apply attribution to
    * ProjectAttribution Object Organization ID
    * ProjectAttribution Object list of AttributionRoleEnum
    """
    return (['73cc1ada-c82b-499e-b3b2-5dc70393e340'], 'c3addb86-a96d-4831-99eb-3899764924da', ['ANALYST', 'DESIGNER'])


def normalize_api_data(current_data: list[Any]) -> list[ProjectAttributionInput]:
    """Helper: Convert raw API Output (Nested Dicts) to Input Format (TypedDict)"""
    normalized_list: list[ProjectAttributionInput] = []

    if not current_data:
        return normalized_list

    for item in current_data:
        # Safety check for malformed data
        if not item.get('organization') or not item['organization'].get('id'):
            continue

        normalized_list.append({
            "organizationId": item['organization']['id'],
            # Convert string roles back to proper Enums
            "roles": [AttributionRoleEnum(r) for r in item.get('roles', [])]
        })
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
        if a['organizationId'] != b['organizationId']:
            return False

        # Compare roles as sets to ignore order (['A', 'B'] == ['B', 'A'])
        if set(a['roles']) != set(b['roles']):
            return False

    return True


def resolve_attribution_list(current_data: list[ProjectAttributionInput],
                             target_attrib_item: ProjectAttributionInput,
                             mode: UpdateMode) -> list[ProjectAttributionInput]:
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

    target_org_id = target_attrib_item['organizationId']
    if mode == UpdateMode.REMOVE:
        # Return list without this org
        return [x for x in current_data if x['organizationId'] != target_org_id]

    working_list = [x.copy() for x in current_data]
    if mode == UpdateMode.ADD:
        # check if org exists
        existing_index = next((i for i, x in enumerate(working_list) if x['organizationId'] == target_org_id), -1)
        if existing_index > -1:
            # MERGE: Combine existing roles with new roles (using set to avoid duplicates)
            existing_roles = set(working_list[existing_index]['roles'])
            new_roles = set(target_attrib_item['roles'])

            # Convert back to list and cast to Enum to satisfy TypedDict
            merged_roles = [AttributionRoleEnum(r) for r in existing_roles.union(new_roles)]
            working_list[existing_index]['roles'] = merged_roles
        else:
            # APPEND: Add new entry to list
            working_list.append(target_attrib_item)

    return working_list


def apply_attribution(rs_api: RiverscapesAPI, attribution_params: tuple[list[str], str, list[str]], mode: UpdateMode):
    """Apply attribution to a project
    """
    # Project.attribution is an array of [ProjectAttribution!]!
    # ProjectAttribution is organization: Organization! , role [AttributionRoleEnum!]
    log = Logger('Apply attribution')
    log.title('Apply attribution')
    mutation_file = Path(__file__).parent / 'updateProjectAttribution.graphql'
    mutation = rs_api.load_mutation(mutation_file)
    get_current_attrib_query_file = Path(__file__).parent / 'getProjectAttribution.graphql'
    get_current_attrib_query = rs_api.load_mutation(get_current_attrib_query_file)

    project_ids, org_id, roles = attribution_params

    target_attrib_item: ProjectAttributionInput = {
        "organizationId": org_id,
        "roles": [AttributionRoleEnum(role) for role in roles]
    }

    updated = 0
    prg = ProgressBar(total=len(project_ids), text='Attributing projects')
    for i, project_id in enumerate(project_ids):

        # Step 1 .Fetch Current attribution
        current_attribution = []
        try:
            resp = rs_api.run_query(get_current_attrib_query, {"id": project_id})
            if resp and 'data' in resp:
                raw_data = resp['data']['project'].get('attribution', [])
                current_attribution = normalize_api_data(raw_data)
            print(current_attribution)
        except Exception as e:
            log.error(f"Failed to fetch current attribution for {project_id}: {e}")
            prg.update(i+1)
            continue

        # Step 2: Calculate desired new attribution state
        final_list = resolve_attribution_list(current_attribution, target_attrib_item, mode)
        if is_attribution_equal(current_attribution, final_list):
            log.debug("No change needed")
        else:
            project_update: ProjectInput = {
                'attribution': final_list
            }
            variables = {
                "projectId": project_id,
                "project": project_update
            }
            try:
                result = rs_api.run_query(mutation, variables)
                if result is None:
                    raise Exception(f'Failed to update project {project_id}. Query returned: {result}')
                updated += 1
            except Exception as e:
                log.error(f"Error executing mutation on {project_id}: {e}")
        prg.update(i+1)
    prg.finish()
    print(f'Process complete. {updated} projects updated.')


def main():
    """Main entry point - process arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--stage',
                        help='Production or staging Data Exchange',
                        type=str,
                        choices=['production', 'staging'],
                        default='staging')
    parser.add_argument('--mode', type=str, choices=[m.value for m in UpdateMode], default='ADD',
                        help="ADD: Append/Merge, REPLACE: Overwrite, REMOVE: Delete specific org")
    args = dotenv.parse_args_env(parser)
    log = Logger('Setup')
    mode_enum = UpdateMode(args.mode)
    # log_path = output_path / 'report.log'
    # log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.info(f'Connecting to {args.stage} environment')
    with RiverscapesAPI(stage=args.stage) as api:
        attribution_params = build_attribution_params()
        apply_attribution(api, attribution_params, mode=mode_enum)


if __name__ == "__main__":
    main()
