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
    return (['73cc1ada-c82b-499e-b3b2-5dc70393e340'], 'cc4fff44-f470-4f4f-ada2-99f741d56b28', ['ANALYST'])


def resolve_attribution_list(current_data: list['ProjectAttributionInput'],
                             target_attrib_item: ProjectAttributionInput,
                             mode: UpdateMode) -> list['ProjectAttributionInput']:
    """given the a project attribution, the change mode and the change element, 
    return the new attribution
    * For REMOVE - it ignores the input roles and removes all attribution for that organization
    """

    # 1. Transform the current data from the output format to the input format
    normalized_list: list[ProjectAttributionInput] = []

    for item in current_data:
        # Safety check for malformed data
        if not item.get('organization') or not item['organization'].get('id'):
            continue

        normalized_list.append({
            "organizationId": item['organization']['id'],
            # Cast strings back to Enums
            "roles": [AttributionRoleEnum(r) for r in item.get('roles', [])]
        })

    target_org_id = target_attrib_item['organizationId']

    # 2. Logic
    if mode == UpdateMode.REPLACE:
        # Override everything
        return [target_attrib_item]

    elif mode == UpdateMode.REMOVE:
        # Return list without this org
        # TODO: Allow for more targetted removal of a specific role
        return [x for x in normalized_list if x['organizationId'] != target_org_id]

    if mode == UpdateMode.ADD:
        # check if org exists
        existing_index = next((i for i, x in enumerate(normalized_list) if x['organizationId'] == target_org_id), -1)
        if existing_index > -1:
            # MERGE: Combine existing roles with new roles (using set to avoid duplicates)
            existing_roles = set(normalized_list[existing_index]['roles'])
            new_roles = set(target_attrib_item['roles'])

            # Update the existing entry with the Union of roles
            normalized_list[existing_index]['roles'] = list(existing_roles.union(new_roles))
        else:
            # APPEND: Add new entry to list
            normalized_list.append(target_attrib_item)

    return normalized_list


def apply_attribution(rs_api: RiverscapesAPI, attribution_params: tuple[list[str], str, list[str]], mode: UpdateMode):
    """Apply attribution to a project
    TODO: Add different modes
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
                current_attribution = resp['data']['project'].get('attribution', [])
            print(current_attribution)
        except Exception as e:
            log.error(f"Failed to fetch current attribution for {project_id}: {e}")
            prg.update(i+1)
            continue

        # Step 2: Calculate desired new attribution state
        final_list = resolve_attribution_list(current_attribution, target_attrib_item, mode)
        if current_attribution == final_list:
            print("No change needed")
        else:
            project_update: ProjectInput = {
                'attribution': [target_attrib_item]
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
    parser.add_argument('--mode', type=str, choices=[m.value for m in UpdateMode], default='REPLACE',
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
