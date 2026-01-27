"""Bulk apply project attribution to projects in Data Exchange
issue https://github.com/Riverscapes/rs-web-monorepo/issues/861

A project's attribution consists of a list of attribution objects,
each of which is an Organization and a list of Roles from the AttributionRoleEnum

There are three ways someone might want to change attribution:
1. add (do not change existing, apply new on top of it)
2. replace (remove any existing attribution and apply new)
3. remove (remove specific attribution but leave all others in place)

* This currently implements MODE 2 ONLY.
* The project will show as having been UPDATED BY the user running the script

Lorin Gaertner
January 2026
"""
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


class ProjectAttributionInput(TypedDict, total=False):
    organizationId: str
    roles: list['AttributionRoleEnum']


class ProjectInput(TypedDict, total=False):
    archived: bool
    attribution: list['ProjectAttributionInput']
    description: str
    heroImageToken: str
    name: str
    summary: str
    tags: list[str]
# ============================================================================================


def build_attribution_params() -> tuple[list[str], str, list[str]]:
    """Assemble:
    * list of projects IDs to apply attribution to
    * ProjectAttribution Object Organization ID
    * ProjectAttribution Object list of AttributionRoleEnum
    """
    return (['73cc1ada-c82b-499e-b3b2-5dc70393e340'], 'cc4fff44-f470-4f4f-ada2-99f741d56b28', ['ANALYST'])


def apply_attribution(rs_api: RiverscapesAPI, attribution_params: tuple[list[str], str, list[str]]):
    """Apply attribution to a project
    TODO: Add different modes
    """
    # Project.attribution is an array of [ProjectAttribution!]!
    # ProjectAttribution is organization: Organization! , role [AttributionRoleEnum!]
    log = Logger('Apply attribution')
    log.title('Apply attribution')
    mutation_file = Path(__file__).parent / 'updateProjectAttribution.graphql'
    mutation = rs_api.load_mutation(mutation_file)
    project_ids, org_id, roles = attribution_params

    attribution_item: ProjectAttributionInput = {
        "organizationId": org_id,
        "roles": [AttributionRoleEnum(role) for role in roles]
    }

    updated = 0
    prg = ProgressBar(total=len(project_ids), text='Attributing projects')
    for i, project_id in enumerate(project_ids):
        project_update: ProjectInput = {
            'attribution': [attribution_item]
        }
        variables = {
            "projectId": project_id,
            "project": project_update
        }
        result = rs_api.run_query(mutation, variables)
        if result is None:
            raise Exception(f'Failed to update project {project_id}. Query returned: {result}')
        updated += 1
        prg.update(i+1)
    prg.finish()
    print(f'Process complete. {updated} projects updated.')


def main():
    """Main entry point - process arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('stage',
                        help='Production or staging Data Exchange',
                        type=str,
                        choices=['production', 'staging'],
                        default='staging')
    args = dotenv.parse_args_env(parser)
    log = Logger('Setup')
    # log_path = output_path / 'report.log'
    # log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.info(f'Connecting to {args.stage} environment')
    with RiverscapesAPI(stage=args.stage) as api:
        attribution_params = build_attribution_params()
        apply_attribution(api, attribution_params)


if __name__ == "__main__":
    main()
