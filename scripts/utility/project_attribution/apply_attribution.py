"""Bulk apply project attribution to projects in Data Exchange
issue https://github.com/Riverscapes/rs-web-monorepo/issues/861 

A project's attribution consists of a list of attribution objects, 
each of which is an Organization and a list of Roles from the AttributionRoleEnum

There are three ways someone might want to change attribution:
1. add (do not change existing, apply new on top of it)
2. replace (remove any existing attribution and apply new)
3. remove (remove specific attribution but leave all others in place)

Lorin Gaertner
January 2026
"""
import argparse

from rsxml import ProgressBar, dotenv, Logger
from pydex import RiverscapesAPI


def build_attribution_params() -> tuple[list[str], str, list[str]]:
    """Assemble:
    * list of projects IDs to apply attribution to
    * ProjectAttribution Object Organization ID
    * ProjectAttribution Object list of AttributionRoleEnum
    """
    return (['73cc1ada-c82b-499e-b3b2-5dc70393e340'], 'cc4fff44-f470-4f4f-ada2-99f741d56b28', ['CO_FUNDER', 'DESIGNER'])


def apply_attribution(api: RiverscapesAPI, stage: str, attribution_params: tuple[list[str], str, list[str]]):
    # Project.attribution is an array of [ProjectAttribution!]!
    # ProjectAttribution is organization: Organization! , role [AttributionRoleEnum!]
    log = Logger('Apply attribution')
    log.title('Apply attribution')
    mutation = api.load_mutation('updateProject')
    project_ids, org_id, roles = attribution_params

    attribution_item = {
        "organizationId": org_id,
        "roles": roles
    }

    prg = ProgressBar(total=len(project_ids), text='Attributing projects')
    for i, project_id in enumerate(project_ids):
        variables = {
            "projectId": project_id,
        }


if __name__ == '__main__':
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
        apply_attribution(api, args.stage, attribution_params)
