"""Bulk apply project attribution to projects in Data Exchange
issue https://github.com/Riverscapes/rs-web-monorepo/issues/861 

Lorin Gaertner
January 2026
"""
import argparse

from rsxml import ProgressBar, dotenv, Logger
from pydex import RiverscapesAPI


def build_attribution_params():
    """Assemble:
    * list of projects IDs to apply attribution to
    * ProjectAttribution Object (Organization ID, list of AttributionRoleEnum)
    """
    return (['73cc1ada-c82b-499e-b3b2-5dc70393e340'], 'cc4fff44-f470-4f4f-ada2-99f741d56b28', ['CO_FUNDER', 'DESIGNER'])


def apply_attribution(api: RiverscapesAPI, stage: str, attribution_params: tuple[list[str], str, list[str]]):
    # ProjectAttribution is organization: Organization! , role [AttributionRoleEnum!]
    # Project.attribution is an array of [ProjectATtribution!]!
    print("hello")


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
