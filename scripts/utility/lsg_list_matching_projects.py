"""
   Given a list, for example of watershed names, find matching projects in the data exchange
   return the list with new matching projects attribute (list of projects that match the criteria)
"""

import os
import json
import time
from rsxml import Logger
from termcolor import colored
from pydex import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams

log = Logger('Search Projects')
log.setup(verbose=False, log_level=30)


def simple_search(api: RiverscapesAPI, searchParams: RiverscapesSearchParams):
    """ Simple search examples

    Args:
        api (RiverscapesAPI): _description_
    """
    # Set Up your search params

    # EXAMPLE: Here's a QUICK query to get a count without looping over everything
    # Really useful when you want summaries of the data. Average query time is < 100ms
    # ====================================================================================================
    log.title("Simple Search")
    search_results = api.search(searchParams)

    return search_results


def process_list(api: RiverscapesAPI):
    input_list = [
        11990,
        61041,
        115903,
        156911,
        224060,
        229054,
        237512,
        263375,
        282844,
        395422,
        424172,
        448000,
        588709,
        593427,
        593444,
        593472,
        593481,
        593513
    ]
    for projectnamekw in input_list:
        searchParam = RiverscapesSearchParams({
            "projectTypeId": "rscontextnz",
            "meta":
            {
                "HUC": str(projectnamekw),
            },
        })
        results = simple_search(api, searchParam)
        # results object is a generator of tuples
        # each tuple has
        # RiverscapesProject object, search stats, number of results
        resultcount = 0
        for result in results:
            print(projectnamekw, result[0].id)
            resultcount = result[2]

        if resultcount != 1:
            print(f"{resultcount} records found for {projectnamekw}")


if __name__ == '__main__':
    log.debug("Starting...")
    starttime = time.time()
    with RiverscapesAPI(stage='PRODUCTION') as riverscapes_api:
        process_list(riverscapes_api)

    log.debug("Total time: {:.2f} seconds".format(time.time()-starttime))
    log.info("Done!")
