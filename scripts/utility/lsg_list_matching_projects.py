"""
   Given a list, for example of watershed names, find matching projects in the data exchange
   return the list with new matching projects attribute (list of projects that match the criteria)
"""

import os
import json
import time
from rsxml import Logger
from termcolor import colored
from rsapi import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams

log = Logger('Search Projects')
log.setup(verbose=False, log_level=30) 


def simple_search(api: RiverscapesAPI, searchParams:RiverscapesSearchParams):
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

def process_list():
    input_list = [
229054,
263375,
593427,
282844,
115903,
156911,
237512,
395422,
224060
]
    for projectnamekw in input_list:
        searchParam = RiverscapesSearchParams({
            "projectTypeId": "vbet",
            "meta": 
              {
                  "HUC": str(projectnamekw),
              },
            })
        results = simple_search(riverscapes_api, searchParam)
        # results object is a generator of tuples
        # each tuple has 
        # RiverscapesProject object, search stats, number of results
        resultcount = 0
        for result in results:
            print (projectnamekw,result[0].id)            
            resultcount = result[2]
        
        if resultcount != 1:
            print (f"{resultcount} records found for {projectnamekw}")

if __name__ == '__main__':
    log.debug("Starting...")
    starttime=time.time()
    with RiverscapesAPI(stage='PRODUCTION') as riverscapes_api:
        process_list ()

    log.debug("Total time: {:.2f} seconds".format(time.time()-starttime))
    log.info("Done!")