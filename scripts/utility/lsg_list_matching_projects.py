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
import psycopg
from pprint import pprint
from collections.abc import Generator
import semver


log = Logger('Search Projects')
log.setup(verbose=False, log_level=30)

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

def get_nz_ws_ids() -> list: 
    """connect to posgresql database and get list of ids"""
    conn = psycopg.connect(service="NZCalibrationService")
    query = '''
    SELECT "HydroID" FROM public.watersheds_to_calib
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall() # a list of tuples. this has just 1 field so 1 element per field

    simple_list = [x[0] for x in rows]
    return simple_list


def simple_search(api: RiverscapesAPI, searchParams: RiverscapesSearchParams) -> Generator[tuple[RiverscapesProject, dict,int], None, None]:
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


def get_matching_projects_for_list(api: RiverscapesAPI, input_list: list[int]) -> dict[int, tuple]:
    """find all rs projects that match HUC per input_list 
        prints all of them
        return {hucid: (latest rs_id, model_version)} where it the latest latest model_version and rs_id corresponding. 
    """
    huc_latest_model_project_list = {}
    for hucid in input_list:
        searchParam = RiverscapesSearchParams({
            "projectTypeId": "rscontextnz",
            "meta":
            {
                "HUC": str(hucid),
            },
        })
        results = simple_search(api, searchParam)
        # results object is a generator of tuples
        # each tuple has
        # RiverscapesProject object, search stats, number of results
        resultcount = 0
        for result in results:
            rsobject = result[0]
            print(hucid, rsobject.id, rsobject.model_version)
            if not hucid in huc_latest_model_project_list: 
                huc_latest_model_project_list[hucid] = (rsobject.id, rsobject.model_version)
            else: 
                if huc_latest_model_project_list[hucid][1] < rsobject.model_version:
                    huc_latest_model_project_list[hucid] = (rsobject.id, rsobject.model_version)
            resultcount = result[2]

        if resultcount != 1:
            print(f"{resultcount} records found for {hucid}")

    # to print all the properties of an RS object
    # pprint(vars(rsobject))
    return huc_latest_model_project_list

def update_table(rows: dict[int, tuple[str, int]]) -> None:
    """
    Batch update using a temporary table.
    rows: {HydroID: (rs_id, rs_version)}
    """
    import psycopg

    conn = psycopg.connect(service="NZCalibrationService")
    with conn:
        with conn.cursor() as cur:
            # 1. Create temporary table
            cur.execute("""
                CREATE TEMP TABLE tmp_updates (
                    HydroID integer PRIMARY KEY,
                    rscontext_rs_id varchar(40),
                    rscontext_rs_version varchar(12)
                ) ON COMMIT DROP;
            """)

            # 2. Bulk insert into temp table
            data = [
                (hydro_id, rs_id, str(rs_version)) # convert model_version from Version to string
                for hydro_id, (rs_id, rs_version) in rows.items()
            ]
            cur.executemany(
                "INSERT INTO tmp_updates (HydroID, rscontext_rs_id, rscontext_rs_version) VALUES (%s, %s, %s)",
                data
            )

            # 3. Batch update main table from temp table
            cur.execute("""
                UPDATE public.watersheds_to_calib AS main
                SET
                    rscontext_rs_id = tmp.rscontext_rs_id,
                    rscontext_rs_version = tmp.rscontext_rs_version
                FROM tmp_updates AS tmp
                WHERE main."HydroID" = tmp.HydroID
                  AND (main.rscontext_rs_id IS DISTINCT FROM tmp.rscontext_rs_id
                       OR main.rscontext_rs_version IS DISTINCT FROM tmp.rscontext_rs_version)
            """)
            updated_rows = cur.rowcount
            print(f"Updated {updated_rows} rows in watersheds_to_calib.")
    print("Batch update complete.")

if __name__ == '__main__':
    log.debug("Starting...")
    starttime = time.time()

    # FOR TESTING ONLY LIMIT TO TOP 
    ws_ids = get_nz_ws_ids()

    with RiverscapesAPI(stage='PRODUCTION') as riverscapes_api:
        matchingprojects = get_matching_projects_for_list(riverscapes_api, ws_ids)
    
    # write results back to postgres
    # pprint (matchingprojects)
    update_table(matchingprojects)

    log.debug("Total time: {:.2f} seconds".format(time.time()-starttime))
    log.info("Done!")
