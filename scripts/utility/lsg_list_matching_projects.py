"""
   Given a list, for example of watershed names, find matching projects in the data exchange
   return the list with new matching projects attribute (list of projects that match the criteria)
"""

# import os
# import json
import time
from rsxml import Logger
# from termcolor import colored
from pydex import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams
import psycopg
from pprint import pprint # used for some debugging statements
from collections.abc import Generator
import semver
from typing import Any

log = Logger('Search Projects')
log.setup(verbose=False, log_level=30) # set to 20 to get info messages

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
    """ Simple search 

    Args:
        api (RiverscapesAPI): _description_
    """
    # Set Up your search params

    log.title("Simple Search")
    search_results = api.search(searchParams)

    return search_results


def get_latest_matching_projects_for_list(api: RiverscapesAPI, input_list: list[int]) -> dict[int, tuple[str, semver.Version]]:
    """find all rs projects that match HUC per input_list 
        prints all of them
        return {hucid: (latest rs_id, model_version)} where it the latest latest model_version and rs_id corresponding. 
        excludes archived projects
    """
    huc_latest_model_projects = {}
    for hucid in input_list:
        searchParam_rscontextnz = RiverscapesSearchParams({
            "projectTypeId": "rscontextnz",
            "meta":
            {
                "HUC": str(hucid),
            },
        })
        searchParam_NZ_taudem_LLP= RiverscapesSearchParams({
            "projectTypeId": "taudem",
            "ownedBy": {                            
                "type": "ORGANIZATION",
                "id": "e7b017ae-9657-46e1-973a-aa50b7e245ad"
            },
            "meta":
            {
                "HUC": str(hucid),
            },
            "excludeArchived" : False, # new param see Issue #6
            "tags": ["longest_level_path"],  # Only return projects that have these tags
        })


        searchParam = searchParam_NZ_taudem_LLP
        results = simple_search(api, searchParam)
        # results object is a generator of tuples
        # each tuple has
        # RiverscapesProject object, search stats, number of results
        resultcount = 0
        for result in results:
            rsobject = result[0]
            print(hucid, rsobject.id, rsobject.model_version, rsobject.created_date, rsobject.name, '*Archived*' if rsobject.archived else '')
            if not hucid in huc_latest_model_projects: 
                huc_latest_model_projects[hucid] = (rsobject.id, rsobject.model_version)
            else: 
                if huc_latest_model_projects[hucid][1] < rsobject.model_version:
                    huc_latest_model_projects[hucid] = (rsobject.id, rsobject.model_version)
            resultcount = result[2]

        if resultcount != 1:
            print(f"{resultcount} records found for {hucid}")

    # to print all the properties of an RS object
    # pprint(vars(rsobject))
    return huc_latest_model_projects

def batch_update_table(
    conn,
    table_name: str,
    id_field: str,
    columns: list[str],
    rows: dict[int, dict[str, Any]],
):
    """
    UNTESTED 
    Batch update columns in a table using a temporary table.
    - table_name: name of the target table (e.g., 'public.watersheds_to_calib')
    - id_field: primary key field name (e.g., 'HydroID')
    - columns: list of columns to update (e.g., ['rscontext_rs_id', 'rscontext_rs_version'])
    - rows: dict of {id: {col: val, ...}}
    """
    with conn:
        with conn.cursor() as cur:
            # 1. Create temp table
            col_defs = ', '.join([f"{col} varchar(255)" for col in columns])
            cur.execute(f"""
                CREATE TEMP TABLE tmp_updates (
                    {id_field} integer PRIMARY KEY,
                    {col_defs}
                ) ON COMMIT DROP;
            """)

            # 2. Bulk insert
            data = [
                tuple([id_] + [vals[col] for col in columns])
                for id_, vals in rows.items()
            ]
            placeholders = ', '.join(['%s'] * (1 + len(columns)))
            cur.executemany(
                f"INSERT INTO tmp_updates ({id_field}, {', '.join(columns)}) VALUES ({placeholders})",
                data
            )

            # 3. Batch update
            set_clause = ', '.join([f"{col} = tmp.{col}" for col in columns])
            distinct_clause = ' OR '.join([f"main.{col} IS DISTINCT FROM tmp.{col}" for col in columns])
            cur.execute(f"""
                UPDATE {table_name} AS main
                SET {set_clause}
                FROM tmp_updates AS tmp
                WHERE main.{id_field} = tmp.{id_field}
                  AND ({distinct_clause})
            """)
            print(f"Updated {cur.rowcount} rows in {table_name}.")

def update_table_rscontextcols(rows: dict[int, tuple[str, int]]) -> None:
    """
    Batch update specific cols in watersheds_to_calib using a temporary table.
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

    ws_ids = get_nz_ws_ids()
    # ws_ids = ws_ids[:1] # FOR TESTING ONLY LIMIT TO TOP 1

    with RiverscapesAPI(stage='PRODUCTION') as riverscapes_api:
        matchingprojects = get_latest_matching_projects_for_list(riverscapes_api, ws_ids)
    
    # write results back to postgres
    pprint (matchingprojects)
    # update_table(matchingprojects)

    log.debug("Total time: {:.2f} seconds".format(time.time()-starttime))
    log.info("Done!")
