"""
Script to query the PBR GraphQL API for projects using the SearchProjects query.
"""
import os
import sys
import json
from datetime import datetime
from typing import Optional
import requests

PBR_GRAPHQL_ENDPOINT = "https://api.pbr.riverscapes.net/"

SEARCH_PROJECTS_QUERY = """
query SearchProjects {
    searchProjects(limit: 1000, offset: 0, searchTerms: { textSearch: "" }) {
        limit
        offset
        total
        results {
            access
            dateCreated
            dateUpdated
            myPermissions
            name
            projectUrl
            streamName
            watershedName
            actions {
                action
                value
            }
            budget {
                usDollarVal
                items {
                    name
                    usDollarVal
                }
            }
            constructionElements
            coverPhoto {
                dateTaken
                description
                id
                projectId
                url
                meta {
                    key
                    value
                }
            }
            dates {
                date
                name
            }
            draft
            extent
            geoCoding {
                continent
                country
                provState
            }
            goals
            id
            lengthKm
            location {
                geohash
                latitude
                longitude
            }
        }
    }
}
"""


def fetch_pbr_projects(output_path: Optional[str] = None):
    """ This function fetches projects from the PBR GraphQL API and saves them to a JSON file.

    Args:
        output_path (Optional[str], optional): The path to the output JSON file. Defaults to None.
    """
    headers = {"Content-Type": "application/json"}
    payload = {"query": SEARCH_PROJECTS_QUERY}
    print(f"Querying PBR GraphQL API at {PBR_GRAPHQL_ENDPOINT} ...")
    response = requests.post(PBR_GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if "errors" in data:
        print("GraphQL errors:", data["errors"])
        return
    projects = data["data"]["searchProjects"]["results"]
    print(f"Fetched {len(projects)} projects.")

    if output_path is None:
        output_dir = os.path.join(os.path.expanduser("~"), "PBRProjects")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"pbr_projects_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=2)
    print(f"Saved project list to {output_path}")


if __name__ == "__main__":
    # Grab a command line argument for output_path
    output_arg = sys.argv[1] if len(sys.argv) > 1 else None

    fetch_pbr_projects(output_arg)
