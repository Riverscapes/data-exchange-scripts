"""Compare project API behavior for project.rs.xml diagnostics.

Relates to https://github.com/Riverscapes/rs-web-monorepo/issues/879

This script intentionally uses multiple API paths to identify where a project
starts failing:
1) search API lookup by project ID keyword
2) file listing via get_project_files
3) project.rs.xml download attempt
4) get_project_full (heavy query)
5) tree-only GraphQL probe (common failure point for missing XML)

Usage:
    python scripts/utility/diagnose_project_rs_xml.py
    python scripts/utility/diagnose_project_rs_xml.py production
    python scripts/utility/diagnose_project_rs_xml.py staging --download-dir logs/project-rs-debug
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pydex import RiverscapesAPI, RiverscapesSearchParams  # noqa: E402

FAILING_PROJECT_ID = "92bb34ea-2908-4c3e-9b63-5d4159f873d0"
WORKING_PROJECT_ID = "97ac75dd-15cd-4695-ac0d-c32e08bd26c6"


def print_header(text: str) -> None:
    print(f"\n{'=' * 96}")
    print(text)
    print(f"{'=' * 96}")


def print_subheader(text: str) -> None:
    print(f"\n--- {text} ---")


def summarize_search(api: RiverscapesAPI, project_id: str) -> dict[str, Any]:
    """Run a keyword search and return exact project match summary if found."""
    search_params = RiverscapesSearchParams(
        {
            "keywords": project_id,
            "excludeArchived": False,
        }
    )

    matches = []
    for search_result in api.search(search_params, progress_bar=False, max_results=25):
        proj = search_result[0]
        if proj.id == project_id:
            matches.append(proj)

    if len(matches) == 0:
        return {"found": False}

    proj = matches[0]
    meta_keys: list[str] = []
    for meta_item in proj.json.get("meta", []):
        if isinstance(meta_item, dict):
            key = meta_item.get("key")
            if isinstance(key, str):
                meta_keys.append(key)

    return {
        "found": True,
        "id": proj.id,
        "name": proj.name,
        "projectType": proj.project_type,
        "createdOn": proj.json.get("createdOn"),
        "updatedOn": proj.json.get("updatedOn"),
        "visibility": proj.visibility,
        "archived": proj.archived,
        "ownedBy": proj.ownedBy,
        "totalSize": proj.json.get("totalSize"),
        "tagsCount": len(proj.tags),
        "metaKeys": sorted(meta_keys),
    }


def summarize_files(api: RiverscapesAPI, project_id: str) -> dict:
    files = api.get_project_files(project_id)
    normalized_paths = [f.get("localPath", "") for f in files]

    rs_xml_matches = [p for p in normalized_paths if p.replace("\\", "/").lower().lstrip("/") == "project.rs.xml"]
    rs_like_matches = [p for p in normalized_paths if "project" in p.lower() and p.lower().endswith(".xml")]

    return {
        "totalFiles": len(files),
        "hasProjectRsXml": len(rs_xml_matches) > 0,
        "projectRsXmlMatches": rs_xml_matches,
        "projectLikeXmlMatches": sorted(rs_like_matches)[:20],
    }


def try_download_project_rs_xml(api: RiverscapesAPI, project_id: str, download_dir: Path) -> dict:
    try:
        out_path = api.download_project_file(project_id, "project.rs.xml", download_dir, force=False)
        return {
            "ok": True,
            "path": str(out_path),
            "size": out_path.stat().st_size if out_path.exists() else None,
        }
    except Exception as err:  # Keep broad for diagnostics
        return {
            "ok": False,
            "errorType": type(err).__name__,
            "error": str(err),
        }


def try_get_project_full(api: RiverscapesAPI, project_id: str) -> dict:
    try:
        full = api.get_project_full(project_id)
        return {
            "ok": True,
            "id": full.id,
            "name": full.name,
            "projectType": full.project_type,
            "tagsCount": len(full.tags),
            "metaKeys": sorted(full.project_meta.keys()),
        }
    except Exception as err:  # Keep broad for diagnostics
        return {
            "ok": False,
            "errorType": type(err).__name__,
            "error": str(err),
        }


def run_tree_probe(api: RiverscapesAPI, project_id: str) -> dict:
    """Probe only the tree field to isolate XML/tree resolver failures."""
    tree_query = """
query projectTreeProbe($id: ID!) {
  project(id: $id) {
    id
    tree {
      defaultView
      description
      leaves {
        id
      }
      branches {
        bid
      }
    }
  }
}
"""
    try:
        result = api.run_query(tree_query, {"id": project_id})
        tree = result.get("data", {}).get("project", {}).get("tree", {})
        leaves = tree.get("leaves") or []
        branches = tree.get("branches") or []
        return {
            "ok": True,
            "defaultView": tree.get("defaultView"),
            "descriptionLength": len(tree.get("description") or ""),
            "leafCount": len(leaves),
            "branchCount": len(branches),
        }
    except Exception as err:  # Keep broad for diagnostics
        return {
            "ok": False,
            "errorType": type(err).__name__,
            "error": str(err),
        }


def run_basic_project_probe(api: RiverscapesAPI, project_id: str) -> dict:
    """Query minimal project fields without tree to confirm core record health."""
    basic_query = """
query basicProjectProbe($id: ID!) {
  project(id: $id) {
    id
    name
    visibility
    archived
    createdOn
    updatedOn
    projectType {
      id
      name
    }
    ownedBy {
      __typename
      ... on Organization {
        id
        name
      }
      ... on User {
        id
        name
      }
    }
    projectFiles(limit: 1, offset: 0) {
      total
      items {
        localPath
      }
    }
  }
}
"""
    try:
        result = api.run_query(basic_query, {"id": project_id})
        project = result.get("data", {}).get("project", {})
        project_files = project.get("projectFiles", {})
        first_item = (project_files.get("items") or [None])[0]
        return {
            "ok": True,
            "id": project.get("id"),
            "name": project.get("name"),
            "visibility": project.get("visibility"),
            "archived": project.get("archived"),
            "projectType": project.get("projectType"),
            "ownedBy": project.get("ownedBy"),
            "projectFilesTotal": project_files.get("total"),
            "firstFile": first_item,
        }
    except Exception as err:  # Keep broad for diagnostics
        return {
            "ok": False,
            "errorType": type(err).__name__,
            "error": str(err),
        }


def run_for_project(api: RiverscapesAPI, project_id: str, download_dir: Path) -> dict:
    return {
        "projectId": project_id,
        "search": summarize_search(api, project_id),
        "basicProjectProbe": run_basic_project_probe(api, project_id),
        "projectFiles": summarize_files(api, project_id),
        "downloadProjectRsXml": try_download_project_rs_xml(api, project_id, download_dir),
        "getProjectFull": try_get_project_full(api, project_id),
        "treeProbe": run_tree_probe(api, project_id),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose project.rs.xml availability and full-project query behavior")
    parser.add_argument("stage", nargs="?", default="production", choices=["production", "staging"], help="API stage")
    parser.add_argument("--project-a", default=FAILING_PROJECT_ID, help="First project ID to inspect")
    parser.add_argument("--project-b", default=WORKING_PROJECT_ID, help="Second project ID to inspect")
    parser.add_argument("--download-dir", default="logs/project_rs_xml_diagnostics", help="Directory for any downloaded diagnostic files")
    parser.add_argument("--output-json", default=None, help="Optional path to write full diagnostic JSON report")
    args = parser.parse_args()

    download_dir = Path(args.download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    print_header("Riverscapes project.rs.xml diagnostics")
    print(f"Stage: {args.stage}")
    print(f"Project A: {args.project_a}")
    print(f"Project B: {args.project_b}")
    print(f"Download dir: {download_dir.resolve()}")

    results = {}

    with RiverscapesAPI(stage=args.stage) as api:
        for project_id in [args.project_a, args.project_b]:
            print_subheader(f"Inspecting project {project_id}")
            result = run_for_project(api, project_id, download_dir)
            results[project_id] = result
            print(json.dumps(result, indent=2))

    print_header("Comparison summary")
    a = results[args.project_a]
    b = results[args.project_b]

    def _ok(payload: dict) -> bool:
        return bool(payload.get("ok") is True)

    summary = {
        "projectA": {
            "projectId": args.project_a,
            "hasProjectRsXml": a["projectFiles"].get("hasProjectRsXml"),
            "downloadOk": _ok(a["downloadProjectRsXml"]),
            "getProjectFullOk": _ok(a["getProjectFull"]),
            "treeProbeOk": _ok(a["treeProbe"]),
            "basicProjectProbeOk": _ok(a["basicProjectProbe"]),
        },
        "projectB": {
            "projectId": args.project_b,
            "hasProjectRsXml": b["projectFiles"].get("hasProjectRsXml"),
            "downloadOk": _ok(b["downloadProjectRsXml"]),
            "getProjectFullOk": _ok(b["getProjectFull"]),
            "treeProbeOk": _ok(b["treeProbe"]),
            "basicProjectProbeOk": _ok(b["basicProjectProbe"]),
        },
    }
    print(json.dumps(summary, indent=2))

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        report_payload = {
            "generatedAtUtc": datetime.now(UTC).isoformat(),
            "stage": args.stage,
            "projectA": args.project_a,
            "projectB": args.project_b,
            "downloadDir": str(download_dir.resolve()),
            "results": results,
            "summary": summary,
        }
        output_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
        print(f"\nWrote JSON report: {output_path.resolve()}")


if __name__ == "__main__":
    main()
