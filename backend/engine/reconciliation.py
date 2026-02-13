"""
Reconciliation engine: compares AAM's export-pipes against DCL's ACTUAL loaded sources.

AAM side = all connections AAM reports via export-pipes (fabric planes + connections)
DCL side = what DCL actually loaded into its graph (source node labels from last run)

This is a REAL diff - not reconciling AAM to itself.
"""

from typing import Dict, Any, List, Set
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def reconcile(
    aam_export: Dict[str, Any],
    dcl_loaded_sources: List[str],
) -> Dict[str, Any]:
    """
    Compare AAM's export-pipes response against DCL's actually-loaded source names.

    Args:
        aam_export: Raw response from AAM's get_pipes() endpoint (fabric_planes structure)
        dcl_loaded_sources: List of source name strings that DCL actually loaded into the graph

    Returns structured diff showing real discrepancies between AAM and DCL.
    """
    fabric_planes = aam_export.get("fabric_planes", [])

    dcl_source_set: Set[str] = set()
    for s in dcl_loaded_sources:
        dcl_source_set.add(s.lower().strip())

    aam_connections: List[Dict[str, Any]] = []
    aam_source_set: Set[str] = set()
    aam_by_plane: Dict[str, List[Dict[str, Any]]] = {}

    for plane in fabric_planes:
        plane_type = (plane.get("plane_type") or "UNMAPPED").upper()
        vendor = plane.get("vendor", "unknown")
        for conn in plane.get("connections", []):
            source_name = conn.get("source_name", "Unknown")
            normalized = source_name.lower().strip()
            entry = {
                "sourceName": source_name,
                "normalized": normalized,
                "vendor": conn.get("vendor", vendor),
                "fabricPlane": plane_type,
                "pipeId": conn.get("pipe_id"),
                "fieldCount": len(conn.get("fields", [])) if conn.get("fields") else 0,
            }
            aam_connections.append(entry)
            aam_source_set.add(normalized)

            if plane_type not in aam_by_plane:
                aam_by_plane[plane_type] = []
            aam_by_plane[plane_type].append(entry)

    in_aam_not_dcl = []
    for conn in aam_connections:
        if conn["normalized"] not in dcl_source_set:
            in_aam_not_dcl.append({
                "sourceName": conn["sourceName"],
                "vendor": conn["vendor"],
                "fabricPlane": conn["fabricPlane"],
                "pipeId": conn["pipeId"],
                "fieldCount": conn["fieldCount"],
                "cause": "AAM reports this connection but DCL did not load it",
            })

    in_dcl_not_aam = []
    for s in dcl_loaded_sources:
        if s.lower().strip() not in aam_source_set:
            in_dcl_not_aam.append({
                "sourceName": s,
                "cause": "DCL loaded this source but AAM does not report it",
            })

    fabric_breakdown = []
    all_planes = set(aam_by_plane.keys())
    for plane_type in sorted(all_planes):
        conns = aam_by_plane.get(plane_type, [])
        aam_count = len(conns)
        dcl_count = sum(1 for c in conns if c["normalized"] in dcl_source_set)
        fabric_breakdown.append({
            "planeType": plane_type,
            "vendor": conns[0]["vendor"] if conns else "unknown",
            "aamConnections": aam_count,
            "dclLoaded": dcl_count,
            "delta": aam_count - dcl_count,
            "missingFromDcl": [c["sourceName"] for c in conns if c["normalized"] not in dcl_source_set],
        })

    diff_causes = []
    if in_aam_not_dcl:
        diff_causes.append({
            "cause": "IN_AAM_NOT_DCL",
            "description": f"{len(in_aam_not_dcl)} connections reported by AAM but not loaded by DCL",
            "severity": "error",
            "count": len(in_aam_not_dcl),
        })
    if in_dcl_not_aam:
        diff_causes.append({
            "cause": "IN_DCL_NOT_AAM",
            "description": f"{len(in_dcl_not_aam)} sources loaded by DCL but not reported by AAM",
            "severity": "warning",
            "count": len(in_dcl_not_aam),
        })
    no_fields = sum(1 for c in aam_connections if c["fieldCount"] == 0)
    if no_fields > 0:
        diff_causes.append({
            "cause": "NO_SCHEMA",
            "description": f"{no_fields} AAM connections have no field/schema information",
            "severity": "info",
            "count": no_fields,
        })

    total_aam = len(aam_connections)
    total_dcl = len(dcl_loaded_sources)
    matched = len(aam_source_set & dcl_source_set)

    if total_aam == 0 and total_dcl == 0:
        status = "empty"
    elif len(in_aam_not_dcl) == 0 and len(in_dcl_not_aam) == 0:
        status = "synced"
    elif len(in_aam_not_dcl) > total_aam * 0.5:
        status = "critical"
    else:
        status = "drifted"

    return {
        "status": status,
        "summary": {
            "aamConnections": total_aam,
            "dclLoadedSources": total_dcl,
            "matched": matched,
            "inAamNotDcl": len(in_aam_not_dcl),
            "inDclNotAam": len(in_dcl_not_aam),
            "fabricCount": len(fabric_planes),
        },
        "diffCauses": diff_causes,
        "fabricBreakdown": fabric_breakdown,
        "inAamNotDcl": in_aam_not_dcl,
        "inDclNotAam": in_dcl_not_aam,
    }
