from typing import Dict, Any, List, Set
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def reconcile(
    aam_pipes: List[Dict[str, Any]],
    dcl_loaded_sources: List[str],
) -> Dict[str, Any]:
    dcl_source_set: Set[str] = set()
    for s in dcl_loaded_sources:
        dcl_source_set.add(s.lower().strip())

    aam_connections: List[Dict[str, Any]] = []
    aam_source_set: Set[str] = set()
    aam_by_plane: Dict[str, List[Dict[str, Any]]] = {}

    for pipe in aam_pipes:
        source_name = pipe.get("display_name", "Unknown")
        normalized = source_name.lower().strip()
        plane_type = (pipe.get("fabric_plane") or "UNMAPPED").upper()
        vendor = pipe.get("source_system", "unknown")
        schema_info = pipe.get("schema_info")
        field_count = len(schema_info) if isinstance(schema_info, list) else 0

        entry = {
            "sourceName": source_name,
            "normalized": normalized,
            "vendor": vendor,
            "fabricPlane": plane_type,
            "pipeId": pipe.get("pipe_id"),
            "fieldCount": field_count,
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
    unmapped_count = len(aam_by_plane.get("UNMAPPED", []))

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
            "unmappedCount": unmapped_count,
        },
        "diffCauses": diff_causes,
        "fabricBreakdown": fabric_breakdown,
        "inAamNotDcl": in_aam_not_dcl,
        "inDclNotAam": in_dcl_not_aam,
    }
