from typing import Dict, Any, List, Set

from backend.aam.ingress import NormalizedPipe
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def reconcile(
    aam_pipes: List[NormalizedPipe],
    dcl_canonical_ids: List[str],
) -> Dict[str, Any]:
    """
    Reconcile AAM's pipe payload against DCL's loaded sources.

    Both sides use canonical IDs produced by normalize_source_id() — no
    ad-hoc normalization happens in this function.

    Args:
        aam_pipes: Validated+normalized pipes from AAMIngressAdapter
        dcl_canonical_ids: Canonical source IDs loaded by DCL
    """
    dcl_id_set: Set[str] = set(dcl_canonical_ids)

    aam_connections: List[Dict[str, Any]] = []
    aam_id_set: Set[str] = set()
    aam_by_plane: Dict[str, List[Dict[str, Any]]] = {}

    for pipe in aam_pipes:
        plane_key = pipe.fabric_plane.upper()
        entry = {
            "sourceName": pipe.display_name,
            "canonicalId": pipe.canonical_id,
            "vendor": pipe.vendor,
            "fabricPlane": plane_key,
            "pipeId": pipe.pipe_id,
            "fieldCount": pipe.field_count,
        }
        aam_connections.append(entry)
        aam_id_set.add(pipe.canonical_id)

        if plane_key not in aam_by_plane:
            aam_by_plane[plane_key] = []
        aam_by_plane[plane_key].append(entry)

    in_aam_not_dcl = []
    for conn in aam_connections:
        if conn["canonicalId"] not in dcl_id_set:
            in_aam_not_dcl.append({
                "sourceName": conn["sourceName"],
                "canonicalId": conn["canonicalId"],
                "vendor": conn["vendor"],
                "fabricPlane": conn["fabricPlane"],
                "pipeId": conn["pipeId"],
                "fieldCount": conn["fieldCount"],
                "cause": "AAM reports this connection but DCL did not load it",
            })

    in_dcl_not_aam = []
    for cid in dcl_canonical_ids:
        if cid not in aam_id_set:
            in_dcl_not_aam.append({
                "sourceName": cid,
                "canonicalId": cid,
                "cause": "DCL loaded this source but AAM does not report it",
            })

    fabric_breakdown = []
    for plane_type in sorted(aam_by_plane.keys()):
        conns = aam_by_plane[plane_type]
        aam_count = len(conns)
        dcl_count = sum(1 for c in conns if c["canonicalId"] in dcl_id_set)
        fabric_breakdown.append({
            "planeType": plane_type,
            "vendor": conns[0]["vendor"] if conns else "unknown",
            "aamConnections": aam_count,
            "dclLoaded": dcl_count,
            "delta": aam_count - dcl_count,
            "missingFromDcl": [c["sourceName"] for c in conns if c["canonicalId"] not in dcl_id_set],
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
    total_dcl = len(dcl_canonical_ids)
    matched = len(aam_id_set & dcl_id_set)
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
