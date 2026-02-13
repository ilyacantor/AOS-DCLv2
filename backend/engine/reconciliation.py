"""
Reconciliation engine: compares AAM push payload against DCL's loaded view.

AAM push payload = full list of pipes AAM exported to DCL
DCL loaded view = what export-pipes returns (fabric plane connections)
"""

from typing import Dict, Any, List, Optional
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def reconcile(push_payload: Dict[str, Any], dcl_view: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare AAM's push payload against DCL's live export-pipes view.
    
    Returns a structured diff with:
    - summary: counts and high-level status
    - unmapped: pipes not assigned to any fabric plane
    - missing: pipes in push but not found in DCL connections
    - fabric_breakdown: per-plane comparison
    """
    push_pipes = push_payload.get("pipes", [])
    fabric_planes = dcl_view.get("fabric_planes", [])
    
    # Build DCL connection set: source names that DCL loaded
    dcl_sources = set()
    dcl_by_plane = {}
    for plane in fabric_planes:
        plane_type = plane.get("plane_type", "unknown")
        conns = plane.get("connections", [])
        dcl_by_plane[plane_type] = {
            "vendor": plane.get("vendor", "unknown"),
            "connectionCount": len(conns),
            "sourceNames": [c.get("source_name", "") for c in conns],
        }
        for c in conns:
            dcl_sources.add(c.get("source_name", "").lower().strip())
    
    # Analyze push pipes
    total_pushed = len(push_pipes)
    unmapped_pipes = []
    mapped_pipes = []
    push_by_plane = {}
    
    for pipe in push_pipes:
        fp = pipe.get("fabric_plane", "UNMAPPED")
        if fp == "UNMAPPED" or not fp:
            unmapped_pipes.append({
                "pipeId": pipe.get("pipe_id"),
                "displayName": pipe.get("display_name"),
                "sourceSystem": pipe.get("source_system"),
                "transportKind": pipe.get("transport_kind"),
                "trustLabels": pipe.get("trust_labels", []),
                "hasSchema": pipe.get("schema_info") is not None,
            })
        else:
            mapped_pipes.append(pipe)
            if fp not in push_by_plane:
                push_by_plane[fp] = []
            push_by_plane[fp].append(pipe)
    
    # Fabric breakdown: compare push mapped pipes vs DCL loaded connections
    fabric_breakdown = []
    all_plane_types = set(list(push_by_plane.keys()) + list(dcl_by_plane.keys()))
    for plane_type in sorted(all_plane_types):
        push_count = len(push_by_plane.get(plane_type, []))
        dcl_info = dcl_by_plane.get(plane_type, {})
        dcl_count = dcl_info.get("connectionCount", 0)
        fabric_breakdown.append({
            "planeType": plane_type,
            "vendor": dcl_info.get("vendor", push_by_plane.get(plane_type, [{}])[0].get("source_system", "unknown") if push_by_plane.get(plane_type) else "unknown"),
            "pushedPipes": push_count,
            "dclConnections": dcl_count,
            "delta": push_count - dcl_count,
        })
    
    # Build DCL pipe_id set from connections (if they carry pipe_id)
    dcl_pipe_ids = set()
    dcl_normalized_keys = set()
    for plane in fabric_planes:
        for c in plane.get("connections", []):
            pid = c.get("pipe_id")
            if pid:
                dcl_pipe_ids.add(pid)
            name = (c.get("source_name") or "").lower().strip()
            vendor = (plane.get("vendor") or "").lower().strip()
            dcl_normalized_keys.add(f"{plane.get('plane_type','')}__{vendor}__{name}")
    
    # Match mapped pipes to DCL connections using pipe_id first, then exact plane+vendor key
    missing_from_dcl = []
    for pipe in mapped_pipes:
        pid = pipe.get("pipe_id")
        if pid and pid in dcl_pipe_ids:
            continue
        
        fp = (pipe.get("fabric_plane") or "").upper()
        display = (pipe.get("display_name") or "").lower().strip()
        source = (pipe.get("source_system") or "").lower().strip()
        key1 = f"{fp}__{source}__{display}"
        key2 = f"{fp}__{source}__{source}"
        if key1 in dcl_normalized_keys or key2 in dcl_normalized_keys:
            continue
        
        # Check if display name or source matches any DCL source name exactly
        if display in dcl_sources or source in dcl_sources:
            continue
        
        missing_from_dcl.append({
            "pipeId": pid,
            "displayName": pipe.get("display_name"),
            "sourceSystem": pipe.get("source_system"),
            "fabricPlane": pipe.get("fabric_plane"),
        })
    
    # Unique source systems in push
    push_source_systems = set()
    for p in push_pipes:
        ss = p.get("source_system")
        if ss:
            push_source_systems.add(ss)
    
    # Diff causes
    diff_causes = []
    if unmapped_pipes:
        diff_causes.append({
            "cause": "UNMAPPED_PIPES",
            "description": f"{len(unmapped_pipes)} pipes have no fabric plane assignment in AAM",
            "severity": "warning",
            "count": len(unmapped_pipes),
        })
    if missing_from_dcl:
        diff_causes.append({
            "cause": "MAPPED_BUT_MISSING",
            "description": f"{len(missing_from_dcl)} mapped pipes not found in DCL connections",
            "severity": "error",
            "count": len(missing_from_dcl),
        })
    no_schema = sum(1 for p in push_pipes if p.get("schema_info") is None)
    if no_schema > 0:
        diff_causes.append({
            "cause": "NO_SCHEMA",
            "description": f"{no_schema} pipes have no schema information",
            "severity": "info",
            "count": no_schema,
        })
    
    # Status
    if total_pushed == 0:
        status = "empty"
    elif len(unmapped_pipes) == total_pushed:
        status = "critical"
    elif len(unmapped_pipes) > 0 or len(missing_from_dcl) > 0:
        status = "drifted"
    else:
        status = "synced"
    
    return {
        "status": status,
        "summary": {
            "totalPushed": total_pushed,
            "mappedPipes": len(mapped_pipes),
            "unmappedPipes": len(unmapped_pipes),
            "dclConnections": sum(v.get("connectionCount", 0) for v in dcl_by_plane.values()),
            "dclFabrics": len(fabric_planes),
            "uniqueSourceSystems": len(push_source_systems),
            "missingFromDcl": len(missing_from_dcl),
        },
        "diffCauses": diff_causes,
        "fabricBreakdown": fabric_breakdown,
        "unmappedPipes": unmapped_pipes,
        "missingFromDcl": missing_from_dcl,
    }
