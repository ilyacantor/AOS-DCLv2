"""
AAM API Client for DCL Integration.

Fetches pipe definitions grouped by fabric plane from AAM.
"""

import os
import time
import httpx
from typing import Dict, Any, Optional, List
from backend.domain import SemanticEdge
from backend.core.constants import AAM_EDGE_CACHE_TTL, AAM_EDGE_CONFIDENCE_MIN
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class AAMClient:
    """Client for AAM's DCL export endpoints."""
    
    def __init__(self, base_url: Optional[str] = None, timeout: float = 30.0):
        raw_url = base_url or os.getenv("AAM_URL")
        if not raw_url:
            raise ValueError(
                "AAM_URL environment variable is required. "
                "Set it in Replit Secrets or your environment."
            )
        self.base_url = raw_url.rstrip("/")
        self.timeout = timeout
        self._client = None
        self._edge_cache: Optional[List[SemanticEdge]] = None
        self._edge_cache_ts: float = 0.0
    
    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)
        return self._client
    
    def get_pipes(self, aod_run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch pipe definitions from AAM grouped by fabric plane.
        
        GET /api/dcl/export-pipes?aod_run_id={id}
        
        Returns fabric planes with their connections and schemas.
        """
        url = f"{self.base_url}/api/dcl/export-pipes"
        params = {}
        if aod_run_id:
            params["aod_run_id"] = aod_run_id
        
        logger.info(f"[AAMClient] Fetching pipes from AAM" + 
                   (f" for run {aod_run_id}" if aod_run_id else ""))
        
        try:
            response = self._get_client().get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            plane_count = len(data.get("fabric_planes", []))
            connection_count = data.get("total_connections", 0)
            logger.info(f"[AAMClient] Fetched {plane_count} fabric planes, "
                       f"{connection_count} total connections")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"[AAMClient] Pipes fetch failed: HTTP {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"[AAMClient] Pipes fetch error: {e}")
            raise
    
    def get_push_history(self) -> List[Dict[str, Any]]:
        """
        Fetch list of pushes from AAM.
        
        GET /api/export/dcl/pushes
        
        Returns a list of push objects.
        """
        url = f"{self.base_url}/api/export/dcl/pushes"
        
        logger.info("[AAMClient] Fetching push history from AAM")
        
        try:
            response = self._get_client().get(url)
            response.raise_for_status()
            data = response.json()
            
            push_count = len(data) if isinstance(data, list) else 0
            logger.info(f"[AAMClient] Fetched {push_count} pushes")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"[AAMClient] Push history fetch failed: HTTP {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"[AAMClient] Push history fetch error: {e}")
            raise
    
    def get_push_detail(self, push_id: str) -> Dict[str, Any]:
        """
        Fetch full push detail with payload from AAM.
        
        GET /api/export/dcl/pushes/{push_id}
        
        Returns the push detail object with payload.
        """
        url = f"{self.base_url}/api/export/dcl/pushes/{push_id}"
        
        logger.info(f"[AAMClient] Fetching push detail for push_id: {push_id}")
        
        try:
            response = self._get_client().get(url)
            response.raise_for_status()
            data = response.json()
            
            pipe_count = data.get("pipe_count", 0)
            logger.info(f"[AAMClient] Fetched push detail for push_id: {push_id}, "
                       f"pipe_count: {pipe_count}")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"[AAMClient] Push detail fetch failed: HTTP {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"[AAMClient] Push detail fetch error: {e}")
            raise
    
    def get_semantic_edges(
        self,
        source_system: Optional[str] = None,
        target_system: Optional[str] = None,
        confidence_min: float = AAM_EDGE_CONFIDENCE_MIN,
    ) -> List[SemanticEdge]:
        """
        Fetch semantic edges from AAM's topology endpoint.

        Returns cached results if within AAM_EDGE_CACHE_TTL.
        On failure: logs warning, returns empty list (graceful degradation).
        """
        now = time.monotonic()
        if (
            self._edge_cache is not None
            and (now - self._edge_cache_ts) < AAM_EDGE_CACHE_TTL
        ):
            logger.debug("[AAMClient] Returning cached semantic edges")
            return self._edge_cache

        url = f"{self.base_url}/api/topology/semantic-edges"
        params: Dict[str, Any] = {"confidence_min": confidence_min}
        if source_system:
            params["source_system"] = source_system
        if target_system:
            params["target_system"] = target_system

        logger.info("[AAMClient] Fetching semantic edges from AAM")

        try:
            response = self._get_client().get(url, params=params)
            response.raise_for_status()
            raw_edges = response.json()
            edges = [SemanticEdge(**e) for e in raw_edges]
            self._edge_cache = edges
            self._edge_cache_ts = now
            logger.info(f"[AAMClient] Fetched {len(edges)} semantic edges")
            return edges
        except Exception as e:
            logger.warning(f"[AAMClient] Semantic edges fetch failed: {e}")
            return []

    def health_check(self) -> Dict[str, Any]:
        """Check AAM API health."""
        url = f"{self.base_url}/health"
        try:
            response = self._get_client().get(url, timeout=5.0)
            response.raise_for_status()
            return {"status": "healthy", "aam_url": self.base_url, "response": response.json()}
        except Exception as e:
            return {"status": "unhealthy", "aam_url": self.base_url, "error": str(e)}
    
    def close(self):
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None


# Singleton instance
_aam_client: Optional[AAMClient] = None


def get_aam_client() -> AAMClient:
    """Get or create the AAM client singleton."""
    global _aam_client
    if _aam_client is None:
        _aam_client = AAMClient()
    return _aam_client
