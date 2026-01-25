"""
Farm API Client for DCL Integration.

Provides client methods to call Farm's DCL integration endpoints:
1. Source of Truth API - GET /api/source/salesforce/invoice/{invoice_id}
2. Verification API - POST /api/verify/salesforce/invoice
3. Toxic Stream API - GET /api/stream/synthetic/mulesoft
"""

import os
import httpx
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class InvoiceRecord(BaseModel):
    """Pristine invoice record from Farm's source of truth."""
    invoice_id: str
    customer_id: str
    amount: float
    currency: str = "USD"
    status: str
    issue_date: str
    due_date: Optional[str] = None
    line_items: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None


class VerificationResult(BaseModel):
    """Result from Farm's verification API."""
    match: bool
    quality_score: float
    mismatches: List[Dict[str, Any]] = []
    field_comparison: Dict[str, Any] = {}


class ToxicStreamRecord(BaseModel):
    """Record from Farm's toxic MuleSoft stream."""
    record_id: str
    payload: Dict[str, Any]
    chaos_injected: bool = False
    chaos_type: Optional[str] = None


class FarmClient:
    """Client for Farm's DCL integration endpoints."""
    
    def __init__(self, base_url: Optional[str] = None, timeout: float = 30.0):
        self.base_url = base_url or os.getenv(
            "FARM_API_URL",
            "https://63971109-a901-48bc-a71f-89583b2e11d4-00-1do0vncksilxt.janeway.replit.dev"
        )
        self.timeout = timeout
        self._client = None
    
    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)
        return self._client
    
    def get_source_invoice(self, invoice_id: str) -> Dict[str, Any]:
        """
        Fetch pristine invoice from Farm's source of truth.
        
        GET /api/source/salesforce/invoice/{invoice_id}
        
        Returns the canonical/ground-truth invoice record that DCL can use
        for repairing drifted data.
        """
        url = f"{self.base_url}/api/source/salesforce/invoice/{invoice_id}"
        logger.info(f"[FarmClient] Fetching source invoice: {invoice_id}")
        
        try:
            response = self._get_client().get(url)
            response.raise_for_status()
            data = response.json()
            logger.info(f"[FarmClient] Source invoice fetched: {invoice_id}")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"[FarmClient] Source lookup failed: HTTP {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"[FarmClient] Source lookup error: {e}")
            raise
    
    def verify_invoice(self, repaired_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Verify a repaired invoice against Farm's ground truth.
        
        POST /api/verify/salesforce/invoice
        
        Accepts a repaired record from DCL and compares it against the
        source of truth. Returns detailed mismatch report.
        """
        url = f"{self.base_url}/api/verify/salesforce/invoice"
        logger.info(f"[FarmClient] Verifying invoice: {repaired_record.get('invoice_id', 'unknown')}")
        
        try:
            response = self._get_client().post(url, json=repaired_record)
            response.raise_for_status()
            data = response.json()
            logger.info(f"[FarmClient] Verification complete: match={data.get('match', False)}")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"[FarmClient] Verification failed: HTTP {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"[FarmClient] Verification error: {e}")
            raise
    
    def get_toxic_stream(
        self,
        limit: int = 100,
        chaos: bool = False,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Fetch records from Farm's toxic MuleSoft stream.
        
        GET /api/stream/synthetic/mulesoft
        
        Simulates a "toxic" data stream for testing DCL's Ingest Sidecar.
        With chaos=true, injects data quality issues (nulls, type errors, etc).
        """
        url = f"{self.base_url}/api/stream/synthetic/mulesoft"
        params = {"limit": limit}
        if chaos:
            params["chaos"] = "true"
        if offset is not None:
            params["offset"] = offset
        
        logger.info(f"[FarmClient] Fetching toxic stream: limit={limit}, chaos={chaos}")
        
        try:
            response = self._get_client().get(url, params=params, timeout=60.0)
            response.raise_for_status()
            data = response.json()
            record_count = len(data.get("data", data)) if isinstance(data, dict) else len(data)
            logger.info(f"[FarmClient] Toxic stream fetched: {record_count} records")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"[FarmClient] Toxic stream failed: HTTP {e.response.status_code}")
            raise
        except httpx.TimeoutException:
            logger.error("[FarmClient] Toxic stream timeout")
            raise
        except Exception as e:
            logger.error(f"[FarmClient] Toxic stream error: {e}")
            raise
    
    def get_toxic_stream_sample(self) -> Dict[str, Any]:
        """
        Get a sample record from the toxic stream.
        
        GET /api/stream/synthetic/mulesoft/sample
        
        Returns a single sample record for schema inspection.
        """
        url = f"{self.base_url}/api/stream/synthetic/mulesoft/sample"
        logger.info("[FarmClient] Fetching toxic stream sample")
        
        try:
            response = self._get_client().get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"[FarmClient] Sample fetch error: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """Check Farm API health."""
        url = f"{self.base_url}/api/health"
        try:
            response = self._get_client().get(url, timeout=5.0)
            response.raise_for_status()
            return {"status": "healthy", "farm_url": self.base_url, "response": response.json()}
        except Exception as e:
            return {"status": "unhealthy", "farm_url": self.base_url, "error": str(e)}
    
    def close(self):
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None


# Singleton instance
_farm_client: Optional[FarmClient] = None


def get_farm_client() -> FarmClient:
    """Get or create the Farm client singleton."""
    global _farm_client
    if _farm_client is None:
        _farm_client = FarmClient()
    return _farm_client
