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
        self.base_url = base_url or os.getenv("FARM_API_URL")
        if not self.base_url:
            raise ValueError(
                "FARM_API_URL environment variable is required. "
                "Set it in Replit Secrets or your environment."
            )
        self.base_url = self.base_url.rstrip("/")
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
    
    def generate_scenario(self, seed: int = 12345, scale: str = "medium") -> Dict[str, Any]:
        """
        Generate a deterministic scenario for testing.
        
        POST /api/scenarios/generate
        
        Returns scenario_id and manifest with entity counts.
        """
        url = f"{self.base_url}/api/scenarios/generate"
        payload = {"seed": seed, "scale": scale}
        logger.info(f"[FarmClient] Generating scenario: seed={seed}, scale={scale}")
        
        try:
            response = self._get_client().post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            scenario_id = data.get("scenario_id", "unknown")
            logger.info(f"[FarmClient] Scenario generated: {scenario_id}")
            return data
        except Exception as e:
            logger.error(f"[FarmClient] Scenario generation error: {e}")
            raise
    
    def get_top_customers(
        self, scenario_id: str, limit: int = 10, time_window: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get top customers by revenue from Farm's ground truth.

        GET /api/scenarios/{id}/metrics/top-customers?limit=N&time_window=last_year

        Returns list of customers sorted by revenue with percent_of_total.
        """
        url = f"{self.base_url}/api/scenarios/{scenario_id}/metrics/top-customers"
        params = {"limit": limit}
        if time_window:
            params["time_window"] = time_window
        logger.info(f"[FarmClient] Fetching top {limit} customers for scenario {scenario_id}" +
                    (f" (time_window={time_window})" if time_window else ""))

        try:
            response = self._get_client().get(url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"[FarmClient] Got {len(data.get('customers', []))} customers")
            return data
        except Exception as e:
            logger.error(f"[FarmClient] Top customers fetch error: {e}")
            raise

    def get_total_revenue(
        self, scenario_id: str, time_window: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get total revenue from Farm's ground truth.

        GET /api/scenarios/{id}/metrics/total-revenue?time_window=last_year

        Returns:
            {
                "total_revenue": 12345678.90,
                "period": "Last Year (2024)",
                "transaction_count": 1200,
                "time_window_applied": "last_year",
                "date_range": {"start": "2024-01-01", "end": "2024-12-31"}
            }
        """
        url = f"{self.base_url}/api/scenarios/{scenario_id}/metrics/total-revenue"
        params = {}
        if time_window:
            params["time_window"] = time_window
        logger.info(f"[FarmClient] Fetching total revenue for scenario {scenario_id}" +
                    (f" (time_window={time_window})" if time_window else ""))

        try:
            response = self._get_client().get(url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"[FarmClient] Got total_revenue=${data.get('total_revenue', 0):,.2f} "
                        f"period={data.get('period', 'N/A')}")
            return data
        except Exception as e:
            logger.error(f"[FarmClient] Total revenue fetch error: {e}")
            raise
    
    def get_revenue_metrics(self, scenario_id: str) -> Dict[str, Any]:
        """
        Get total revenue metrics from Farm's ground truth.
        
        GET /api/scenarios/{id}/metrics/revenue
        """
        url = f"{self.base_url}/api/scenarios/{scenario_id}/metrics/revenue"
        logger.info(f"[FarmClient] Fetching revenue metrics for scenario {scenario_id}")
        
        try:
            response = self._get_client().get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"[FarmClient] Revenue metrics fetch error: {e}")
            raise
    
    def get_vendor_spend(self, scenario_id: str) -> Dict[str, Any]:
        """
        Get vendor spend breakdown from Farm's ground truth.
        
        GET /api/scenarios/{id}/metrics/vendor-spend
        """
        url = f"{self.base_url}/api/scenarios/{scenario_id}/metrics/vendor-spend"
        logger.info(f"[FarmClient] Fetching vendor spend for scenario {scenario_id}")
        
        try:
            response = self._get_client().get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"[FarmClient] Vendor spend fetch error: {e}")
            raise
    
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
