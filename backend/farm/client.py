"""
Farm API Client for DCL Integration.

Provides client methods to call Farm's DCL integration endpoints:

v1 (legacy):
1. Source of Truth API - GET /api/source/salesforce/invoice/{invoice_id}
2. Verification API - POST /api/verify/salesforce/invoice
3. Toxic Stream API - GET /api/stream/synthetic/mulesoft

v2 (business data):
4. Generation - POST /api/business-data/generate
5. Ground Truth - GET /api/business-data/ground-truth/{run_id}
6. Metrics - GET /api/business-data/ground-truth/{run_id}/metric/{metric}
7. Dimensional - GET /api/business-data/ground-truth/{run_id}/dimensional/{dim}
8. Conflicts - GET /api/business-data/ground-truth/{run_id}/conflicts
9. Profile - GET /api/business-data/profile/{run_id}
10. Payload - GET /api/business-data/payload/{run_id}/{pipe_id}
11. Runs - GET /api/business-data/runs
"""

import os
import httpx
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
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
    
    # =========================================================================
    # v2 Business Data Endpoints
    # =========================================================================

    def generate_business_data(
        self,
        push_to_dcl: bool = True,
        dcl_ingest_url: Optional[str] = None,
        seed: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Trigger Farm v2 business data generation.

        POST /api/business-data/generate

        When push_to_dcl=True, Farm will POST each of the 20 pipe payloads
        to DCL's ingest endpoint automatically.

        Returns:
            {
                "run_id": "...",
                "record_counts": {...},
                "manifest_valid": true,
                "pipes_pushed": 20
            }
        """
        url = f"{self.base_url}/api/business-data/generate"
        payload: Dict[str, Any] = {"push_to_dcl": push_to_dcl}
        if dcl_ingest_url:
            payload["dcl_ingest_url"] = dcl_ingest_url
        if seed is not None:
            payload["seed"] = seed

        logger.info(
            f"[FarmClient] Generating business data: push_to_dcl={push_to_dcl}"
        )

        try:
            response = self._get_client().post(url, json=payload, timeout=120.0)
            response.raise_for_status()
            data = response.json()
            run_id = data.get("run_id", "unknown")
            logger.info(
                f"[FarmClient] Business data generated: run_id={run_id}, "
                f"pipes_pushed={data.get('pipes_pushed', 0)}"
            )
            return data
        except httpx.HTTPStatusError as e:
            logger.error(
                f"[FarmClient] Generation failed: HTTP {e.response.status_code} "
                f"body={e.response.text[:500]}"
            )
            raise
        except Exception as e:
            logger.error(f"[FarmClient] Generation error: {e}")
            raise

    def get_ground_truth(self, run_id: str) -> Dict[str, Any]:
        """
        Fetch full v2.0 ground truth manifest.

        GET /api/business-data/ground-truth/{run_id}

        Returns 89 metrics/quarter, 13 dimensional breakdowns,
        36 expected conflicts.
        """
        url = f"{self.base_url}/api/business-data/ground-truth/{run_id}"
        logger.info(f"[FarmClient] Fetching ground truth: run_id={run_id}")

        response = self._get_client().get(url, timeout=30.0)
        response.raise_for_status()
        data = response.json()
        metrics_count = len(data.get("metrics", {}))
        logger.info(
            f"[FarmClient] Ground truth fetched: {metrics_count} metrics"
        )
        return data

    def get_ground_truth_metric(
        self, run_id: str, metric: str, quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Look up a single metric from the ground truth.

        GET /api/business-data/ground-truth/{run_id}/metric/{metric}?quarter=2024-Q1
        """
        url = (
            f"{self.base_url}/api/business-data/ground-truth/{run_id}"
            f"/metric/{metric}"
        )
        params = {}
        if quarter:
            params["quarter"] = quarter

        response = self._get_client().get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_ground_truth_dimensional(
        self, run_id: str, dimension: str
    ) -> Dict[str, Any]:
        """
        Fetch a dimensional breakdown from the ground truth.

        GET /api/business-data/ground-truth/{run_id}/dimensional/{dimension}

        13 dimensions available in v2.0 (e.g., revenue_by_region,
        pipeline_by_stage).
        """
        url = (
            f"{self.base_url}/api/business-data/ground-truth/{run_id}"
            f"/dimensional/{dimension}"
        )

        response = self._get_client().get(url)
        response.raise_for_status()
        return response.json()

    def get_ground_truth_conflicts(self, run_id: str) -> Dict[str, Any]:
        """
        Fetch expected cross-system conflicts.

        GET /api/business-data/ground-truth/{run_id}/conflicts

        Returns the 3 intentional conflicts DCL should detect:
        - Revenue timing (~5%)
        - Headcount (+3 contractors)
        - CSAT (~4% missing data)
        """
        url = (
            f"{self.base_url}/api/business-data/ground-truth/{run_id}/conflicts"
        )

        response = self._get_client().get(url)
        response.raise_for_status()
        data = response.json()
        conflict_count = len(data.get("conflicts", []))
        logger.info(
            f"[FarmClient] Ground truth conflicts: {conflict_count} expected"
        )
        return data

    def get_business_profile(self, run_id: str) -> Dict[str, Any]:
        """
        Fetch full financial model trajectory.

        GET /api/business-data/profile/{run_id}

        Returns ARR waterfall, P&L, BS, CF, SaaS metrics per quarter.
        """
        url = f"{self.base_url}/api/business-data/profile/{run_id}"
        response = self._get_client().get(url)
        response.raise_for_status()
        return response.json()

    def get_pipe_payload(self, run_id: str, pipe_id: str) -> Dict[str, Any]:
        """
        Fetch raw DCL payload for a specific pipe (debugging).

        GET /api/business-data/payload/{run_id}/{pipe_id}
        """
        url = (
            f"{self.base_url}/api/business-data/payload/{run_id}/{pipe_id}"
        )
        response = self._get_client().get(url, timeout=30.0)
        response.raise_for_status()
        return response.json()

    def list_business_data_runs(self) -> Dict[str, Any]:
        """
        List all stored generation runs.

        GET /api/business-data/runs
        """
        url = f"{self.base_url}/api/business-data/runs"
        response = self._get_client().get(url)
        response.raise_for_status()
        return response.json()

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
