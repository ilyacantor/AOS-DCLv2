"""
Farm Ground Truth Client - Fetches deterministic ground truth from Farm API.

This module provides:
1. Scenario generation with deterministic seeds
2. Ground truth metrics for validating NLQ outputs
3. Comparison utilities for testing

Usage:
    client = FarmGroundTruthClient()
    scenario = client.generate_scenario(seed=12345, scale="medium")
    truth = client.get_top_customers(scenario.scenario_id, limit=5)

    # Compare with NLQ result
    assert_customers_match(nlq_result, truth)
"""
import os
import requests
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

FARM_API_URL = os.getenv("FARM_API_URL", "https://autonomos.farm")


@dataclass
class FarmScenario:
    """Generated scenario from Farm."""
    scenario_id: str
    seed: int
    scale: str
    customer_count: int
    invoice_count: int
    vendor_count: int
    asset_count: int


@dataclass
class CustomerGroundTruth:
    """Ground truth for a customer."""
    customer_id: str
    name: str
    revenue: float
    region: Optional[str] = None
    industry: Optional[str] = None


@dataclass
class MoMGroundTruth:
    """Ground truth for month-over-month metrics."""
    current_month: str
    previous_month: str
    current_value: float
    previous_value: float
    delta_absolute: float
    delta_percent: float


@dataclass
class ResourceHealthGroundTruth:
    """Ground truth for resource health."""
    active_count: int
    zombie_count: int
    orphan_count: int
    active_ids: List[str] = field(default_factory=list)
    zombie_ids: List[str] = field(default_factory=list)
    orphan_ids: List[str] = field(default_factory=list)


class FarmGroundTruthClient:
    """Client for fetching ground truth from Farm API."""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or FARM_API_URL
        self._session = requests.Session()
        self._session.timeout = 30

    def _get(self, path: str, params: Dict = None) -> Dict:
        """Make GET request to Farm API."""
        url = f"{self.base_url}{path}"
        try:
            resp = self._session.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"Farm API error: {e}")
            raise

    def _post(self, path: str, json: Dict = None) -> Dict:
        """Make POST request to Farm API."""
        url = f"{self.base_url}{path}"
        try:
            resp = self._session.post(url, json=json)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"Farm API error: {e}")
            raise

    def generate_scenario(self, seed: int = 12345, scale: str = "medium") -> FarmScenario:
        """
        Generate a deterministic scenario.

        Args:
            seed: Random seed for deterministic generation
            scale: "small", "medium", or "large"

        Returns:
            FarmScenario with scenario_id and counts
        """
        data = self._post("/api/scenarios/generate", json={"seed": seed, "scale": scale})

        return FarmScenario(
            scenario_id=data.get("scenario_id", ""),
            seed=seed,
            scale=scale,
            customer_count=data.get("customer_count", 0),
            invoice_count=data.get("invoice_count", 0),
            vendor_count=data.get("vendor_count", 0),
            asset_count=data.get("asset_count", 0),
        )

    def get_top_customers(self, scenario_id: str, limit: int = 5) -> List[CustomerGroundTruth]:
        """
        Get ground truth for top N customers by revenue.

        Args:
            scenario_id: Scenario ID from generate_scenario
            limit: Number of top customers

        Returns:
            List of CustomerGroundTruth sorted by revenue descending
        """
        data = self._get(f"/api/scenarios/{scenario_id}/metrics/top-customers", params={"limit": limit})

        customers = []
        for c in data.get("customers", []):
            customers.append(CustomerGroundTruth(
                customer_id=c.get("customer_id", ""),
                name=c.get("name", ""),
                revenue=float(c.get("revenue", 0)),
                region=c.get("region"),
                industry=c.get("industry"),
            ))

        return customers

    def get_revenue_mom(self, scenario_id: str) -> MoMGroundTruth:
        """
        Get ground truth for revenue month-over-month.

        Args:
            scenario_id: Scenario ID from generate_scenario

        Returns:
            MoMGroundTruth with current, previous, and delta values
        """
        data = self._get(f"/api/scenarios/{scenario_id}/metrics/revenue-mom")

        return MoMGroundTruth(
            current_month=data.get("current_month", ""),
            previous_month=data.get("previous_month", ""),
            current_value=float(data.get("current_value", 0)),
            previous_value=float(data.get("previous_value", 0)),
            delta_absolute=float(data.get("delta_absolute", 0)),
            delta_percent=float(data.get("delta_percent", 0)),
        )

    def get_total_revenue(self, scenario_id: str) -> float:
        """Get ground truth for total revenue."""
        data = self._get(f"/api/scenarios/{scenario_id}/metrics/revenue")
        return float(data.get("total_revenue_usd", 0))

    def get_vendor_spend(self, scenario_id: str) -> Dict[str, float]:
        """Get ground truth for vendor spend breakdown."""
        data = self._get(f"/api/scenarios/{scenario_id}/metrics/vendor-spend")
        return {v["vendor_name"]: float(v["total_spend"]) for v in data.get("vendors", [])}

    def get_resource_health(self, scenario_id: str) -> ResourceHealthGroundTruth:
        """
        Get ground truth for resource health (active/zombie/orphan).

        Args:
            scenario_id: Scenario ID from generate_scenario

        Returns:
            ResourceHealthGroundTruth with counts and IDs
        """
        data = self._get(f"/api/scenarios/{scenario_id}/metrics/resource-health")

        return ResourceHealthGroundTruth(
            active_count=data.get("active_count", 0),
            zombie_count=data.get("zombie_count", 0),
            orphan_count=data.get("orphan_count", 0),
            active_ids=data.get("active_ids", []),
            zombie_ids=data.get("zombie_ids", []),
            orphan_ids=data.get("orphan_ids", []),
        )

    def health_check(self) -> bool:
        """Check if Farm API is available."""
        try:
            resp = self._session.get(f"{self.base_url}/api/health", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False


# =============================================================================
# Comparison Utilities
# =============================================================================

def compare_top_customers(
    nlq_customers: List[Dict[str, Any]],
    ground_truth: List[CustomerGroundTruth],
    tolerance_pct: float = 1.0,
) -> tuple[bool, List[str]]:
    """
    Compare NLQ top customers result against ground truth.

    Args:
        nlq_customers: List from NLQ aggregations['top_customers']
        ground_truth: List from Farm get_top_customers()
        tolerance_pct: Allowed percentage difference in revenue

    Returns:
        (passed, list of discrepancy messages)
    """
    discrepancies = []

    if len(nlq_customers) != len(ground_truth):
        discrepancies.append(f"Count mismatch: NLQ={len(nlq_customers)}, truth={len(ground_truth)}")

    for i, (nlq, truth) in enumerate(zip(nlq_customers, ground_truth)):
        nlq_name = nlq.get("name", "")
        nlq_rev = nlq.get("revenue", 0)

        # Check name match
        if nlq_name.lower() != truth.name.lower():
            discrepancies.append(f"Rank {i+1} name mismatch: NLQ='{nlq_name}', truth='{truth.name}'")

        # Check revenue within tolerance
        if truth.revenue > 0:
            pct_diff = abs(nlq_rev - truth.revenue) / truth.revenue * 100
            if pct_diff > tolerance_pct:
                discrepancies.append(
                    f"Rank {i+1} revenue mismatch: NLQ=${nlq_rev:,.0f}, truth=${truth.revenue:,.0f} ({pct_diff:.1f}% diff)"
                )

    return len(discrepancies) == 0, discrepancies


def compare_mom_delta(
    nlq_delta_pct: float,
    ground_truth: MoMGroundTruth,
    tolerance_pct: float = 1.0,
) -> tuple[bool, List[str]]:
    """
    Compare NLQ MoM delta against ground truth.

    Args:
        nlq_delta_pct: Delta percentage from NLQ result
        ground_truth: MoMGroundTruth from Farm
        tolerance_pct: Allowed absolute difference in percentage points

    Returns:
        (passed, list of discrepancy messages)
    """
    discrepancies = []

    diff = abs(nlq_delta_pct - ground_truth.delta_percent)
    if diff > tolerance_pct:
        discrepancies.append(
            f"MoM delta mismatch: NLQ={nlq_delta_pct:.1f}%, truth={ground_truth.delta_percent:.1f}% ({diff:.1f}pp diff)"
        )

    return len(discrepancies) == 0, discrepancies


def compare_resource_counts(
    nlq_counts: Dict[str, int],
    ground_truth: ResourceHealthGroundTruth,
) -> tuple[bool, List[str]]:
    """
    Compare NLQ resource health counts against ground truth.

    Args:
        nlq_counts: Dict with 'zombie_count', 'active_count', etc.
        ground_truth: ResourceHealthGroundTruth from Farm

    Returns:
        (passed, list of discrepancy messages)
    """
    discrepancies = []

    checks = [
        ("zombie", nlq_counts.get("zombie_count", 0), ground_truth.zombie_count),
        ("active", nlq_counts.get("active_count", 0), ground_truth.active_count),
        ("orphan", nlq_counts.get("orphan_count", 0), ground_truth.orphan_count),
    ]

    for name, nlq_val, truth_val in checks:
        if nlq_val != truth_val:
            discrepancies.append(f"{name} count mismatch: NLQ={nlq_val}, truth={truth_val}")

    return len(discrepancies) == 0, discrepancies
