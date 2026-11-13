"""Fraud scoring rules.

Pure-Python so the same logic can be unit tested without Spark and reused
inside the Spark job as plain column expressions or a pandas UDF.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt

HIGH_RISK_MCC = {"4829", "6051", "7995", "5967"}  # wires, crypto/quasi-cash, gambling, inbound telemarketing


@dataclass(frozen=True)
class RuleWeights:
    amount_spike: float = 0.35
    velocity: float = 0.25
    impossible_travel: float = 0.30
    high_risk_mcc: float = 0.10


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two points."""
    lat1, lon1, lat2, lon2 = map(radians, (lat1, lon1, lat2, lon2))
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * asin(sqrt(a))


def amount_spike(amount: float, avg_amount: float | None, multiplier: float = 5.0) -> bool:
    """Flag when a transaction is far above the card's recent average."""
    if avg_amount is None or avg_amount <= 0:
        return amount >= 2_000
    return amount >= multiplier * avg_amount


def velocity(tx_count_last_minute: int, threshold: int = 5) -> bool:
    """Flag bursts of card activity (card testing / account takeover)."""
    return tx_count_last_minute >= threshold


def impossible_travel(km: float, seconds: float, max_kmh: float = 900.0, min_km: float = 100.0) -> bool:
    """Flag when two swipes far apart imply travel faster than a commercial jet.

    Swipes under `min_km` apart are ignored so GPS/merchant-location noise
    within a metro area never looks like teleporting.
    """
    if km < min_km:
        return False
    return (km / (max(seconds, 1.0) / 3600.0)) > max_kmh


def score(
    *,
    amount: float,
    avg_amount: float | None,
    tx_count_last_minute: int,
    km_from_last: float,
    seconds_from_last: float,
    mcc: str,
    weights: RuleWeights = RuleWeights(),
) -> tuple[float, list[str]]:
    """Return a 0-1 risk score and the list of rules that fired."""
    fired: list[str] = []
    total = 0.0
    if amount_spike(amount, avg_amount):
        fired.append("amount_spike")
        total += weights.amount_spike
    if velocity(tx_count_last_minute):
        fired.append("velocity")
        total += weights.velocity
    if impossible_travel(km_from_last, seconds_from_last):
        fired.append("impossible_travel")
        total += weights.impossible_travel
    if mcc in HIGH_RISK_MCC:
        fired.append("high_risk_mcc")
        total += weights.high_risk_mcc
    return round(min(total, 1.0), 4), fired


def decision(risk: float, review_at: float = 0.35, block_at: float = 0.6) -> str:
    if risk >= block_at:
        return "BLOCK"
    if risk >= review_at:
        return "REVIEW"
    return "APPROVE"
