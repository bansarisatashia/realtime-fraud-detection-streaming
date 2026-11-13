import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rules import amount_spike, decision, haversine_km, impossible_travel, score, velocity  # noqa: E402


def test_haversine_dallas_to_london():
    km = haversine_km(32.7767, -96.7970, 51.5074, -0.1278)
    assert 7550 < km < 7650


def test_amount_spike_uses_card_average():
    assert amount_spike(600, 100)
    assert not amount_spike(300, 100)


def test_amount_spike_without_history_uses_floor():
    assert amount_spike(2500, None)
    assert not amount_spike(500, None)


def test_velocity_threshold():
    assert velocity(5)
    assert not velocity(4)


def test_impossible_travel():
    assert impossible_travel(7600, 3600)       # Dallas -> London in an hour
    assert not impossible_travel(20, 3600)     # across town


def test_score_blocks_when_multiple_rules_fire():
    risk, fired = score(
        amount=2000, avg_amount=100, tx_count_last_minute=6,
        km_from_last=7600, seconds_from_last=600, mcc="6051",
    )
    assert set(fired) == {"amount_spike", "velocity", "impossible_travel", "high_risk_mcc"}
    assert risk == 1.0
    assert decision(risk) == "BLOCK"


def test_normal_purchase_is_approved():
    risk, fired = score(
        amount=45, avg_amount=60, tx_count_last_minute=1,
        km_from_last=3, seconds_from_last=7200, mcc="5411",
    )
    assert fired == []
    assert decision(risk) == "APPROVE"


def test_nearby_swipes_seconds_apart_are_not_travel():
    assert not impossible_travel(5, 2)
