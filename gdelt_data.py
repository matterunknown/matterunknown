"""
GDELT Data Module — matterunknown
Global news events and sentiment. No API key required.
Uses GDELT DOC 2.0 API tonechart mode for weighted sentiment.
"""

import requests
from datetime import datetime, timezone

GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

def fetch_tone(query: str, days: int = 7) -> dict:
    """
    Fetch weighted average tone for a query using tonechart mode.
    Bins range -10 to +10. Returns count-weighted average tone.
    """
    params = {
        "query": query,
        "mode": "tonechart",
        "timespan": f"{days}d",
        "format": "json",
    }
    try:
        r = requests.get(GDELT_DOC, params=params, timeout=20)
        r.raise_for_status()
        chart = r.json().get("tonechart", [])
        if not chart:
            return {"tone": 0.0, "count": 0, "status": "no_data", "query": query}

        total_count = sum(b["count"] for b in chart)
        if total_count == 0:
            return {"tone": 0.0, "count": 0, "status": "no_articles", "query": query}

        weighted_tone = sum(b["bin"] * b["count"] for b in chart) / total_count
        return {
            "tone": round(weighted_tone, 4),
            "count": total_count,
            "query": query,
            "status": "ok"
        }
    except Exception as e:
        return {"tone": 0.0, "count": 0, "status": f"error: {e}", "query": query}


def fetch_geopolitical_risk(days: int = 7) -> dict:
    """Composite risk signal from multiple query categories."""
    queries = {
        "conflict":  "war conflict military sanctions",
        "trade":     "tariff trade war protectionism",
        "economic":  "recession inflation federal reserve rate hike",
    }
    results = {}
    import time
    for label, query in queries.items():
        results[label] = fetch_tone(query, days)
        time.sleep(2)  # GDELT rate limit

    tones = [r["tone"] for r in results.values() if r["status"] == "ok"]
    if tones:
        avg_tone = sum(tones) / len(tones)
        risk_score = max(0.0, min(1.0, (-avg_tone + 10) / 20))
    else:
        risk_score = 0.5

    return {
        "risk_score": round(risk_score, 4),
        "components": results,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def get_regime_signal() -> dict:
    """Single regime signal for quant model defensive rotation layer."""
    geo = fetch_geopolitical_risk(days=7)
    risk = geo["risk_score"]

    if risk > 0.65:
        regime = "defensive"
    elif risk < 0.35:
        regime = "risk_on"
    else:
        regime = "neutral"

    return {
        "regime_signal": regime,
        "geopolitical_risk": risk,
        "components": geo["components"],
        "timestamp": geo["timestamp"]
    }


if __name__ == "__main__":
    print("GDELT Geopolitical Risk Signal")
    print("=" * 40)
    signal = get_regime_signal()
    print(f"Regime signal:      {signal['regime_signal'].upper()}")
    print(f"Geopolitical risk:  {signal['geopolitical_risk']:.4f}")
    print()
    for k, v in signal["components"].items():
        print(f"  {k:12}  tone={v.get('tone', 0):+.3f}  articles={v.get('count', 0):4}  [{v.get('status')}]")
