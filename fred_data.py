"""
FRED Data Module — matterunknown
Federal Reserve Economic Data. Macro overlay for the quant model.
Key signals: yield curve, CPI, Fed funds rate, unemployment.
"""

import requests
import boto3
import json
import pandas as pd
from datetime import datetime, timedelta

def get_api_key() -> str:
    client = boto3.client("secretsmanager", region_name="us-east-2")
    secret = client.get_secret_value(SecretId="matterunknown/api/fred")
    return json.loads(secret["SecretString"])["api_key"]

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# Series we care about for the quant model
SERIES = {
    "fed_funds_rate":    "FEDFUNDS",       # Federal funds effective rate
    "cpi_yoy":           "CPIAUCSL",       # CPI all urban consumers
    "unemployment":      "UNRATE",         # Unemployment rate
    "t10y2y":            "T10Y2Y",         # 10Y-2Y yield spread (recession indicator)
    "t10y3m":            "T10Y3M",         # 10Y-3M yield spread
    "vix":               "VIXCLS",         # VIX (FRED carries it)
    "real_gdp_growth":   "A191RL1Q225SBEA",# Real GDP growth rate
    "consumer_sentiment":"UMCSENT",        # UMich consumer sentiment
}

def fetch_series(series_id: str, api_key: str, limit: int = 60) -> pd.Series:
    """Fetch a FRED series, return as a pandas Series indexed by date."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }
    r = requests.get(FRED_BASE, params=params, timeout=15)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    data = {
        pd.to_datetime(o["date"]): float(o["value"])
        for o in obs
        if o["value"] != "."
    }
    return pd.Series(data).sort_index()


def fetch_macro_snapshot(api_key: str = None) -> dict:
    """
    Fetch all key macro indicators and return a snapshot dict.
    Used as a macro overlay in the equity regime detection.
    """
    if api_key is None:
        api_key = get_api_key()

    snapshot = {}
    for name, series_id in SERIES.items():
        try:
            s = fetch_series(series_id, api_key, limit=24)
            if not s.empty:
                snapshot[name] = {
                    "latest": round(s.iloc[-1], 4),
                    "prev": round(s.iloc[-2], 4) if len(s) > 1 else None,
                    "change": round(s.iloc[-1] - s.iloc[-2], 4) if len(s) > 1 else None,
                    "date": str(s.index[-1].date()),
                    "status": "ok"
                }
        except Exception as e:
            snapshot[name] = {"status": f"error: {e}"}

    return snapshot


def determine_macro_regime(snapshot: dict) -> str:
    """
    Determine macro regime from FRED snapshot.
    Combines with equity_factors.py determine_equity_regime() for full picture.

    Returns: defensive / neutral / risk_on
    """
    signals = []

    # Yield curve inversion = recession warning = defensive
    t10y2y = snapshot.get("t10y2y", {}).get("latest")
    if t10y2y is not None:
        if t10y2y < 0:
            signals.append("defensive")  # Inverted
        elif t10y2y > 1.0:
            signals.append("risk_on")    # Steep = growth
        else:
            signals.append("neutral")

    # CPI trend — rising inflation = defensive
    cpi = snapshot.get("cpi_yoy", {})
    if cpi.get("change") is not None:
        if cpi["change"] > 0.3:
            signals.append("defensive")
        elif cpi["change"] < -0.2:
            signals.append("risk_on")
        else:
            signals.append("neutral")

    # Consumer sentiment — falling = defensive
    sentiment = snapshot.get("consumer_sentiment", {})
    if sentiment.get("change") is not None:
        if sentiment["change"] < -3:
            signals.append("defensive")
        elif sentiment["change"] > 3:
            signals.append("risk_on")
        else:
            signals.append("neutral")

    # Majority vote
    if not signals:
        return "neutral"
    defensive = signals.count("defensive")
    risk_on = signals.count("risk_on")
    if defensive > risk_on:
        return "defensive"
    elif risk_on > defensive:
        return "risk_on"
    return "neutral"


if __name__ == "__main__":
    print("FRED Macro Snapshot")
    print("=" * 40)
    key = get_api_key()
    snap = fetch_macro_snapshot(key)
    for name, data in snap.items():
        if data.get("status") == "ok":
            change_str = f"({data['change']:+.3f})" if data['change'] is not None else ""
            print(f"  {name:25} {data['latest']:8.3f}  {change_str:12}  [{data['date']}]")
        else:
            print(f"  {name:25} ERROR: {data['status']}")
    print()
    regime = determine_macro_regime(snap)
    print(f"  Macro regime signal: {regime.upper()}")
