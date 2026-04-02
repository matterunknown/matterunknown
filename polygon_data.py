"""
Polygon.io Data Module — matterunknown
Equity pricing, aggregates, and market data.
Replaces Alpha Vantage in the quant model data pipeline.
"""

import requests
import boto3
import json
import pandas as pd
from datetime import datetime, timedelta, timezone

POLYGON_BASE = "https://api.polygon.io"

def get_api_key() -> str:
    client = boto3.client("secretsmanager", region_name="us-east-2")
    secret = client.get_secret_value(SecretId="matterunknown/api/polygon")
    return json.loads(secret["SecretString"])["api_key"]


def fetch_daily_bars(symbol: str, api_key: str = None, days: int = 200) -> pd.DataFrame:
    """
    Fetch daily OHLCV bars for a symbol.
    Returns DataFrame with open, high, low, close, volume, adj_close indexed by date.
    """
    if api_key is None:
        api_key = get_api_key()

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days + 60)  # buffer for weekends/holidays

    url = f"{POLYGON_BASE}/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()

        if data.get("status") == "ERROR" or not data.get("results"):
            return pd.DataFrame()

        results = data["results"]
        df = pd.DataFrame(results)
        df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
        df = df.set_index("date")
        df = df.rename(columns={
            "o": "open", "h": "high", "l": "low",
            "c": "close", "v": "volume", "vw": "vwap"
        })
        df["adj_close"] = df["close"]  # Polygon returns adjusted by default
        return df[["open", "high", "low", "close", "adj_close", "volume"]].tail(days)

    except Exception as e:
        print(f"  Polygon error {symbol}: {e}")
        return pd.DataFrame()


def fetch_snapshot(symbols: list, api_key: str = None) -> dict:
    """
    Fetch real-time snapshot for multiple symbols at once.
    Returns dict of symbol -> {price, change, change_pct, volume}.
    Efficient — single API call for up to 250 symbols.
    """
    if api_key is None:
        api_key = get_api_key()

    tickers = ",".join(symbols)
    url = f"{POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {"tickers": tickers, "apiKey": api_key}

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()

        results = {}
        for item in data.get("tickers", []):
            sym = item["ticker"]
            day = item.get("day", {})
            prev = item.get("prevDay", {})
            results[sym] = {
                "price": item.get("lastTrade", {}).get("p") or day.get("c"),
                "change": item.get("todaysChange"),
                "change_pct": item.get("todaysChangePerc"),
                "volume": day.get("v"),
                "prev_close": prev.get("c"),
            }
        return results

    except Exception as e:
        print(f"  Polygon snapshot error: {e}")
        return {}


def fetch_market_status(api_key: str = None) -> dict:
    """Check if the market is currently open."""
    if api_key is None:
        api_key = get_api_key()

    url = f"{POLYGON_BASE}/v1/marketstatus/now"
    try:
        r = requests.get(url, params={"apiKey": api_key}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"status": f"error: {e}"}


if __name__ == "__main__":
    print("Polygon.io Data Module Test")
    print("=" * 40)

    key = get_api_key()

    # Market status
    status = fetch_market_status(key)
    print(f"Market status: {status.get('market', 'unknown')}")
    print()

    # Test daily bars on a few universe symbols
    test_symbols = ["XLP", "XLV", "XOM", "JNJ"]
    for sym in test_symbols:
        df = fetch_daily_bars(sym, key, days=30)
        if not df.empty:
            latest = df.iloc[-1]
            ret_30 = (df["adj_close"].iloc[-1] / df["adj_close"].iloc[0] - 1) * 100
            print(f"  {sym:6}  close=${latest['adj_close']:.2f}  30d_return={ret_30:+.2f}%  rows={len(df)}")
        else:
            print(f"  {sym:6}  NO DATA")

    print()

    # Snapshot test
    snap = fetch_snapshot(test_symbols, key)
    print("Snapshot:")
    for sym, d in snap.items():
        print(f"  {sym:6}  ${d.get('price', 0):.2f}  {d.get('change_pct', 0):+.2f}%")
