import os
import json
import time
import threading
import websocket
import requests
import pandas as pd
from datetime import datetime
from ta.momentum import RSIIndicator

# --- CONFIGURATION ---
SYMBOL = "BTC-USD"
GRANULARITIES = {
    "1m": 60,
    "5m": 300,
    "15m": 900
}
RSI_PERIOD = 14
REST_SYNC_INTERVAL = 300  # every 5 minutes
TERMINAL_PRINT_INTERVAL = 15  # every 15 seconds
# ----------------------

candle_data = {tf: pd.DataFrame() for tf in GRANULARITIES}
live_prices = []
lock = threading.Lock()

def fetch_rest_data():
    while True:
        for tf, granularity in GRANULARITIES.items():
            try:
                url = f"https://api.exchange.coinbase.com/products/{SYMBOL}/candles"
                params = {"granularity": granularity}
                resp = requests.get(url, params=params)
                data = resp.json()
                if isinstance(data, list):
                    df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
                    df["time"] = pd.to_datetime(df["time"], unit="s")
                    df = df.sort_values("time")
                    with lock:
                        candle_data[tf] = df
            except Exception as e:
                print(f"[{datetime.now()}] REST API error for {tf}: {e}")
        time.sleep(REST_SYNC_INTERVAL)

def aggregate_tick_to_ohlcv():
    buffer = []
    current_minute = None

    while True:
        time.sleep(1)
        with lock:
            if not live_prices:
                continue
            for ts, price in live_prices:
                minute = ts.replace(second=0, microsecond=0)
                if current_minute is None:
                    current_minute = minute
                if minute != current_minute:
                    ohlcv = build_ohlcv(buffer, current_minute)
                    candle_data["1m"] = pd.concat([candle_data["1m"], pd.DataFrame([ohlcv])])
                    candle_data["1m"] = candle_data["1m"].drop_duplicates("time").sort_values("time").tail(RSI_PERIOD + 10)
                    buffer = []
                    current_minute = minute
                buffer.append((ts, price))
            live_prices.clear()

def build_ohlcv(trades, ts):
    prices = [p for _, p in trades]
    return {
        "time": ts,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": len(prices)
    }

def calculate_rsi(series):
    if len(series) >= RSI_PERIOD:
        rsi = RSIIndicator(close=series, window=RSI_PERIOD).rsi()
        return round(rsi.iloc[-1], 2)
    return None

def monitor_rsi():
    while True:
        time.sleep(TERMINAL_PRINT_INTERVAL)
        output = f"[{datetime.now()}] RSI —"
        with lock:
            for tf, df in candle_data.items():
                if not df.empty and "close" in df.columns:
                    rsi = calculate_rsi(df["close"])
                    if rsi is not None:
                        status = f"{rsi} ⚠️" if rsi > 70 or rsi < 30 else f"{rsi}"
                        output += f" {tf}: {status} |"
                        if rsi > 70 or rsi < 30:
                            play_alert_sound()
                    else:
                        output += f" {tf}: Not enough data |"
                else:
                    output += f" {tf}: Waiting... |"
        print(output.strip(" |"))

def play_alert_sound():
    for _ in range(3):
        os.system("afplay /System/Library/Sounds/Ping.aiff")
        time.sleep(0.3)

def on_message(ws, message):
    try:
        data = json.loads(message)
        if data["type"] == "match" and data["product_id"] == SYMBOL:
            price = float(data["price"])
            timestamp = datetime.strptime(data["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
            with lock:
                live_prices.append((timestamp, price))
    except Exception as e:
        print(f"[{datetime.now()}] Error parsing message: {e}")

def on_error(ws, error):
    print(f"[{datetime.now()}] WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"[{datetime.now()}] 🔌 WebSocket closed")

def on_open(ws):
    print(f"[{datetime.now()}] ✅ Subscribing to {SYMBOL}")
    subscribe_msg = {
        "type": "subscribe",
        "channels": [{"name": "matches", "product_ids": [SYMBOL]}]
    }
    ws.send(json.dumps(subscribe_msg))

def main():
    threading.Thread(target=fetch_rest_data, daemon=True).start()
    threading.Thread(target=aggregate_tick_to_ohlcv, daemon=True).start()
    threading.Thread(target=monitor_rsi, daemon=True).start()

    ws = websocket.WebSocketApp(
        "wss://ws-feed.exchange.coinbase.com",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()

if __name__ == "__main__":
    main()
