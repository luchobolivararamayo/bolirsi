import websocket
import requests
import threading
import pandas as pd
import time
import json
import os
from ta.momentum import RSIIndicator
from datetime import datetime, timedelta

# Configuration
symbol = "BTC-USD"
rsi_period = 14
print_interval = 5  # seconds

candles = {
    "1m": [],
    "5m": [],
    "15m": []
}

rsi_values = {
    "1m": None,
    "5m": None,
    "15m": None
}

def fetch_candles_granularity(granularity):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/candles?granularity={granularity}"
    response = requests.get(url)
    data = response.json()
    if isinstance(data, list):
        df = pd.DataFrame(data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
        df.sort_values('time', inplace=True)
        return df
    else:
        print(f"[Error fetching {granularity}s candles] {data}")
        return pd.DataFrame()

def update_candle_data():
    while True:
        candles["1m"] = fetch_candles_granularity(60)['close'].tolist()
        candles["5m"] = fetch_candles_granularity(300)['close'].tolist()
        candles["15m"] = fetch_candles_granularity(900)['close'].tolist()
        time.sleep(60)  # Refresh every minute for accuracy

def calculate_rsi():
    for tf in ["1m", "5m", "15m"]:
        closes = candles[tf]
        if len(closes) >= rsi_period:
            rsi = RSIIndicator(pd.Series(closes), window=rsi_period).rsi()
            rsi_values[tf] = rsi.iloc[-1]
        else:
            rsi_values[tf] = None

def play_alert_sound(timeframe):
    sound_map = {
        "1m": "/System/Library/Sounds/Ping.aiff",
        "5m": "/System/Library/Sounds/Pop.aiff",
        "15m": "/System/Library/Sounds/Submarine.aiff"
    }
    sound_path = sound_map.get(timeframe, "/System/Library/Sounds/Ping.aiff")
    for _ in range(3):
        os.system(f"afplay {sound_path}")
        time.sleep(0.3)

def print_rsi_loop():
    while True:
        calculate_rsi()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        output = [f"[{now}] RSI:"]
        for tf in ["1m", "5m", "15m"]:
            val = rsi_values[tf]
            if val is None:
                output.append(f"{tf}: Waiting for data...")
            else:
                status = "Normal"
                if val > 70:
                    status = "Overbought ⚠️"
                    play_alert_sound(tf)
                elif val < 30:
                    status = "Oversold ⚠️"
                    play_alert_sound(tf)
                output.append(f"{tf}: {val:.2f} ({status})")

        print(" | ".join(output))
        time.sleep(print_interval)

def on_open(ws):
    print(f"[{datetime.now()}] ✅ Subscribing to {symbol} for keep-alive")
    subscribe_msg = {
        "type": "subscribe",
        "channels": [{"name": "heartbeat", "product_ids": [symbol]}]
    }
    ws.send(json.dumps(subscribe_msg))

def on_message(ws, message):
    pass  # Just used to keep the connection alive

def on_error(ws, error):
    print(f"❌ WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WebSocket closed")

def run_websocket():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://ws-feed.exchange.coinbase.com",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
        except Exception as e:
            print(f"⚠️ WebSocket exception: {e}")
        print("🔁 Reconnecting to WebSocket in 5 seconds...")
        time.sleep(5)

def main():
    threading.Thread(target=update_candle_data, daemon=True).start()
    threading.Thread(target=print_rsi_loop, daemon=True).start()
    run_websocket()

if __name__ == "__main__":
    main()
