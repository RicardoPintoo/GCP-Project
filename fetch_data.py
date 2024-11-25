import requests
import pandas as pd

# Binance API Base URL
BASE_URL = "https://api.binance.com/api/v3"

# Get the current price
def get_current_price(symbol="DOGEUSDT"):
    endpoint = f"{BASE_URL}/ticker/price"
    params = {"symbol": symbol}
    response = requests.get(endpoint, params=params)
    data = response.json()
    return {"symbol": data["symbol"], "current_price": float(data["price"])}

# Get last 24 hours stats
def get_24hr_stats(symbol="DOGEUSDT"):
    endpoint = f"{BASE_URL}/ticker/24hr"
    params = {"symbol": symbol}
    response = requests.get(endpoint, params=params)
    data = response.json()
    return {
        "symbol": data["symbol"],
        "price_change": float(data["priceChange"]),
        "price_change_percent": float(data["priceChangePercent"]),
        "high_price": float(data["highPrice"]),
        "low_price": float(data["lowPrice"]),
        "volume": float(data["volume"]),
        "trades": int(data["count"]),
    }

# Historical klines
def get_historical_klines(symbol="DOGEUSDT", interval="1h", limit=100):
    endpoint = f"{BASE_URL}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(endpoint, params=params)
    data = response.json()
    # to dataframe
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    # change columns
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df = df[["open_time", "open", "high", "low", "close", "volume", "close_time"]]
    return df

if __name__ == "__main__":
    # Actual Price
    current_price = get_current_price()
    print("Actual Price:")
    print(current_price)

    # 24hr stats
    stats_24hr = get_24hr_stats()
    print("\nLast 24hr stats:")
    print(stats_24hr)

    # Candles history
    historical_data = get_historical_klines(limit=50)  # last 50
    print("\nCandles data history:")
    print(historical_data.head())
