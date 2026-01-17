import pandas as pd
import numpy as np

import yfinance as yf

def get_stock_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = yf.download(symbol, start=start_date, end=end_date)
    df.reset_index(inplace=True)
    return df

def get_dummy_market_data():
    return pd.DataFrame({
        "symbol": ["RELIANCE", "ICICIBANK", "TCS"],
        "avg_volume_20d": [1.8, 2.2, 0.9],
        "atr_percent": [1.6, 1.2, 0.7],
        "price_vs_vwap": [0.2, -0.1, 0.05]
    })
