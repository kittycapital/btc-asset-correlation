"""
Bitcoin Correlation Dashboard - Data Update Script
Downloads latest data for BTC, SPY, QQQ, IGV, GLD, DXY
and generates data.json for the dashboard.

Usage:
    pip install yfinance pandas
    python update_data.py
"""

import yfinance as yf
import pandas as pd
import json
import math
import os
from datetime import datetime, timedelta

# === Config ===
TICKERS = {
    'BTC': 'BTC-USD',
    'SPY': 'SPY',
    'QQQ': 'QQQ',
    'IGV': 'IGV',
    'GLD': 'GLD',
    'DXY': 'DX-Y.NYB',
}

DATA_DIR = 'data'
OUTPUT_JSON = 'data.json'
START_DATE = '2014-01-01'

CORRELATION_ASSETS = ['SPY', 'QQQ', 'IGV', 'GLD', 'DXY']
CORRELATION_PERIODS = [13, 26, 52]


def download_data():
    """Download or update CSV files for all tickers."""
    os.makedirs(DATA_DIR, exist_ok=True)

    for name, ticker in TICKERS.items():
        csv_path = os.path.join(DATA_DIR, f'{name}.csv')
        print(f'[{name}] Downloading {ticker}...')

        try:
            df = yf.download(
                ticker,
                start=START_DATE,
                end=(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'),
                interval='1d',
                auto_adjust=False,
            )

            if df.empty:
                print(f'  ⚠️  {name}: empty data, skipping')
                continue

            # Handle multi-level columns (newer yfinance)
            if hasattr(df.columns, 'levels') and len(df.columns.levels) > 1:
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df = df[['Date', 'Close', 'High', 'Low', 'Open', 'Volume']]
            df['Volume'] = df['Volume'].fillna(0).astype(int)
            df.to_csv(csv_path, index=False)
            print(f'  ✅ {csv_path} ({len(df)} rows)')

        except Exception as e:
            print(f'  ❌ {name} failed: {e}')


def generate_json():
    """Process CSV files and generate data.json for the dashboard."""
    dfs = {}
    for name in TICKERS.keys():
        csv_path = os.path.join(DATA_DIR, f'{name}.csv')
        if not os.path.exists(csv_path):
            print(f'  ⚠️  {csv_path} not found, skipping')
            continue
        df = pd.read_csv(csv_path, parse_dates=['Date'])
        df = df.sort_values('Date').reset_index(drop=True)
        dfs[name] = df

    if 'BTC' not in dfs:
        print('❌ BTC data not found!')
        return

    # BTC weekly candles
    btc = dfs['BTC'].set_index('Date')
    btc_weekly = btc.resample('W-MON', label='left', closed='left').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
    }).dropna()

    # Weekly closes for all assets
    weekly_closes = {}
    for name, df in dfs.items():
        s = df.set_index('Date')['Close'].resample('W-MON', label='left', closed='left').last().dropna()
        weekly_closes[name] = s

    combined = pd.DataFrame(weekly_closes).dropna(subset=['BTC'])
    returns = combined.pct_change().dropna()

    # Helper functions
    def clean(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return round(float(v), 2)

    def clean_corr(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return round(float(v), 4)

    # Build candle data
    candles = []
    for dt, row in btc_weekly.iterrows():
        candles.append({
            't': dt.strftime('%Y-%m-%d'),
            'o': clean(row['Open']),
            'h': clean(row['High']),
            'l': clean(row['Low']),
            'c': clean(row['Close']),
        })

    # Build correlation data
    correlations = {}
    for period in CORRELATION_PERIODS:
        corr_list = []
        for i, dt in enumerate(returns.index):
            entry = {'t': dt.strftime('%Y-%m-%d')}
            for asset in CORRELATION_ASSETS:
                if asset in returns.columns and i >= period - 1:
                    window = returns.iloc[i - period + 1:i + 1]
                    if len(window) == period:
                        c = window['BTC'].corr(window[asset])
                        entry[asset] = clean_corr(c)
                    else:
                        entry[asset] = None
                else:
                    entry[asset] = None
            corr_list.append(entry)
        correlations[str(period)] = corr_list

    # Weekly close prices for overlay (normalized)
    prices = {}
    for asset in CORRELATION_ASSETS:
        if asset in combined.columns:
            series = combined[asset].dropna()
            prices[asset] = [
                {'t': dt.strftime('%Y-%m-%d'), 'v': clean(v)}
                for dt, v in series.items()
            ]

    output = {
        'lastUpdated': btc_weekly.index.max().strftime('%Y-%m-%d'),
        'btcLatest': clean(btc_weekly['Close'].iloc[-1]),
        'candles': candles,
        'correlations': correlations,
    }

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(output, f)

    size_kb = os.path.getsize(OUTPUT_JSON) / 1024
    print(f'\n✅ {OUTPUT_JSON} generated ({size_kb:.1f} KB)')
    print(f'   Candles: {len(candles)}, Last: {output["lastUpdated"]}')
    print(f'   BTC Latest: ${output["btcLatest"]:,.2f}')


if __name__ == '__main__':
    print('=' * 50)
    print('Bitcoin Correlation Dashboard - Data Update')
    print('=' * 50)
    download_data()
    print()
    generate_json()
    print('\n✅ All done!')
