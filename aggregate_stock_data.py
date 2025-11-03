import pandas as pd
import json
import argparse
import os


def aggregate_for_symbol(symbol, price_csv=None, news_csv=None, out_json=None):
    """Aggregate market data and news for a given stock symbol.

    Inputs:
      - symbol: stock symbol string (e.g. 'AAPL')
      - price_csv: optional path to price CSV (defaults to SYMBOL_price.csv)
      - news_csv: optional path to news CSV (defaults to SYMBOL_headlines.csv)
      - out_json: optional output JSON path (defaults to SYMBOL_aggregated.json)

    Produces:
      - writes aggregated JSON to out_json
    """
    symbol_up = str(symbol).upper()
    price_csv = price_csv or f"stock_price_history/{symbol_up}_price.csv"
    news_csv = news_csv or f"news_headlines/{symbol_up}_headlines.csv"
    out_json = out_json or f"aggregated_data/{symbol_up}_aggregated.json"

    # Load
    price_df = pd.read_csv(price_csv)
    news_df = pd.read_csv(news_csv)

    # Prices
    price_df['date'] = pd.to_datetime(price_df['date'], format='%Y-%m-%d', errors='coerce')
    price_df = price_df.sort_values('date')[['date', 'high', 'low', 'close', 'volume']]

    # Full calendar & forward-fill
    full_range = pd.date_range(price_df['date'].min(), price_df['date'].max(), freq='D')
    price_daily = (
        price_df.set_index('date')
        .reindex(full_range)
        .rename_axis('date')
        .ffill()
        .reset_index()
    )

    # News: explicit parse with fallback
    parsed = pd.to_datetime(news_df['Date'], format='%Y-%m-%d %H:%M:%S %Z', errors='coerce', utc=True)
    fallback_mask = parsed.isna()
    if fallback_mask.any():
        parsed.loc[fallback_mask] = pd.to_datetime(news_df.loc[fallback_mask, 'Date'], utc=True, errors='coerce')
    news_df['Date_parsed'] = parsed
    news_df = news_df.dropna(subset=['Date_parsed']).copy()

    # Normalize to UTC & extract date/time
    # parsed had utc=True above, so we can safely convert to UTC tz
    news_df['Date_parsed'] = news_df['Date_parsed'].dt.tz_convert('UTC')
    news_df['date_only'] = news_df['Date_parsed'].dt.date
    news_df['time_str'] = news_df['Date_parsed'].dt.strftime('%H:%M:%S')
    # keep the expected column name 'Article_title' to stay compatible with existing CSVs
    news_df = news_df[['date_only', 'time_str', 'Article_title']].sort_values(['date_only', 'time_str'])

    news_by_date = (
        news_df.groupby('date_only', sort=True)
        .apply(lambda g: [{'time': t, 'headline': h} for t, h in zip(g['time_str'], g['Article_title'])])
        .to_dict()
    )

    # Combine
    history = []
    for _, row in price_daily.iterrows():
        date_obj = row['index'] if 'index' in price_daily.columns else row['date']
        if isinstance(date_obj, pd.Timestamp):
            date_str = date_obj.strftime('%Y-%m-%d')
            date_key = date_obj.date()
        else:
            date_str = pd.to_datetime(date_obj).strftime('%Y-%m-%d')
            date_key = pd.to_datetime(date_obj).date()
        history.append({
            "date": date_str,
            "market_data": {
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close']),
                "volume": int(row['volume'])
            },
            "news_headlines": news_by_date.get(date_key, [])
        })

    # Ensure output directory exists if an explicit path was provided
    out_dir = os.path.dirname(out_json)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump({"history": history}, f, indent=2)


def aggregate_for_aapl(price_csv=None, news_csv=None, out_json=None):
    """Backward-compatible wrapper to preserve previous behavior."""
    return aggregate_for_symbol('AAPL', price_csv=price_csv, news_csv=news_csv, out_json=out_json)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Aggregate stock price and news into JSON')
    parser.add_argument('symbol', nargs='?', default='AAPL', help='Stock symbol, e.g. AAPL')
    parser.add_argument('--price-csv', dest='price_csv', help='Path to price CSV (default: SYMBOL_price.csv)')
    parser.add_argument('--news-csv', dest='news_csv', help='Path to news CSV (default: SYMBOL_headlines.csv)')
    parser.add_argument('--out-json', dest='out_json', help='Output JSON path (default: SYMBOL_aggregated.json)')
    args = parser.parse_args()

    aggregate_for_symbol(args.symbol, price_csv=args.price_csv, news_csv=args.news_csv, out_json=args.out_json)
