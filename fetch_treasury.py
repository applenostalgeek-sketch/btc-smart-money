"""
fetch_treasury.py — Corporate Bitcoin treasury holdings
Source: CoinGecko public API (no auth required, no Cloudflare block from GitHub Actions)
  https://api.coingecko.com/api/v3/companies/public_treasury/bitcoin

Output: data/treasury_data.json
  {
    "last_updated": "2026-04-16 10:00 UTC",
    "source": "CoinGecko Public API",
    "companies": [
      {"name": "Strategy", "ticker": "MSTR", "btc": 780897, "usd_value": 58332181197}
      ...
    ]
  }

Only companies with >= 1000 BTC are included.
Exits with code 1 on total failure.
"""

import sys
import json
import os
import requests
from datetime import datetime

os.makedirs('data', exist_ok=True)

JSON_PATH = 'data/treasury_data.json'
COINGECKO_URL = 'https://api.coingecko.com/api/v3/companies/public_treasury/bitcoin'

# Minimum BTC to include (filters noise)
MIN_BTC = 1000

# Name overrides: CoinGecko uses "Strategy" for MicroStrategy (rebranded Jan 2025)
# Keep the original recognizable names for display
NAME_OVERRIDES = {
    'Strategy': 'MicroStrategy',
    'MARA Holdings': 'Marathon',
    'Riot Platforms': 'Riot',
    'Coinbase Global': 'Coinbase',
}

# Ticker cleanup: CoinGecko returns "MSTR.US", "MARA.US", etc.
def clean_ticker(symbol):
    """Remove exchange suffix: 'MSTR.US' → 'MSTR'"""
    return symbol.split('.')[0] if symbol else symbol


def fetch_treasury():
    print('=== fetch_treasury.py — Corporate BTC Holdings ===\n')

    try:
        print(f'[1] Fetching from CoinGecko: {COINGECKO_URL}')
        r = requests.get(COINGECKO_URL, timeout=15)
        r.raise_for_status()
        data = r.json()
        print(f'    HTTP {r.status_code} — got {len(data.get("companies", []))} companies')
    except Exception as e:
        print(f'\n❌ FAILURE: CoinGecko request failed: {e}')
        sys.exit(1)

    raw_companies = data.get('companies', [])
    if not raw_companies:
        print('\n❌ FAILURE: CoinGecko returned empty companies list')
        sys.exit(1)

    companies = []
    for c in raw_companies:
        btc = c.get('total_holdings', 0) or 0
        if btc < MIN_BTC:
            continue

        name = c.get('name', '')
        name = NAME_OVERRIDES.get(name, name)  # apply override if any

        symbol = c.get('symbol', '')
        ticker = clean_ticker(symbol)

        usd_value = c.get('total_current_value_usd', 0) or 0

        companies.append({
            'name': name,
            'ticker': ticker,
            'btc': int(round(btc)),
            'usd_value': int(round(usd_value)),
        })

    if not companies:
        print(f'\n❌ FAILURE: No companies with >= {MIN_BTC} BTC found')
        sys.exit(1)

    # Sort by BTC holdings descending — garder top 15 pour le JSON
    companies.sort(key=lambda c: c['btc'], reverse=True)
    companies = companies[:15]

    output = {
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'source': 'CoinGecko Public API — /api/v3/companies/public_treasury/bitcoin',
        'total_companies': len(companies),
        'companies': companies,
    }

    with open(JSON_PATH, 'w') as f:
        json.dump(output, f, indent=2)

    print(f'\n✅ Saved {JSON_PATH}')
    print(f'   Total companies with >= {MIN_BTC} BTC: {len(companies)}')
    print(f'   Top 10:')
    for c in companies[:10]:
        print(f'     {c["name"]} ({c["ticker"]}): {c["btc"]:,} BTC — ${c["usd_value"]/1e9:.2f}B')
    print(f'   Updated: {output["last_updated"]}')


if __name__ == '__main__':
    fetch_treasury()
