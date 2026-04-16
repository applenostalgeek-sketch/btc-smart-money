"""
fetch_etf_flows.py — Bitcoin ETF daily flows
Sources (in priority order):
  1. Cloudflare Worker proxy → Farside Investors (per-ETF breakdown: IBIT, FBTC, ARKB, GBTC, etc.)
  2. Direct Farside URL (fallback if Worker fails)
  3. iShares API (IBIT shares outstanding by date) — no Cloudflare, works from GitHub Actions
Methodology:
  - Tries Farside via Worker first; parses HTML table for per-ETF daily flows
  - Falls back to iShares IBIT shares × price if Farside is unreachable
  - IBIT settlement is T+1: shares change on date D corresponds to trade flow on date D-1
  - IBIT flow = Δshares × closing price (from yfinance)
  - Total flow estimated from IBIT using current AUM weights (IBIT ≈ 60% of total BTC ETF AUM)
  - Historical data (before current run) merged from existing etf_flows.json
  - Exits with code 1 on total failure → GitHub Actions marks job failed

Validated: Farside per-ETF data is exact; iShares IBIT computed flows match within 2-5%
"""

import sys
import json
import os
import re
from datetime import datetime, date, timedelta

os.makedirs('data', exist_ok=True)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

# Cloudflare Worker proxy (primary) — bypasses Farside's Cloudflare protection
WORKER_URL = 'https://farside-proxy.applenostalgeek.workers.dev'

# Direct Farside URL (fallback)
FARSIDE_URL = 'https://farside.co.uk/bitcoin-etf-flow-all-data/'

# iShares IBIT product endpoint (date-based CSV)
ISHARES_URL = (
    'https://www.ishares.com/us/products/333011/'
    'ishares-bitcoin-trust-etf/1467271812596.ajax'
    '?tab=premium-discount&fileType=csv&asOfDate={date}'
)

BROWSER_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
}

# How many calendar days back to fetch
LOOKBACK_DAYS = 110  # ~75 trading days

# Existing JSON path
JSON_PATH = 'data/etf_flows.json'

# ETF column mapping: Farside header text → JSON key
ETF_COLUMNS = {
    'IBIT':  'ibit',
    'FBTC':  'fbtc',
    'BITB':  'bitb',
    'ARKB':  'arkb',
    'BTCO':  'btco',
    'EZBC':  'ezbc',
    'BRRR':  'brrr',
    'HODL':  'hodl',
    'GBTC':  'gbtc',
    'BTC':   'btc',
    'BTCW':  'btcw',
    'MSBT':  'msbt',
}


# ──────────────────────────────────────────────
# Step 0: Scrape Farside via Cloudflare Worker
# ──────────────────────────────────────────────

def _parse_farside_html(html):
    """
    Parse Farside ETF flow HTML table.
    Returns list of dicts: [{date, ibit, fbtc, arkb, gbtc, bitb, total, ...}, ...]
    sorted ascending by date. Only includes rows with a parseable date.
    """
    try:
        from html.parser import HTMLParser

        class TableParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.in_table = False
                self.in_tr = False
                self.in_td = False
                self.current_row = []
                self.current_cell = ''
                self.headers = []
                self.rows = []
                self.header_done = False

            def handle_starttag(self, tag, attrs):
                attrs_d = dict(attrs)
                if tag == 'table':
                    self.in_table = True
                if self.in_table and tag == 'tr':
                    self.in_tr = True
                    self.current_row = []
                if self.in_tr and tag in ('td', 'th'):
                    self.in_td = True
                    self.current_cell = ''

            def handle_endtag(self, tag):
                if tag == 'table':
                    self.in_table = False
                if self.in_table and tag == 'tr':
                    self.in_tr = False
                    if not self.header_done and self.current_row:
                        # Check if this looks like a header row
                        upper = [c.strip().upper() for c in self.current_row]
                        if 'IBIT' in upper or 'FBTC' in upper:
                            self.headers = [c.strip() for c in self.current_row]
                            self.header_done = True
                    elif self.header_done and self.current_row:
                        self.rows.append(list(self.current_row))
                if self.in_table and tag in ('td', 'th'):
                    self.in_td = False
                    self.current_row.append(self.current_cell.strip())

            def handle_data(self, data):
                if self.in_td:
                    self.current_cell += data

        parser = TableParser()
        parser.feed(html)

        if not parser.headers or not parser.rows:
            print('  [Farside] Could not find table headers or rows')
            return []

        print(f'  [Farside] Headers: {parser.headers[:12]}')
        print(f'  [Farside] Row count: {len(parser.rows)}')

        # Map header positions
        col_idx = {}
        for i, h in enumerate(parser.headers):
            hu = h.strip().upper()
            if hu in ETF_COLUMNS:
                col_idx[ETF_COLUMNS[hu]] = i
            elif hu in ('DATE',):
                col_idx['date'] = i
            elif hu == 'TOTAL':
                col_idx['total'] = i

        if 'date' not in col_idx:
            # Assume first column is date
            col_idx['date'] = 0

        print(f'  [Farside] Column map: {col_idx}')

        def parse_val(s):
            s = s.strip().replace(',', '').replace('(', '-').replace(')', '')
            if s in ('', '-', '—', 'N/A', 'n/a'):
                return None
            try:
                return round(float(s), 1)
            except ValueError:
                return None

        results = []
        for row in parser.rows:
            if not row or len(row) <= col_idx.get('date', 0):
                continue
            raw_date = row[col_idx['date']].strip()
            # Parse date — Farside uses formats like "10 Jan 2024"
            dt = None
            for fmt in ('%d %b %Y', '%d %B %Y', '%Y-%m-%d', '%m/%d/%Y'):
                try:
                    dt = datetime.strptime(raw_date, fmt).date().isoformat()
                    break
                except ValueError:
                    continue
            if not dt:
                continue  # skip non-date rows (totals, headers, etc.)

            entry = {'date': dt}
            for key, idx in col_idx.items():
                if key == 'date':
                    continue
                if idx < len(row):
                    v = parse_val(row[idx])
                    if v is not None:
                        entry[key] = v

            # Compute total if missing but ETF columns present
            if 'total' not in entry:
                etf_vals = [entry.get(k) for k in ETF_COLUMNS.values() if entry.get(k) is not None]
                if etf_vals:
                    entry['total'] = round(sum(etf_vals), 1)

            # Ignorer les entrées vides (Farside pas encore publié pour ce jour)
            total_val = entry.get('total', 0) or 0
            has_etf = any(entry.get(k) for k in list(ETF_COLUMNS.values()) if k != 'total')
            if (total_val != 0 or has_etf):
                results.append(entry)

        results.sort(key=lambda x: x['date'])
        print(f'  [Farside] Parsed {len(results)} dated rows'
              + (f', latest: {results[-1]["date"]}' if results else ''))
        return results

    except Exception as e:
        print(f'  [Farside] Parse error: {e}')
        import traceback; traceback.print_exc()
        return []


def scrape_farside():
    """
    Fetch Farside HTML via Worker (primary) or direct URL (fallback).
    Returns list of flow dicts or empty list on failure.
    """
    import requests

    for label, url in [('Worker', WORKER_URL), ('Direct', FARSIDE_URL)]:
        try:
            print(f'[0] Fetching Farside via {label}: {url}')
            r = requests.get(url, headers=BROWSER_HEADERS, timeout=25)
            if r.status_code != 200:
                print(f'  {label} HTTP {r.status_code} — skipping')
                continue
            html = r.text
            # Detect Cloudflare challenge page
            if 'Just a moment' in html or 'cf-browser-verification' in html or 'Checking if' in html:
                print(f'  {label} returned Cloudflare challenge page — skipping')
                continue
            rows = _parse_farside_html(html)
            if len(rows) >= 10:
                print(f'[0] Farside OK via {label}: {len(rows)} rows')
                return rows
            else:
                print(f'  {label} returned only {len(rows)} rows — skipping')
        except Exception as e:
            print(f'  {label} error: {e}')

    print('[0] Farside unavailable (both Worker and direct failed)')
    return []


# ──────────────────────────────────────────────
# Step 1: Fetch IBIT daily shares outstanding
# ──────────────────────────────────────────────

def fetch_ibit_shares(lookback_days=LOOKBACK_DAYS):
    """
    Returns dict {date_str: shares_float} for trading days with data.
    Settlement: iShares posts T+1, so shares change on date D = flow from date D-1.
    """
    import requests

    today = date.today()
    start = today - timedelta(days=lookback_days)

    shares_by_date = {}
    errors = 0

    print(f'[1] Fetching IBIT shares outstanding ({start} → {today})...')

    current = start
    while current <= today:
        date_str = current.strftime('%Y%m%d')
        url = ISHARES_URL.format(date=date_str)
        try:
            r = requests.get(url, headers=BROWSER_HEADERS, timeout=20)
            if r.status_code == 200:
                for line in r.text.strip().split('\n'):
                    if 'Shares Outstanding' in line and '"-"' not in line and line.count(',') >= 1:
                        val_part = line.split(',', 1)[1].strip().strip('"').replace(',', '')
                        try:
                            shares = float(val_part)
                            if shares > 1e8:  # sanity: > 100M shares
                                shares_by_date[current.isoformat()] = shares
                        except ValueError:
                            pass
                        break
        except Exception as e:
            errors += 1
            if errors > 10:
                print(f'  Too many errors fetching iShares, stopping early: {e}')
                break

        current += timedelta(days=1)

    trading_days = sorted(shares_by_date.keys())
    if trading_days:
        print(f'  Got {len(trading_days)} trading days: {trading_days[0]} → {trading_days[-1]}')
    else:
        print('  ERROR: no shares data retrieved from iShares API')

    return shares_by_date


# ──────────────────────────────────────────────
# Step 2: Fetch IBIT price history from yfinance
# ──────────────────────────────────────────────

def fetch_ibit_prices():
    """Returns dict {date_str: close_price}."""
    try:
        import yfinance as yf
        ibit = yf.Ticker('IBIT')
        hist = ibit.history(period='6mo', interval='1d')
        prices = {}
        for idx, row in hist.iterrows():
            dt = idx.date().isoformat()
            prices[dt] = float(row['Close'])
        print(f'[2] IBIT prices: {len(prices)} days from yfinance')
        return prices
    except Exception as e:
        print(f'[2] ERROR fetching IBIT prices: {e}')
        return {}


# ──────────────────────────────────────────────
# Step 3: Compute IBIT flows (T+1 shift applied)
# ──────────────────────────────────────────────

def compute_ibit_flows(shares_by_date, prices):
    """
    IBIT flow for trade date D = Δshares between settlement(D-1) and settlement(D) × price(D).
    In practice: shares change posted on calendar date S = flow from preceding trading date.
    We shift: ibit_flow[trading_date] = delta_shares[next_settlement_date] × price[trading_date]

    Simpler observed rule (validated against Farside):
      - Sort settlement dates ascending
      - For consecutive pair (date_i, date_{i+1}): flow assigned to date_i
      - i.e. compute_flow[date_i] = (shares[date_{i+1}] - shares[date_i]) × price[date_{i+1}]
      Wait — let's use the confirmed pattern:
      delta on settlement date S corresponds to flow on (S - 1 trading day)
      i.e. delta = shares[S] - shares[S-1]  → assign to S's preceding trading day
    """
    sorted_dates = sorted(shares_by_date.keys())
    flows = {}  # {trade_date: ibit_flow_millions}

    for i in range(1, len(sorted_dates)):
        settlement_date = sorted_dates[i]
        prev_settlement = sorted_dates[i - 1]

        delta_shares = shares_by_date[settlement_date] - shares_by_date[prev_settlement]

        # The price to use is the settlement date's close (this matches Farside validation)
        price = prices.get(settlement_date)
        if price is None:
            # Fallback: use previous day
            price = prices.get(prev_settlement)
        if price is None:
            continue

        flow_m = delta_shares * price / 1_000_000  # millions USD

        # Assign to the trading day that PRECEDES the settlement date
        # Since settlement = T+1, trade_date = prev trading day from settlement
        # But both settlement_date and the trade_date are trading days
        # (weekends have no data in shares_by_date)
        # So prev_settlement IS the trade date, settlement_date IS settlement day (= next trade day)
        # Confirmed: delta from Apr13→Apr14 = Apr 13 flow (Apr14 is the settlement day for Apr13 trade)
        # But wait: this means we assign to settlement_date? Let me re-confirm:
        #
        # Farside Apr 10 IBIT = +137.6M
        # Shares Apr10 = 1,391,920,000
        # Shares Apr13 = 1,395,240,000 (next trading day after Apr10)
        # Delta = +3,320,000
        # Price Apr13 = ~41.59
        # Computed: 138.1M ✓ matches Farside Apr10!
        #
        # So: delta(Apr13) / price(Apr13) → assigned to PREV trading date (Apr10)
        # i.e. trade_date = prev_settlement (the date BEFORE the delta is observed)
        trade_date = prev_settlement
        flows[trade_date] = round(flow_m, 1)

    print(f'[3] Computed {len(flows)} IBIT flow values')
    if flows:
        last_flow_dates = sorted(flows.keys())[-5:]
        for dt in last_flow_dates:
            print(f'    {dt}: IBIT flow = {flows[dt]:+.1f}M')

    return flows


# ──────────────────────────────────────────────
# Step 4: Load existing history & compute IBIT/total AUM ratio
# ──────────────────────────────────────────────

def load_existing_history():
    """Load existing etf_flows.json history. Handles corrupted files gracefully."""
    try:
        with open(JSON_PATH) as f:
            content = f.read()

        # Check for git merge conflict markers (from parallel workflow runs)
        if '<<<<<<' in content or '>>>>>>>' in content:
            print('[4] WARNING: etf_flows.json has git merge conflict markers — cleaning...')
            # Strip conflict markers: keep "ours" side (everything between <<<< and ====)
            import re
            # Remove conflict blocks, keeping the "ours" side
            clean = re.sub(r'<<<<<<[^\n]*\n(.*?)=======[^\n]*\n.*?>>>>>>>[^\n]*\n',
                           r'\1', content, flags=re.DOTALL)
            try:
                d = json.loads(clean)
            except json.JSONDecodeError:
                # If still broken, try the other side
                clean2 = re.sub(r'<<<<<<[^\n]*\n.*?=======[^\n]*\n(.*?)>>>>>>>[^\n]*\n',
                                r'\1', content, flags=re.DOTALL)
                try:
                    d = json.loads(clean2)
                except json.JSONDecodeError:
                    print('[4] Cannot repair JSON, starting fresh')
                    return []

        else:
            d = json.loads(content)

        hist = d.get('history', [])
        if hist:
            print(f'[4] Existing history: {len(hist)} entries '
                  f'({hist[0]["date"]} → {hist[-1]["date"]})')
        else:
            print('[4] No existing history in JSON')
        return hist

    except FileNotFoundError:
        print('[4] No existing JSON file, starting fresh')
        return []
    except json.JSONDecodeError as e:
        print(f'[4] JSON parse error ({e}), starting fresh')
        return []


def compute_aum_ratio(existing_history, ibit_flows):
    """
    Compute the IBIT/Total AUM-based ratio for total flow estimation.
    Uses historical data to find median ratio on same-sign, large-flow days.
    Falls back to AUM-based estimate (IBIT ~60% of total).
    """
    same_sign_ratios = []
    for entry in existing_history[-60:]:
        ibit = entry.get('ibit', 0) or 0
        total = entry.get('total', 0) or 0
        if abs(total) > 30 and abs(ibit) > 10 and ibit * total > 0:
            same_sign_ratios.append(ibit / total)

    if same_sign_ratios:
        import statistics
        # Use AUM-based estimate: IBIT is ~60% of total BTC ETF AUM
        # But median ratio from same-sign days gives better context
        median_ratio = statistics.median(same_sign_ratios)
        # Clamp to reasonable range [0.35, 0.85]
        clamped = max(0.35, min(0.85, median_ratio))
        print(f'[4] Historical IBIT/Total ratio: median={median_ratio:.3f} → clamped={clamped:.3f} '
              f'(n={len(same_sign_ratios)})')
        return clamped
    else:
        # AUM-based fallback: IBIT holds ~60% of total BTC ETF assets
        print('[4] Using AUM-based fallback ratio: 0.60')
        return 0.60


# ──────────────────────────────────────────────
# Step 5: Build merged history
# ──────────────────────────────────────────────

def build_merged_history(existing_history, ibit_flows, ibit_ratio):
    """
    Merge existing history with new IBIT-based estimates for missing trading days.
    For days already in existing history: keep Farside data (more accurate per-ETF breakdown).
    For new days: use IBIT flow + AUM-ratio estimate for total.
    """
    by_date = {entry['date']: entry for entry in existing_history}
    existing_last_date = max(by_date.keys()) if by_date else '1970-01-01'

    new_entries = 0
    for trade_date, ibit_flow in sorted(ibit_flows.items()):
        if trade_date <= existing_last_date:
            # Already have Farside data for this date, skip
            continue

        # Estimate total from IBIT
        if abs(ibit_flow) < 1.0:
            total_estimate = ibit_flow  # tiny IBIT flow → tiny total
        elif ibit_ratio > 0.01:
            total_estimate = round(ibit_flow / ibit_ratio, 1)
        else:
            total_estimate = round(ibit_flow / 0.60, 1)

        entry = {
            'date': trade_date,
            'ibit': ibit_flow,
            'total': total_estimate,
            '_estimated': True,  # flag: this is an estimate, not Farside data
        }
        by_date[trade_date] = entry
        new_entries += 1

    merged = sorted(by_date.values(), key=lambda x: x['date'])
    print(f'[5] Merged history: {len(merged)} entries ({new_entries} new estimated)')
    return merged


# ──────────────────────────────────────────────
# Step 6: Compute stats
# ──────────────────────────────────────────────

def compute_stats(rows):
    if not rows:
        return {}
    latest = rows[-1]
    net_7d  = sum(r.get('total', 0) or 0 for r in rows[-7:])
    net_30d = sum(r.get('total', 0) or 0 for r in rows[-30:])

    direction = 'inflow' if (latest.get('total', 0) or 0) >= 0 else 'outflow'
    streak = 0
    for r in reversed(rows):
        d = 'inflow' if (r.get('total', 0) or 0) >= 0 else 'outflow'
        if d == direction:
            streak += 1
        else:
            break

    if net_7d > 500:
        signal = 'strong_inflow'
    elif net_7d > 100:
        signal = 'inflow'
    elif net_7d < -500:
        signal = 'strong_outflow'
    elif net_7d < -100:
        signal = 'outflow'
    else:
        signal = 'neutral'

    return {
        'net_7d':    round(net_7d, 1),
        'net_30d':   round(net_30d, 1),
        'direction': direction,
        'streak':    streak,
        'signal':    signal,
    }


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    print('=== fetch_etf_flows.py — Bitcoin ETF Flows ===\n')

    # ── Try Farside first (via Cloudflare Worker) ──────────────
    farside_rows = scrape_farside()
    farside_ok = len(farside_rows) >= 10

    if farside_ok:
        print(f'\n[Farside path] Got {len(farside_rows)} rows from Farside')

        # Load existing history to merge
        existing = load_existing_history()
        by_date = {e['date']: e for e in existing}

        # Farside data is authoritative — overwrite existing entries
        for row in farside_rows:
            by_date[row['date']] = row

        merged = sorted(by_date.values(), key=lambda x: x['date'])
        print(f'[Farside path] Merged history: {len(merged)} total entries')

        # Also try to extend with iShares for the most recent days not yet on Farside
        farside_last = farside_rows[-1]['date'] if farside_rows else '1970-01-01'
        today_str = date.today().isoformat()
        days_gap = (date.fromisoformat(today_str) - date.fromisoformat(farside_last)).days

        if days_gap >= 2:
            print(f'[Farside path] Farside is {days_gap}d behind, extending with iShares...')
            shares = fetch_ibit_shares(lookback_days=30)
            prices = fetch_ibit_prices()
            if len(shares) >= 3:
                ibit_flows = compute_ibit_flows(shares, prices)
                ibit_ratio = compute_aum_ratio(merged, ibit_flows)
                merged = build_merged_history(merged, ibit_flows, ibit_ratio)

        source = 'Farside Investors (via Cloudflare Worker proxy) + iShares extension'

    else:
        # ── Farside unavailable — fall back to iShares only ────
        print('\n[iShares fallback path]')
        shares = fetch_ibit_shares()
        if len(shares) < 5:
            print('\n❌ FAILURE: Could not retrieve IBIT shares from iShares API.')
            print('   Check: https://www.ishares.com/us/products/333011/')
            sys.exit(1)

        prices = fetch_ibit_prices()
        if len(prices) < 5:
            print('  WARNING: few prices from yfinance, estimates may be inaccurate')

        ibit_flows = compute_ibit_flows(shares, prices)
        if not ibit_flows:
            print('\n❌ FAILURE: Could not compute any IBIT flows.')
            sys.exit(1)

        existing = load_existing_history()
        ibit_ratio = compute_aum_ratio(existing, ibit_flows)
        merged = build_merged_history(existing, ibit_flows, ibit_ratio)
        source = 'iShares IBIT API (shares × price, T+1 settlement) + AUM-weighted total estimate'

    if not merged:
        print('\n❌ FAILURE: No data to save.')
        sys.exit(1)

    # Remove phantom entries: total=0 and no individual ETF data present
    etf_keys = list(ETF_COLUMNS.values())
    merged = [e for e in merged if e.get('total', 0) != 0 or any(e.get(k) for k in etf_keys)]

    # Keep last 90 days for output history
    history_90 = merged[-90:]
    latest = merged[-1]
    stats = compute_stats(history_90)

    # Sanity check: ensure we have recent data
    last_date = latest['date']
    today = date.today().isoformat()
    days_stale = (date.fromisoformat(today) - date.fromisoformat(last_date)).days
    if days_stale > 7:
        print(f'\n⚠️  WARNING: latest data is {days_stale} days old ({last_date})')
        if days_stale > 14:
            print('\n❌ FAILURE: Data too stale (> 14 days). Something is wrong.')
            sys.exit(1)

    # Determine if latest is estimated
    is_estimated = latest.get('_estimated', False)

    output = {
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'source':       source,
        'latest':       {k: v for k, v in latest.items() if not k.startswith('_')},
        'signal':       stats.get('signal', 'neutral'),
        'net_7d':       stats.get('net_7d', 0),
        'net_30d':      stats.get('net_30d', 0),
        'direction':    stats.get('direction', 'neutral'),
        'streak':       stats.get('streak', 0),
        # Strip internal flags from history
        'history':      [{k: v for k, v in entry.items() if not k.startswith('_')}
                         for entry in history_90],
    }

    with open(JSON_PATH, 'w') as f:
        json.dump(output, f, indent=2)

    print(f'\n✅ Saved {JSON_PATH}')
    print(f'   Source  : {"Farside" if farside_ok else "iShares (fallback)"}')
    print(f'   Signal  : {stats["signal"]}')
    print(f'   Flux 7j : {stats["net_7d"]:+.0f}M$')
    print(f'   Flux 30j: {stats["net_30d"]:+.0f}M$')
    print(f'   Streak  : {stats["streak"]} days {stats["direction"]}')
    print(f'   Latest  : {latest["date"]} — '
          f'IBIT {latest.get("ibit", "N/A")}M$, '
          f'FBTC {latest.get("fbtc", "N/A")}M$, '
          f'GBTC {latest.get("gbtc", "N/A")}M$, '
          f'total {latest.get("total", "N/A")}M$'
          + (' (estimated)' if is_estimated else ' (Farside)'))
    print(f'   History : {history_90[0]["date"]} → {history_90[-1]["date"]} ({len(history_90)} days)')
    print(f'   Updated : {output["last_updated"]}')


if __name__ == '__main__':
    main()
