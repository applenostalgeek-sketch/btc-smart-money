"""
fetch_etf_flows.py — Flux ETF Bitcoin depuis Farside Investors
Source principale  : https://farside.co.uk/bitcoin-etf-flow-all-data/  (historique complet Jan 2024→)
Source fallback    : https://farside.co.uk/btc/  (dernières semaines seulement)
Sauvegarde         : data/etf_flows.json

Cloudflare mitigation: cloudscraper (simule un vrai navigateur) en priorité,
avec fallback sur requests + headers navigateur.
Exit 1 en cas d'échec total → GitHub Actions marque le job failed.
"""

import sys
import json
import os
from datetime import datetime

os.makedirs('data', exist_ok=True)

# ──────────────────────────────────────────────
# URLs
# ──────────────────────────────────────────────
URL_ALL  = 'https://farside.co.uk/bitcoin-etf-flow-all-data/'  # historique complet
URL_BTCQ = 'https://farside.co.uk/btc/'                         # semaines récentes

BROWSER_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
}

# ETFs dans l'ordre des colonnes Farside
ETF_COLS = ['IBIT', 'FBTC', 'BITB', 'ARKB', 'BTCO', 'EZBC', 'BRRR',
            'HODL', 'BTCW', 'MSBT', 'GBTC', 'BTC', 'Total']


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def get_session():
    """Retourne un objet session — cloudscraper si dispo, sinon requests.Session."""
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'darwin', 'desktop': True}
        )
        print("  Using cloudscraper (Cloudflare-aware)")
        return scraper, True
    except ImportError:
        import requests
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)
        print("  cloudscraper not installed — using requests (may fail on CF-protected IPs)")
        return session, False


def fetch_url(session, url, timeout=45):
    """GET une URL, lève une exception si HTTP != 200 ou si Cloudflare challenge."""
    import requests
    r = session.get(url, headers=BROWSER_HEADERS, timeout=timeout)
    r.raise_for_status()
    if 'Just a moment' in r.text and len(r.text) < 20000:
        raise RuntimeError(f"Cloudflare challenge page reçue pour {url} (HTTP {r.status_code})")
    print(f"  HTTP {r.status_code} — {len(r.text):,} chars depuis {url}")
    return r.text


def parse_value(s):
    """'(9.4)' → -9.4,  '160.8' → 160.8,  '' → None."""
    s = s.strip()
    if not s or s == '-':
        return None
    if s.startswith('(') and s.endswith(')'):
        try:
            return -float(s[1:-1].replace(',', ''))
        except ValueError:
            return None
    try:
        return float(s.replace(',', ''))
    except ValueError:
        return None


def parse_date(s):
    """'23 Mar 2026' → '2026-03-23'."""
    s = s.strip()
    for fmt in ('%d %b %Y', '%d %B %Y', '%m/%d/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


# ──────────────────────────────────────────────
# Parser HTML Farside
# ──────────────────────────────────────────────

def parse_html(html):
    """Parse le HTML Farside et retourne une liste de dicts triée par date ASC."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    if not tables:
        raise ValueError("Aucune table <table> dans le HTML")

    # La table principale est la plus grande
    table = max(tables, key=lambda t: len(t.find_all('tr')))
    rows = table.find_all('tr')
    print(f"  Table principale : {len(rows)} lignes")

    # Détecter les indices de colonnes via le header IBIT
    col_idx = {}
    for row in rows[:5]:
        cells = row.find_all(['th', 'td'])
        texts = [
            c.get_text(separator='', strip=True)
             .replace('\xa0', '').replace(' ', '').upper()
            for c in cells
        ]
        if 'IBIT' in texts:
            for i, t in enumerate(texts):
                for etf in ETF_COLS:
                    if t == etf.upper():
                        col_idx[etf] = i
            col_idx['DATE'] = 0
            print(f"  Colonnes détectées : {col_idx}")
            break

    if not col_idx:
        # Fallback : positions fixes connues
        names = ['DATE'] + ETF_COLS
        col_idx = {name: i for i, name in enumerate(names)}
        print(f"  Colonnes fallback (positions fixes) : {col_idx}")

    rows_data = []
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 3:
            continue
        texts = [c.get_text(strip=True) for c in cells]

        date_str = parse_date(texts[col_idx.get('DATE', 0)])
        if not date_str:
            continue

        entry = {'date': date_str}
        for etf in ETF_COLS:
            idx = col_idx.get(etf)
            if idx is not None and idx < len(texts):
                val = parse_value(texts[idx])
                if val is not None:
                    entry[etf.lower()] = val

        # Recalculer total si absent
        if 'total' not in entry:
            component_sum = sum(
                entry.get(e.lower(), 0) or 0
                for e in ETF_COLS[:-1]
            )
            if component_sum != 0:
                entry['total'] = round(component_sum, 2)

        if 'total' in entry or 'ibit' in entry:
            rows_data.append(entry)

    rows_data.sort(key=lambda x: x['date'])
    return rows_data


# ──────────────────────────────────────────────
# Merge avec l'historique existant
# ──────────────────────────────────────────────

def load_existing_history(path='data/etf_flows.json'):
    """Charge l'historique déjà sauvegardé, retourne [] si absent."""
    try:
        with open(path) as f:
            d = json.load(f)
        hist = d.get('history', [])
        if hist:
            print(f"  Historique existant : {len(hist)} entrées "
                  f"({hist[0]['date']} → {hist[-1]['date']})")
        return hist
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def merge_histories(existing, new_rows):
    """Fusionne deux listes triées par date ; les nouvelles données écrasent les anciennes."""
    by_date = {r['date']: r for r in existing}
    for r in new_rows:
        by_date[r['date']] = r
    merged = sorted(by_date.values(), key=lambda x: x['date'])
    return merged


# ──────────────────────────────────────────────
# Stats
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
    print("=== fetch_etf_flows.py — Bitcoin ETF Flows (Farside) ===\n")

    session, using_cloudscraper = get_session()
    existing = load_existing_history()

    rows = None
    used_url = None

    # Tentative 1 : URL historique complet (la meilleure source)
    print(f"\n[1/4] Tentative URL historique complet : {URL_ALL}")
    try:
        html = fetch_url(session, URL_ALL)
        rows = parse_html(html)
        used_url = URL_ALL
        print(f"  ✓ {len(rows)} jours parsés ({rows[0]['date']} → {rows[-1]['date']})")
    except Exception as e:
        print(f"  ✗ Échec : {e}")

    # Tentative 2 : URL récente + merge avec l'existant
    if not rows:
        print(f"\n[2/4] Tentative URL récente : {URL_BTCQ}")
        try:
            html = fetch_url(session, URL_BTCQ)
            new_rows = parse_html(html)
            if new_rows:
                rows = merge_histories(existing, new_rows)
                used_url = URL_BTCQ + ' (+ historique local)'
                print(f"  ✓ {len(new_rows)} jours nouveaux + {len(existing)} existants "
                      f"→ {len(rows)} total")
        except Exception as e:
            print(f"  ✗ Échec : {e}")

    # Tentative 3 : requests basique sur l'URL complète (si cloudscraper a échoué)
    if not rows and using_cloudscraper:
        print(f"\n[3/4] Tentative requests basique : {URL_ALL}")
        try:
            import requests
            r = requests.get(URL_ALL, headers=BROWSER_HEADERS, timeout=45)
            if r.status_code == 200 and 'Just a moment' not in r.text:
                rows = parse_html(r.text)
                used_url = URL_ALL + ' (requests fallback)'
                print(f"  ✓ {len(rows)} jours parsés")
            else:
                print(f"  ✗ HTTP {r.status_code} ou Cloudflare challenge")
        except Exception as e:
            print(f"  ✗ Échec : {e}")

    # Tentative 4 : requests basique sur l'URL récente + merge
    if not rows:
        print(f"\n[4/4] Tentative requests basique : {URL_BTCQ}")
        try:
            import requests
            r = requests.get(URL_BTCQ, headers=BROWSER_HEADERS, timeout=45)
            if r.status_code == 200 and 'Just a moment' not in r.text:
                new_rows = parse_html(r.text)
                if new_rows:
                    rows = merge_histories(existing, new_rows)
                    used_url = URL_BTCQ + ' (requests fallback + historique local)'
                    print(f"  ✓ {len(new_rows)} nouveaux + merge → {len(rows)} total")
            else:
                print(f"  ✗ HTTP {r.status_code} ou Cloudflare challenge")
        except Exception as e:
            print(f"  ✗ Échec : {e}")

    # Échec total
    if not rows:
        print("\n❌ ÉCHEC TOTAL : impossible de récupérer les données ETF depuis Farside.")
        print("   Farside est probablement protégé par Cloudflare et bloque les IPs datacenter.")
        print("   Vérifiez manuellement : https://farside.co.uk/bitcoin-etf-flow-all-data/")
        sys.exit(1)

    # Sauvegarde
    stats = compute_stats(rows)
    latest = rows[-1]

    output = {
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'source':       f'Farside Investors — {used_url}',
        'latest':       latest,
        'signal':       stats.get('signal', 'neutral'),
        'net_7d':       stats.get('net_7d', 0),
        'net_30d':      stats.get('net_30d', 0),
        'direction':    stats.get('direction', 'neutral'),
        'streak':       stats.get('streak', 0),
        # 90 derniers jours pour le graphique
        'history':      rows[-90:],
    }

    out_path = 'data/etf_flows.json'
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Saved {out_path}")
    print(f"   Signal  : {stats['signal']}")
    print(f"   Flux 7j : {stats['net_7d']:+.0f}M$")
    print(f"   Flux 30j: {stats['net_30d']:+.0f}M$")
    print(f"   Dernière journée ({latest['date']}) : "
          f"{latest.get('total', '?'):+.1f}M$ total, "
          f"IBIT {latest.get('ibit', '?')}M$")
    print(f"   Historique : {rows[0]['date']} → {rows[-1]['date']} ({len(rows)} jours)")


if __name__ == '__main__':
    main()
