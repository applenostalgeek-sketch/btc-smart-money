"""
fetch_etf_flows.py — Scrape les flux ETF Bitcoin depuis Farside Investors
Source : https://farside.co.uk/btc/
Données : flux quotidiens en $M pour IBIT, FBTC, ARKB, GBTC, etc.
Sauvegarde : data/etf_flows.json
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import os
from datetime import datetime, timedelta

os.makedirs('data', exist_ok=True)

URL = 'https://farside.co.uk/btc/'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/123.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com/',
}

# ETFs à suivre (dans l'ordre Farside)
ETF_COLS = ['IBIT', 'FBTC', 'BITB', 'ARKB', 'BTCO', 'EZBC', 'BRRR',
            'HODL', 'BTCW', 'MSBT', 'GBTC', 'BTC', 'Total']


def parse_value(s):
    """Convertit '(9.4)' → -9.4, '160.8' → 160.8, '' → None."""
    s = s.strip()
    if not s or s == '-':
        return None
    # Valeur négative entre parenthèses
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
    """Convertit '23 Mar 2026' → '2026-03-23'."""
    s = s.strip()
    for fmt in ('%d %b %Y', '%d %B %Y', '%m/%d/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def scrape_farside():
    print(f"  Fetching {URL}")
    session = requests.Session()
    r = session.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    print(f"  HTTP {r.status_code} — {len(r.text)} chars")

    soup = BeautifulSoup(r.text, 'html.parser')
    tables = soup.find_all('table')
    if not tables:
        raise ValueError("Aucune table trouvée sur la page")

    # Prendre la table principale (la plus grande)
    table = max(tables, key=lambda t: len(t.find_all('tr')))
    rows = table.find_all('tr')

    # Détecter les colonnes depuis le header
    # Les noms de colonnes sont dans les <th> avec \xa0 et spans imbriqués
    col_idx = {}
    for row in rows[:5]:
        cells = row.find_all(['th', 'td'])
        # Nettoyer le texte : supprimer \xa0, strip, upper
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
            col_idx['DATE'] = 0  # toujours colonne 0
            print(f"  Colonnes trouvées: {col_idx}")
            break

    if not col_idx:
        # Fallback : positions fixes basées sur structure Farside connue
        # Date, IBIT, FBTC, BITB, ARKB, BTCO, EZBC, BRRR, HODL, BTCW, MSBT, GBTC, BTC, Total
        names = ['DATE'] + ETF_COLS
        col_idx = {name: i for i, name in enumerate(names)}
        print(f"  Fallback positions fixes: {col_idx}")

    # Parser les lignes de données
    rows_data = []
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 3:
            continue
        texts = [c.get_text(strip=True) for c in cells]

        # Première colonne = date
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

        # Recalculer le total si absent ou si on a les composants
        if 'total' not in entry:
            component_sum = sum(
                entry.get(e.lower(), 0) or 0
                for e in ETF_COLS[:-1]  # tous sauf 'Total'
            )
            if component_sum != 0:
                entry['total'] = round(component_sum, 2)

        if 'total' in entry or 'ibit' in entry:
            rows_data.append(entry)

    # Trier par date ASC
    rows_data.sort(key=lambda x: x['date'])
    return rows_data


def compute_stats(rows):
    """Calcule les stats clés depuis l'historique."""
    if not rows:
        return {}

    totals = [r.get('total', 0) or 0 for r in rows]
    latest = rows[-1]

    # Cumul 7 jours et 30 jours
    net_7d  = sum(r.get('total', 0) or 0 for r in rows[-7:])
    net_30d = sum(r.get('total', 0) or 0 for r in rows[-30:])

    # Jours consécutifs d'inflow/outflow
    direction = 'inflow' if (latest.get('total', 0) or 0) >= 0 else 'outflow'
    streak = 0
    for r in reversed(rows):
        d = 'inflow' if (r.get('total', 0) or 0) >= 0 else 'outflow'
        if d == direction:
            streak += 1
        else:
            break

    # Signal
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


def main():
    print("=== fetch_etf_flows.py — Bitcoin ETF Flows (Farside) ===\n")

    try:
        rows = scrape_farside()
        print(f"\n  ✓ {len(rows)} jours de données parsés")
        if rows:
            print(f"  Du {rows[0]['date']} au {rows[-1]['date']}")
    except Exception as e:
        print(f"\n❌ Erreur scraping Farside: {e}")
        # Essai fallback URL alternatif
        try:
            print("  Tentative sur /bitcoin-etf-flow-all-data/ …")
            global URL
            URL = 'https://farside.co.uk/bitcoin-etf-flow-all-data/'
            rows = scrape_farside()
            print(f"  ✓ {len(rows)} jours via URL alternative")
        except Exception as e2:
            print(f"❌ Fallback échoué aussi: {e2}")
            return

    if not rows:
        print("❌ Aucune donnée parsée")
        return

    stats = compute_stats(rows)
    latest = rows[-1]

    output = {
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M UTC'),
        'source':       'Farside Investors — farside.co.uk/btc/',
        'latest':       latest,
        'signal':       stats.get('signal', 'neutral'),
        'net_7d':       stats.get('net_7d', 0),
        'net_30d':      stats.get('net_30d', 0),
        'direction':    stats.get('direction', 'neutral'),
        'streak':       stats.get('streak', 0),
        # Garder les 90 derniers jours pour le graphique
        'history':      rows[-90:],
    }

    out_path = 'data/etf_flows.json'
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Saved {out_path}")
    print(f"   Signal : {stats['signal']}")
    print(f"   Flux 7j : {stats['net_7d']:+.0f}M$")
    print(f"   Flux 30j: {stats['net_30d']:+.0f}M$")
    print(f"   Dernière journée ({latest['date']}) : "
          f"{latest.get('total', '?'):+.1f}M$ total, "
          f"IBIT {latest.get('ibit', '?')}M$")


if __name__ == '__main__':
    main()
