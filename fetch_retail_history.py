"""
fetch_retail_history.py — Carnet de bord quotidien : positionnement des PARTICULIERS + prix BTC

Pourquoi ce fichier ?
  Binance ne garde que ~30 jours d'historique du ratio de comptes long/short.
  Pour avoir un historique qui DÉPASSE 30 jours, il faut l'enregistrer nous-mêmes,
  jour après jour. Ce script s'en charge : à chaque passage il récupère les ~30
  derniers jours dispo et les fusionne avec ce qu'on a déjà → l'historique grandit.

Sources :
  - Binance globalLongShortAccountRatio (proxy PARTICULIERS) — % de comptes à la hausse
  - Binance topLongShortAccountRatio    (gros traders)       — pour la divergence retail↔pros
  - CryptoCompare histoday              (prix BTC clôture)

Sortie : data/retail_history.json
  {
    "last_updated": "...",
    "history": [
      {"date": "2026-05-01", "retail_long": 0.49, "top_long": 0.51, "btc_close": 71000.0},
      ...
    ]
  }

Le "gagnant" du jour (institutions vs particuliers) n'est PAS stocké ici : il se
calcule à l'affichage à partir du prix (mouvement du lendemain) + des flux ETF.

Exits 1 on total failure.
"""

import sys
import json
import os
import requests
from datetime import datetime, timezone

os.makedirs('data', exist_ok=True)

JSON_PATH = 'data/retail_history.json'
KEEP_DAYS = 400  # plafond de l'historique conservé

BINANCE_GLOBAL = 'https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=1d&limit=30'
BINANCE_TOP    = 'https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=1d&limit=30'
CRYPTOCOMPARE  = 'https://min-api.cryptocompare.com/data/v2/histoday?fsym=BTC&tsym=USD&limit=40'

HEADERS = {'User-Agent': 'Mozilla/5.0 (btc-smart-money daily logger)'}


def ts_to_date(ms):
    """Timestamp Binance (ms, début de période UTC) → 'YYYY-MM-DD'."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()


def fetch_binance_ratio(url, key_name):
    """Retourne {date: longAccount_float} depuis un endpoint ratio Binance."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        out = {}
        for row in r.json():
            d = ts_to_date(int(row['timestamp']))
            out[d] = round(float(row['longAccount']), 4)
        print(f'  [{key_name}] {len(out)} jours ({min(out)} → {max(out)})' if out else f'  [{key_name}] vide')
        return out
    except Exception as e:
        print(f'  [{key_name}] erreur: {e}')
        return {}


def fetch_btc_closes():
    """Retourne {date: close_float} depuis CryptoCompare (clôture quotidienne UTC)."""
    try:
        r = requests.get(CRYPTOCOMPARE, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json().get('Data', {}).get('Data', [])
        out = {}
        for row in data:
            d = datetime.fromtimestamp(int(row['time']), tz=timezone.utc).date().isoformat()
            c = float(row.get('close', 0) or 0)
            if c > 0:
                out[d] = round(c, 2)
        print(f'  [prix BTC] {len(out)} jours ({min(out)} → {max(out)})' if out else '  [prix BTC] vide')
        return out
    except Exception as e:
        print(f'  [prix BTC] erreur: {e}')
        return {}


def load_existing():
    try:
        with open(JSON_PATH) as f:
            d = json.load(f)
        hist = d.get('history', [])
        print(f'[existant] {len(hist)} jours déjà enregistrés'
              + (f' ({hist[0]["date"]} → {hist[-1]["date"]})' if hist else ''))
        return {e['date']: e for e in hist}
    except FileNotFoundError:
        print('[existant] aucun fichier — premier passage (seed 30 jours)')
        return {}
    except (json.JSONDecodeError, KeyError) as e:
        print(f'[existant] fichier illisible ({e}) — on repart de zéro')
        return {}


def main():
    print('=== fetch_retail_history.py — carnet de bord quotidien ===\n')

    print('[1] Binance — particuliers (tous comptes)')
    retail = fetch_binance_ratio(BINANCE_GLOBAL, 'particuliers')
    print('[2] Binance — gros traders')
    top = fetch_binance_ratio(BINANCE_TOP, 'gros traders')
    print('[3] CryptoCompare — prix BTC')
    closes = fetch_btc_closes()

    if not retail and not closes:
        print('\n❌ ÉCHEC : ni le retail ni le prix récupérés.')
        sys.exit(1)

    # Fusion avec l'existant (on conserve tout, on met à jour les jours connus)
    by_date = load_existing()

    # Toutes les dates vues sur cette exécution
    all_dates = set(retail) | set(top) | set(closes)
    for d in all_dates:
        entry = by_date.get(d, {'date': d})
        if d in retail:  entry['retail_long'] = retail[d]
        if d in top:     entry['top_long']    = top[d]
        if d in closes:  entry['btc_close']   = closes[d]
        # Ne garder que les jours qui ont AU MOINS retail OU prix
        if 'retail_long' in entry or 'btc_close' in entry:
            by_date[d] = entry

    merged = sorted(by_date.values(), key=lambda x: x['date'])[-KEEP_DAYS:]

    output = {
        'last_updated': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
        'source': 'Binance global/top LongShortAccountRatio + CryptoCompare (close BTC)',
        'history': merged,
    }
    with open(JSON_PATH, 'w') as f:
        json.dump(output, f, indent=2)

    # Récap
    print(f'\n✅ Saved {JSON_PATH}')
    print(f'   {len(merged)} jours au total ({merged[0]["date"]} → {merged[-1]["date"]})')
    last = merged[-1]
    rl = last.get('retail_long')
    rl_txt = f'{rl*100:.0f}%' if isinstance(rl, (int, float)) else '?'
    print(f'   Dernier : {last["date"]} — particuliers {rl_txt} à la hausse, BTC {last.get("btc_close", "?")}$')


if __name__ == '__main__':
    main()
