"""
fetch_cot.py — Récupère les données COT (Commitments of Traders) CFTC
pour les futures Bitcoin (CME), calcule les positions institutionnelles nettes,
et sauvegarde dans data/cot_data.json

Source officielle : CFTC Traders in Financial Futures (TFF)
Mise à jour : hebdomadaire (vendredi)
"""

import requests
import zipfile
import io
import json
import os
from datetime import datetime, timedelta

try:
    import pandas as pd
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pandas', 'openpyxl', 'xlrd'])
    import pandas as pd

os.makedirs('data', exist_ok=True)

CFTC_COLS = {
    'Market_and_Exchange_Names': 'market',
    'As_of_Date_In_Form_YYMMDD': 'date',
    'Open_Interest_All': 'open_interest',
    'Asset_Mgr_Positions_Long_All': 'inst_long',
    'Asset_Mgr_Positions_Short_All': 'inst_short',
    'Lev_Money_Positions_Long_All': 'hf_long',
    'Lev_Money_Positions_Short_All': 'hf_short',
    'NonRept_Positions_Long_All': 'retail_long',
    'NonRept_Positions_Short_All': 'retail_short',
}

def fetch_cftc_year(year):
    """Télécharge le zip CFTC TFF pour une année donnée."""
    url = f"https://www.cftc.gov/files/dea/history/fut_fin_xls_{year}.zip"
    print(f"  Fetching CFTC {year}: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # Le fichier Excel est le seul fichier dans le zip
        name = [n for n in z.namelist() if n.endswith(('.xls', '.xlsx'))][0]
        with z.open(name) as f:
            df = pd.read_excel(io.BytesIO(f.read()), dtype=str)

    return df

def parse_btc(df):
    """Filtre les lignes Bitcoin et calcule les positions nettes."""

    # Filtrer uniquement le contrat standard Bitcoin CME (pas les micro, nano, perp)
    # Le contrat principal = "BITCOIN - CHICAGO MERCANTILE EXCHANGE"
    all_btc = df[df['Market_and_Exchange_Names'].str.contains('BITCOIN', na=False, case=False)]
    markets = all_btc['Market_and_Exchange_Names'].unique()
    print(f"  Marchés BTC trouvés: {list(markets)}")

    # Priorité : contrat CME standard uniquement
    btc = df[df['Market_and_Exchange_Names'].str.strip() == 'BITCOIN - CHICAGO MERCANTILE EXCHANGE'].copy()
    if btc.empty:
        # Fallback : tout ce qui contient BITCOIN CME
        btc = df[df['Market_and_Exchange_Names'].str.contains('BITCOIN.*CHICAGO', na=False, case=False)].copy()
    if btc.empty:
        print("  ⚠️  Aucune ligne Bitcoin CME standard trouvée")
        return pd.DataFrame()

    print(f"  → Utilisation de: {btc['Market_and_Exchange_Names'].iloc[0]}")

    # Pas d'agrégation nécessaire (un seul marché)
    # Convertir les colonnes numériques d'abord
    num_cols = [
        'Open_Interest_All',
        'Asset_Mgr_Positions_Long_All', 'Asset_Mgr_Positions_Short_All',
        'Lev_Money_Positions_Long_All', 'Lev_Money_Positions_Short_All',
        'NonRept_Positions_Long_All', 'NonRept_Positions_Short_All',
        'Other_Rept_Positions_Long_All', 'Other_Rept_Positions_Short_All',
    ]
    for col in num_cols:
        if col in btc.columns:
            btc[col] = pd.to_numeric(btc[col], errors='coerce').fillna(0)

    # Parser les dates
    def parse_date(s):
        try:
            s = str(s).strip()
            if len(s) == 6:
                return datetime.strptime(s, '%y%m%d').strftime('%Y-%m-%d')
            if '/' in s:  # MM/DD/YYYY
                return datetime.strptime(s, '%m/%d/%Y').strftime('%Y-%m-%d')
            return s[:10]
        except:
            return None

    # Utiliser Report_Date_as_MM_DD_YYYY si disponible, sinon As_of_Date_In_Form_YYMMDD
    date_col = 'Report_Date_as_MM_DD_YYYY' if 'Report_Date_as_MM_DD_YYYY' in btc.columns else 'As_of_Date_In_Form_YYMMDD'
    btc['date'] = btc[date_col].apply(parse_date)
    btc = btc.dropna(subset=['date'])

    # Grouper par date (agréger micro + standard BTC)
    agg_cols = {col: 'sum' for col in num_cols if col in btc.columns}
    btc = btc.groupby('date', as_index=False).agg(agg_cols)
    btc = btc.sort_values('date')

    # NOTE: Depuis 2024, les institutions utilisent les ETFs spot (IBIT, FBTC…)
    # et non les futures. Les Asset_Mgr = 0 est normal.
    # Le signal le plus pertinent est Lev_Money (hedge funds, CTAs).
    # Lev_Money net long = hedge funds haussiers, net short = baissiers.

    # Calculs nets
    btc['hf_long']  = btc.get('Lev_Money_Positions_Long_All', pd.Series(0, index=btc.index))
    btc['hf_short'] = btc.get('Lev_Money_Positions_Short_All', pd.Series(0, index=btc.index))
    btc['inst_long']  = btc.get('Asset_Mgr_Positions_Long_All', pd.Series(0, index=btc.index))
    btc['inst_short'] = btc.get('Asset_Mgr_Positions_Short_All', pd.Series(0, index=btc.index))
    btc['other_long']  = btc.get('Other_Rept_Positions_Long_All', pd.Series(0, index=btc.index))
    btc['other_short'] = btc.get('Other_Rept_Positions_Short_All', pd.Series(0, index=btc.index))

    btc['hf_net']   = btc['hf_long']   - btc['hf_short']
    btc['inst_net'] = btc['inst_long']  - btc['inst_short']

    # Signal principal = hedge funds (Lev_Money) car Asset_Mgr ≈ 0 depuis lancement ETFs spot
    # Lev_Money = hedge funds, CTAs, fonds spéculatifs — les plus actifs en futures BTC
    btc = btc.copy()
    btc['inst_net'] = btc['hf_net']  # override: utiliser HF comme proxy smart money

    btc['inst_net_change'] = btc['inst_net'].diff()
    btc['hf_net_change']   = btc['hf_net'].diff()

    return btc

def signal_from_data(recent):
    """Détermine le signal institutionnel à partir des données récentes."""
    if recent.empty or len(recent) < 1:
        return 'neutral', 'Not enough data'

    last  = recent.iloc[-1]
    inst_net = float(last.get('inst_net', 0) or 0)
    inst_chg = float(last.get('inst_net_change', 0) or 0)

    # Signal basé sur la position nette ET la direction du changement
    # Note: depuis 2024, Asset_Mgr = 0 (ils utilisent les ETFs spot)
    # inst_net ici = Lev_Money (hedge funds, CTAs) — les plus actifs en futures
    if inst_net > 0 and inst_chg > 200:
        return 'accumulate_strong', f'HF nets longs +{int(inst_net):,} contrats, achètent agressivement (+{int(inst_chg):,} cette semaine)'
    elif inst_net > 0 and inst_chg >= 0:
        return 'accumulate', f'HF nets longs +{int(inst_net):,} contrats, position stable/haussière'
    elif inst_net > 0 and inst_chg < 0:
        return 'reducing', f'HF nets longs +{int(inst_net):,} contrats, réduisent les longs ({int(inst_chg):,})'
    elif inst_net < 0 and inst_chg > 500:
        return 'covering', f'HF nets courts {int(inst_net):,} mais couvrent massivement (+{int(inst_chg):,}) — retournement haussier possible'
    elif inst_net < 0 and inst_chg > 0:
        return 'covering_light', f'HF nets courts {int(inst_net):,} contrats, couvrent progressivement'
    elif inst_net < 0 and inst_chg < -200:
        return 'short_heavy', f'HF nets courts {int(inst_net):,} contrats, ajoutent des shorts ({int(inst_chg):,})'
    else:
        return 'neutral', f'HF nets courts {int(inst_net):,} contrats, position stable'

def main():
    print("=== fetch_cot.py — CFTC Bitcoin Institutional Data ===\n")

    current_year = datetime.now().year
    all_frames = []

    # Essayer l'année courante + l'année précédente pour avoir ~2 ans
    for year in [current_year - 1, current_year]:
        try:
            df = fetch_cftc_year(year)
            btc = parse_btc(df)
            if not btc.empty:
                all_frames.append(btc)
                print(f"  ✓ {year}: {len(btc)} semaines de données Bitcoin")
        except Exception as e:
            print(f"  ✗ {year}: {e}")

    if not all_frames:
        print("\n❌ Impossible de récupérer les données CFTC")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['date']).sort_values('date')

    # Recalculer les changements sur le dataset complet
    combined['inst_net_change'] = combined['inst_net'].diff()
    combined['hf_net_change']   = combined['hf_net'].diff()

    # Garder les 104 dernières semaines (2 ans)
    recent = combined.tail(104)
    signal, signal_desc = signal_from_data(recent.tail(2))

    # Construire le JSON de sortie
    def safe_int(val, default=0):
        try:
            v = float(val)
            return default if (v != v) else int(v)  # NaN check
        except (TypeError, ValueError):
            return default

    history = []
    for _, row in recent.iterrows():
        history.append({
            'date':            str(row.get('date', '')),
            'inst_net':        safe_int(row.get('inst_net', 0)),
            'inst_long':       safe_int(row.get('inst_long', 0)),
            'inst_short':      safe_int(row.get('inst_short', 0)),
            'hf_net':          safe_int(row.get('hf_net', 0)),
            'inst_net_change': safe_int(row.get('inst_net_change', 0)),
        })

    last_row = recent.iloc[-1] if not recent.empty else {}

    output = {
        'last_updated':    datetime.now().strftime('%Y-%m-%d %H:%M UTC'),
        'report_date':     str(last_row.get('date', 'N/A')),
        'signal':          signal,
        'signal_desc':     signal_desc,
        'inst_net':        safe_int(last_row.get('inst_net', 0)),
        'inst_net_change': safe_int(last_row.get('inst_net_change', 0)),
        'hf_net':          safe_int(last_row.get('hf_net', 0)),
        'history':         history,
        'source':          'CFTC Traders in Financial Futures (TFF) — Official US Government Data',
        'update_frequency': 'Weekly (every Friday for Tuesday data)',
    }

    out_path = 'data/cot_data.json'
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Saved {out_path}")
    print(f"   Signal: {signal}")
    print(f"   {signal_desc}")
    print(f"   Institutional net position: {output['inst_net']:+,} contracts")
    print(f"   Week-over-week change:      {output['inst_net_change']:+,} contracts")
    print(f"   Report date: {output['report_date']}")

if __name__ == '__main__':
    main()
