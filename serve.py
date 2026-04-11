"""
serve.py — Lance fetch_cot.py puis démarre le serveur HTTP local
Ouvre automatiquement le browser sur http://localhost:8082
"""
import subprocess, sys, os, time, webbrowser, threading
from http.server import HTTPServer, SimpleHTTPRequestHandler

PORT = 8090
DIR  = os.path.dirname(os.path.abspath(__file__))

class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args): pass  # silence les logs
    def end_headers(self):
        # CORS headers pour autoriser les fetch locaux
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

def open_browser():
    time.sleep(0.8)
    webbrowser.open(f'http://localhost:{PORT}')

if __name__ == '__main__':
    os.chdir(DIR)

    # 1. Générer les données si elles n'existent pas ou si --refresh passé
    cot_path = os.path.join(DIR, 'data', 'cot_data.json')
    etf_path = os.path.join(DIR, 'data', 'etf_flows.json')
    refresh  = '--refresh' in sys.argv or not os.path.exists(cot_path)

    if refresh:
        print('📡 Récupération données CFTC COT...')
        result = subprocess.run([sys.executable, 'fetch_cot.py'], capture_output=False)
        if result.returncode != 0:
            print('⚠️  fetch_cot.py a échoué — le site fonctionnera sans données COT')
    else:
        print(f'✓ Données COT existantes ({cot_path})')

    if '--refresh' in sys.argv or not os.path.exists(etf_path):
        print('📡 Récupération flux ETF Bitcoin (Farside)...')
        result2 = subprocess.run([sys.executable, 'fetch_etf_flows.py'], capture_output=False)
        if result2.returncode != 0:
            print('⚠️  fetch_etf_flows.py a échoué — le site fonctionnera sans données ETF')
    else:
        print(f'✓ Données ETF existantes ({etf_path})')

    # 2. Démarrer le serveur
    server = HTTPServer(('', PORT), QuietHandler)
    print(f'\n🌊 Serveur démarré → http://localhost:{PORT}')
    print('   Ctrl+C pour arrêter\n')

    # 3. Ouvrir le browser
    threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n✓ Serveur arrêté')
