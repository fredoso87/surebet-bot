import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# La API key la tomas de variable de entorno en Render (más seguro que hardcodear)
API_KEY = os.getenv("SPORTRADAR_API_KEY", "xnCeW896IpZvYU3i8bSziTU9i4AthfjDn3Oa18Ie")

@app.route("/")
def index():
    return "Servicio activo. Endpoints: /health y /competitions"

@app.route("/health")
def health():
    return "OK", 200

@app.route("/competitions")
def get_competitions():
    url = "https://api.sportradar.com/oddscomparison-prematch/trial/v2/en/sports/sr:sport:1/competitions.json"
    headers = {
        "Accept": "application/json",
        "x-api-key": API_KEY
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        return jsonify({
            "status_code": resp.status_code,
            "ok": resp.ok,
            # si la respuesta es JSON válido lo pintamos, si no mostramos texto
            "response": resp.json() if resp.ok else resp.text[:500]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Render detecta el puerto desde la variable PORT
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
