import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# Tu API key la guardas como variable de entorno en Render
API_KEY = "xnCeW896IpZvYU3i8bSziTU9i4AthfjDn3Oa18Ie"

@app.route("/competitions")
def get_competitions():
    url = "https://api.sportradar.com/oddscomparison-prematch/trial/v2/en/sports/sr:sport:1/competitions.json"
    headers = {
        "Accept": "application/json",
        "x-api-key": API_KEY
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        # Devuelvo status y contenido para que veas si terminó bien o mal
        return jsonify({
            "status_code": resp.status_code,
            "ok": resp.ok,
            "response": resp.json() if resp.ok else resp.text
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    # Render detecta el puerto desde la variable PORT
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
