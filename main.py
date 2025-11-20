import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# Tu API key la guardas como variable de entorno en Render
API_KEY = os.getenv("2a5684033edc1582d1e7befd417fda79")

@app.route("/competitions")
def get_competitions():
    url = "https://api.sportradar.com/oddscomparison-prematch/trial/v2/en/sports/sr:sport:1/competitions.json"
    headers = {
        "Accept": "application/json",
        "x-api-key": API_KEY
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    # Render detecta el puerto desde la variable PORT
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
