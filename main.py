#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
import threading
from datetime import datetime, timedelta

import requests
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify

# ---------------------------------
# CONFIG
# ---------------------------------
SPORTRADAR_API_KEY = "xnCeW896IpZvYU3i8bSziTU9i4AthfjDn3Oa18Ie"
TG_TOKEN = "8252990863:AAEAN1qEh8xCwKT6-61rA1lp8nSHrHSFQLc"
TG_CHAT  = "1206397833"

PG_HOST = "dpg-d4b25nggjchc73f7d1o0-a"
PG_PORT = 5432
PG_DB   = "surebet_db"
PG_USER = "surebet_db_user"
PG_PASS = "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU"

INSERT_HOUR = 15
POLL_SECONDS = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("surebet_sportradar.log"),
        logging.StreamHandler()
    ]
)

# ---------------------------------
# DB
# ---------------------------------
def db():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS
    )

def db_exec(query, params=(), fetch=False):
    conn = db()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
    finally:
        conn.close()

# ---------------------------------
# TELEGRAM
# ---------------------------------
def send_telegram(message: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT, "text": message}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning(f"Telegram error: {r.text}")
    except Exception as e:
        logging.error(f"Telegram exception: {e}")

# ---------------------------------
# PREMATCH: Consumo Sportradar con headers
# ---------------------------------
def safe_request(url, retries=3, delay=5):
    headers = {
        "Accept": "application/json",
        "x-api-key": SPORTRADAR_API_KEY
    }
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=25)
            if r.status_code >= 400:
                logging.warning(f"HTTP {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.warning(f"Error en request: {e}, intento {i+1}/{retries}")
            time.sleep(delay)
    return {}

def valid_odds(odds):
    try:
        return odds is not None and float(odds) > 1.01
    except Exception:
        return False

def fetch_prematch_over25():
    # Competencias de soccer en Odds Comparison Prematch v2 (trial)
    url = "https://api.sportradar.com/oddscomparison-prematch/trial/v2/en/sports/sr:sport:1/competitions.json"
    data = safe_request(url)

    results = []
    for sport in data.get("sports", []):
        if (sport.get("name") or "").lower() != "soccer":
            continue

        for tournament in sport.get("tournaments", []):
            for ev in tournament.get("sport_events", []):
                event_id = ev.get("id")
                competitors = ev.get("competitors", [])
                if len(competitors) < 2:
                    continue
                home = competitors[0].get("name")
                away = competitors[1].get("name")
                commence_time = ev.get("scheduled")

                best_odds = None
                best_bookmaker = None
                for market in ev.get("markets", []):
                    mname = (market.get("name") or "").lower()
                    # Ampliamos nombres posibles del mercado de totales
                    if mname in {"total", "totals", "over/under", "goals over/under"}:
                        for outcome in market.get("outcomes", []):
                            name = (outcome.get("name") or "").lower()
                            total = outcome.get("total")
                            odds = outcome.get("odds")
                            bookmaker = outcome.get("bookmaker")
                            if name.startswith("over") and total == 2.5 and valid_odds(odds):
                                oddsv = float(odds)
                                if best_odds is None or oddsv > best_odds:
                                    best_odds = oddsv
                                    best_bookmaker = bookmaker

                if best_odds:
                    results.append({
                        "event_id": event_id,
                        "home_team": home,
                        "away_team": away,
                        "commence_time": commence_time,
                        "bookmaker": best_bookmaker or "Sportradar",
                        "odds": best_odds
                    })
    return results

def insert_matches(rows):
    ids = []
    for row in rows:
        q = """
        INSERT INTO matches (event_id, home_team, away_team, commence_time, bookmaker, market, selection, odds, created_at, updated_at, bet_placed)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),FALSE)
        ON CONFLICT (event_id, bookmaker, market, selection)
        DO UPDATE SET odds = EXCLUDED.odds, updated_at = NOW()
        RETURNING id
        """
        raw_dt = row.get("commence_time")
        try:
            if isinstance(raw_dt, str):
                commence_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
            else:
                commence_dt = datetime.utcnow()
        except Exception:
            commence_dt = datetime.utcnow()

        vals = (
            row["event_id"],
            row["home_team"],
            row["away_team"],
            commence_dt,
            row["bookmaker"],
            "over_under",
            "over_2.5",
            row["odds"]
        )
        try:
            res = db_exec(q, vals, fetch=True)
            if res:
                ids.append(res[0]["id"])
        except Exception as e:
            logging.error(f"DB insert error (event_id={row.get('event_id')}): {e}")
    return ids

# ---------------------------------
# CICLO PRINCIPAL
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now()
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        send_telegram("Heartbeat: script activo y monitoreando (solo pre-match).")
        _last_heartbeat = now

def run_cycle(tag):
    rows = fetch_prematch_over25()
    ids = insert_matches(rows)
    logging.info(f"[{tag}] Insertados/actualizados {len(ids)} partidos pre-match (Over 2.5).")
    # Log amigable para ver qué entra
    for row in rows[:50]:
        logging.info(
            f"{tag} | {row['home_team']} vs {row['away_team']} | "
            f"BM: {row['bookmaker']} | Odds: {row['odds']} | EventID: {row['event_id']}"
        )
    send_telegram(f"[{tag}] Insertados/actualizados {len(ids)} partidos pre-match en DB.")

def main():
    logging.info("Script iniciado correctamente (solo pre-match).")
    last_insert_date = None
    inserted_today = False

    # Inserción inmediata al arrancar
    try:
        run_cycle("ARRANQUE")
        last_insert_date = datetime.now().date()
        inserted_today = True
    except Exception as e:
        logging.error(f"Error en inserción inicial: {e}")

    # Ciclo infinito
    while True:
        now = datetime.now()
        try:
            # Reset bandera al cambiar de día
            if last_insert_date != now.date():
                inserted_today = False

            # Inserción diaria a la hora fija
            if (last_insert_date != now.date()) and now.hour == INSERT_HOUR and not inserted_today:
                run_cycle("DIARIO")
                last_insert_date = now.date()
                inserted_today = True

        except Exception as e:
            logging.error(f"Error en inserción diaria: {e}")

        try:
            heartbeat()
        except Exception as e:
            logging.error(f"Error en heartbeat: {e}")

        time.sleep(POLL_SECONDS)

# ---------------------------------
# FLASK (Render Web Service)
# ---------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Surebet bot (solo pre-match) activo. Endpoints: /health, /competitions, /prematch"

@app.route("/health")
def health():
    return "OK", 200

@app.route("/competitions")
def competitions():
    url = "https://api.sportradar.com/oddscomparison-prematch/trial/v2/en/sports/sr:sport:1/competitions.json"
    headers = {
        "Accept": "application/json",
        "x-api-key": SPORTRADAR_API_KEY
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        return jsonify({
            "status_code": r.status_code,
            "ok": r.ok,
            "response": (r.json() if r.ok else r.text[:500])
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/prematch")
def prematch():
    try:
        rows = fetch_prematch_over25()
        return jsonify({
            "count": len(rows),
            "items": rows[:100]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def start_app():
    # Hilo para el ciclo principal
    t = threading.Thread(target=main, daemon=True)
    t.start()
    # Flask web service
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    start_app()
