#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify
import threading
import os

# ---------------------------------
# CONFIG (variables en duro)
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
REQUEST_DELAY = 1.2

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
            logging.warning(f"Error Telegram: {r.text}")
    except Exception as e:
        logging.error(f"Excepción Telegram: {e}")

# ---------------------------------
# REQUESTS
# ---------------------------------
def safe_request(url, retries=3, delay=REQUEST_DELAY):
    headers = {"Accept": "application/json", "x-api-key": SPORTRADAR_API_KEY}
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=25)
            if r.status_code == 429:
                logging.warning(f"HTTP 429: {r.text[:300]}")
                time.sleep(max(delay * 2, 2.0))
                continue
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

# ---------------------------------
# PREMATCH: odds por evento (Over 2.5) usando markets -> books -> outcomes
# ---------------------------------
def fetch_event_odds_over25(event_id, locale="es"):
    url = f"https://api.sportradar.com/oddscomparison-prematch/trial/v2/{locale}/sport_events/{event_id}/sport_event_markets.json"
    data = safe_request(url)

    mejor_cuota = None
    mejor_casa = None

    for mercado in data.get("markets", []):
        nombre = (mercado.get("name") or "").lower()
        if nombre in {"total", "totales", "over/under", "goles más/menos"}:
            for book in mercado.get("books", []):
                casa = book.get("name")
                for outcome in book.get("outcomes", []):
                    tipo = (outcome.get("type") or "").lower()
                    total = outcome.get("total")
                    cuota = outcome.get("odds_decimal")

                    # Normalizar total a float
                    try:
                        total_val = float(total) if total else None
                    except Exception:
                        total_val = None

                    if tipo == "over" and total_val == 2.5 and valid_odds(cuota):
                        cuota_val = float(cuota)
                        if mejor_cuota is None or cuota_val > mejor_cuota:
                            mejor_cuota = cuota_val
                            mejor_casa = casa

    if mejor_cuota:
        return {
            "evento": event_id,
            "casa": mejor_casa or "Sportradar",
            "cuota_over25": mejor_cuota
        }
    else:
        return {
            "evento": event_id,
            "mensaje": "No se encontraron cuotas para Over 2.5"
        }

# ---------------------------------
# PREMATCH: schedules (hoy y mañana) + odds por evento
# ---------------------------------
def fetch_prematch_over25():
    hoy = datetime.utcnow().date()
    manana = hoy + timedelta(days=1)
    fechas = [hoy.isoformat(), manana.isoformat()]

    resultados = []
    for fecha in fechas:
        # respuesta en español (es) y estructura con "schedules" -> "sport_event"
        sched_url = f"https://api.sportradar.com/oddscomparison-prematch/trial/v2/es/sports/sr:sport:1/schedules/{fecha}/schedules.json?limit=50&start=0"
        data = safe_request(sched_url)

        for item in data.get("schedules", []):
            ev = item.get("sport_event", {})
            event_id = ev.get("id")
            competitors = ev.get("competitors", [])
            if len(competitors) < 2:
                continue

            local = competitors[0].get("name")
            visitante = competitors[1].get("name")

            # campo de fecha/hora en schedules español: "start_time"
            fecha_hora = (
                ev.get("start_time") or
                ev.get("fecha_inicio") or
                ev.get("scheduled")
            )

            # obtener mejor cuota Over 2.5 en prematch
            cuota_info = fetch_event_odds_over25(event_id, locale="es")

            if "cuota_over25" in cuota_info:
                resultados.append({
                    "evento": event_id,
                    "local": local,
                    "visitante": visitante,
                    "fecha_hora": fecha_hora,
                    "casa": cuota_info["casa"],
                    "cuota_over25": cuota_info["cuota_over25"]
                })
            else:
                # opcional: registrar eventos sin mercado Over 2.5
                logging.info(f"Sin Over 2.5 prematch: {local} vs {visitante} | {event_id}")
    return resultados

# ---------------------------------
# INSERT DB (prematch Over 2.5)
# ---------------------------------
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
        raw_dt = row.get("fecha_hora")
        try:
            if isinstance(raw_dt, str):
                commence_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
            else:
                commence_dt = datetime.utcnow()
        except Exception:
            commence_dt = datetime.utcnow()

        vals = (
            row["evento"],
            row["local"],
            row["visitante"],
            commence_dt,
            row["casa"],
            "over_under",
            "over_2.5",
            row["cuota_over25"]
        )
        try:
            res = db_exec(q, vals, fetch=True)
            if res:
                ids.append(res[0]["id"])
        except Exception as e:
            logging.error(f"DB insert error (event_id={row.get('evento')}): {e}")
    return ids

# ---------------------------------
# CICLO PRINCIPAL
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now()
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        send_telegram("Heartbeat: activo y monitoreando partidos pre-match Over 2.5.")
        _last_heartbeat = now

def run_cycle_prematch(tag):
    rows = fetch_prematch_over25()
    ids = []
    try:
        ids = insert_matches(rows)
    except Exception as e:
        logging.error(f"Error insert prematch: {e}")
    logging.info(f"[{tag}] Prematch Over 2.5 insertados/actualizados: {len(ids)}")
    send_telegram(f"[{tag}] Prematch Over 2.5 en DB: {len(ids)}")

def main():
    logging.info("Script iniciado correctamente (prematch Over 2.5).")
    last_insert_date = None
    inserted_today = False

    # Inserción inmediata
    try:
        run_cycle_prematch("ARRANQUE")
        last_insert_date = datetime.now().date()
        inserted_today = True
    except Exception as e:
        logging.error(f"Error en inserción inicial: {e}")

    while True:
        now = datetime.now()
        try:
            # Inserción diaria a la hora fija
            if (last_insert_date != now.date()) and now.hour == INSERT_HOUR and not inserted_today:
                run_cycle_prematch("DIARIO")
                last_insert_date = now.date()
                inserted_today = True

            # Reset bandera al cambiar de día
            if last_insert_date != now.date():
                inserted_today = False
        except Exception as e:
            logging.error(f"Error en inserción diaria: {e}")

        try:
            heartbeat()
        except Exception as e:
            logging.error(f"Error en heartbeat: {e}")

        time.sleep(POLL_SECONDS)

# ---------------------------------
# FLASK
# ---------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Servicio activo: prematch Over 2.5 (schedules + odds)."

@app.route("/salud")
def salud():
    return "OK", 200

@app.route("/prematch")
def prematch():
    try:
        datos = fetch_prematch_over25()
        return jsonify({"cantidad": len(datos), "partidos": datos})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # hilo para el ciclo principal
    t = threading.Thread(target=main, daemon=True)
    t.start()

    # levantar Flask (Render suele inyectar PORT)
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
