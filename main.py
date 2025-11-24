#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
import psycopg2.extras
from flask import Flask
import threading
import os

# ---------------------------------
# CONFIG
# ---------------------------------
SPORTMONKS_BASE = "https://api.sportmonks.com/v3/football"
SPORTMONKS_TOKEN = "vCglzLkFonsx4mGHVGhvKnLqpOAtYxNRpfcruDQKU88ZfsXBPqES18dgsk2j"

TG_TOKEN = "8252990863:AAEAN1qEh8xCwKT6-61rA1lp8nSHrHSFQLc"
TG_CHAT  = "1206397833"

PG_HOST = "dpg-d4b25nggjchc73f7d1o0-a"
PG_PORT = 5432
PG_DB   = "surebet_db"
PG_USER = "surebet_db_user"
PG_PASS = "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU"

INSERT_HOUR = 15
POLL_SECONDS = 300
BASE_STAKE = 100.0
MAX_STAKE = 500.0
CURRENCY = "USD"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("surebet_sportmonks.log"),
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
# SPORTMONKS REQUEST
# ---------------------------------
def sportmonks_request(endpoint, params=None):
    if params is None:
        params = {}
    params["api_token"] = SPORTMONKS_TOKEN
    url = f"{SPORTMONKS_BASE}{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Error Sportmonks request {url}: {e}")
        return {}

def valid_odds(odds):
    try:
        return odds is not None and float(odds) > 1.01
    except Exception:
        return False

# ---------------------------------
# PREMATCH (usando odds/inplay/fixtures/{fixtureId}/markets/5)
# ---------------------------------
def fetch_prematch_over25():
    hoy = datetime.utcnow().date()
    manana = hoy + timedelta(days=1)
    fechas = [hoy.isoformat(), manana.isoformat()]

    resultados = []
    for fecha in fechas:
        fixtures = sportmonks_request(f"/fixtures/date/{fecha}", params={"include": "participants"})
        for fixture in fixtures.get("data", []):
            fixture_id = fixture.get("id")
            participants = fixture.get("participants", [])
            if len(participants) < 2:
                continue

            local = participants[0].get("name")
            visitante = participants[1].get("name")
            fecha_hora = fixture.get("starting_at")

            # Consultamos el mercado de goles (id=5 → Over/Under)
            odds_data = sportmonks_request(f"/odds/inplay/fixtures/{fixture_id}/markets/5")
            mejor_cuota = None
            mejor_casa = None

            for book in odds_data.get("data", []):
                bookmaker_name = (book.get("bookmaker", {}) or {}).get("name")
                for outcome in book.get("odds", []) or []:
                    label = (outcome.get("label") or "").lower()
                    if label in {"over 2.5", "over2.5", "over_2.5"}:
                        try:
                            cuota = float(outcome.get("value"))
                        except Exception:
                            continue
                        if mejor_cuota is None or cuota > mejor_cuota:
                            mejor_cuota = cuota
                            mejor_casa = bookmaker_name

            if mejor_cuota and valid_odds(mejor_cuota):
                resultados.append({
                    "evento": fixture_id,
                    "local": local,
                    "visitante": visitante,
                    "fecha_hora": fecha_hora,
                    "casa": mejor_casa,
                    "cuota_over25": mejor_cuota,
                    "stake": BASE_STAKE
                })
    return resultados

# ---------------------------------
# INSERT DB (igual que antes)
# ---------------------------------
def insert_matches(rows):
    ids = []
    for row in rows:
        q = """
        INSERT INTO matches (event_id, home_team, away_team, commence_time, bookmaker, market, selection, odds, stake, created_at, updated_at, bet_placed)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),TRUE)
        ON CONFLICT (event_id, bookmaker, market, selection)
        DO UPDATE SET odds = EXCLUDED.odds, stake = EXCLUDED.stake, updated_at = NOW()
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
            row["cuota_over25"],
            row.get("stake", BASE_STAKE),
        )
        try:
            res = db_exec(q, vals, fetch=True)
            if res:
                ids.append(res[0]["id"])
        except Exception as e:
            logging.error(f"DB insert error (event_id={row.get('evento')}): {e}")
    return ids

# ---------------------------------
# LIVE
# ---------------------------------
def fetch_live_under25():
    # Incluimos participants y scores si tu plan lo soporta
    data = sportmonks_request("/livescores/inplay", params={"include": "participants,scores,time"})
    eventos = []
    for fixture in data.get("data", []):
        fixture_id = fixture.get("id")
        participants = fixture.get("participants", [])
        if len(participants) < 2:
            continue

        home = participants[0].get("name")
        away = participants[1].get("name")
        minute = ((fixture.get("time", {}) or {}).get("minute")) or 0
        scores = fixture.get("scores", {}) or {}
        score_home = scores.get("home_score") or 0
        score_away = scores.get("away_score") or 0

        # Consultamos directamente el mercado Over/Under (id=5)
        odds_data = sportmonks_request(f"/odds/inplay/fixtures/{fixture_id}/markets/5")
        under_odds = None
        bookmaker = None

        for market in odds_data.get("data", []) or []:
            bookmaker_name = (market.get("bookmaker", {}) or {}).get("name")
            for outcome in market.get("odds", []) or []:
                label = (outcome.get("label") or "").lower()
                if label in {"under 2.5", "under2.5", "under_2.5"}:
                    try:
                        cuota = float(outcome.get("value"))
                    except Exception:
                        continue
                    if under_odds is None or cuota > under_odds:
                        under_odds = cuota
                        bookmaker = bookmaker_name

        if under_odds and valid_odds(under_odds):
            eventos.append({
                "event_id": fixture_id,
                "home_team": home,
                "away_team": away,
                "minute": int(minute) if isinstance(minute, (int, float, str)) else 0,
                "score_home": int(score_home) if isinstance(score_home, (int, float, str)) else 0,
                "score_away": int(score_away) if isinstance(score_away, (int, float, str)) else 0,
                "odds": under_odds,
                "bookmaker": bookmaker
            })
    return eventos

def monitor_live_and_notify():
    events = fetch_live_under25()
    for ev in events:
        fixture_id = ev["event_id"]
        home = ev["home_team"]
        away = ev["away_team"]
        minute = ev["minute"]
        score_home = ev["score_home"]
        score_away = ev["score_away"]
        total_goals = score_home + score_away

        # Recuperar Over 2.5 pre-match ya confirmado
        q = "SELECT id, odds, stake FROM matches WHERE event_id=%s AND bet_placed=TRUE AND selection='over_2.5' LIMIT 1"
        res = db_exec(q, (fixture_id,), fetch=True)
        if not res:
            continue
        match_id_db = res[0]["id"]
        over_odds_prematch = float(res[0]["odds"])
        stake_over_prematch = float(res[0]["stake"] or BASE_STAKE)

        # Gol temprano → cashout sugerido
        if minute <= 20 and total_goals >= 1:
            msg = f"Gol temprano en {home} vs {away} (min {minute}, {score_home}-{score_away}). CASHOUT sugerido."
            send_telegram(msg)
            log_alert(match_id_db, "cashout", msg, None, None)
            continue

        under_live = float(ev["odds"])
        bookmaker_live = ev["bookmaker"]

        # Evaluar surebet vs cobertura (sin modificar tu lógica base)
        if minute >= 20 and total_goals == 0 and valid_odds(over_odds_prematch) and valid_odds(under_live):
            implied_sum, s_over_base, s_under_base, profit_abs_base, profit_pct_base = compute_surebet_stakes(
                over_odds_prematch, under_live, BASE_STAKE
            )

            if implied_sum < 1.0:
                min_profit = min_profit_by_bookmaker(bookmaker_live or "")
                if profit_pct_base >= min_profit:
                    scale = max(1.0, (profit_pct_base / min_profit) ** 0.5)
                    dynamic_stake = min(MAX_STAKE, BASE_STAKE * scale)

                    _, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
                        over_odds_prematch, under_live, dynamic_stake
                    )

                    msg = (
                        f"🔥 Surebet {home} vs {away} (min {minute}, {score_home}-{score_away}).\n"
                        f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live}).\n"
                        f"Stake: {dynamic_stake:.2f} {CURRENCY} ⇒ Over: {s_over:.2f}, Under: {s_under:.2f}.\n"
                        f"Profit esperado: {profit_abs:.2f} {CURRENCY} ({profit_pct*100:.2f}%)."
                    )
                    send_telegram(msg)
                    log_alert(match_id_db, "surebet", msg, profit_pct, profit_abs)

                    # Guardar resumen de surebet
                    try:
                        db_exec("""
                            INSERT INTO surebets (event_id, home_team, away_team, odds_over, odds_under, stake_total, profit_abs, profit_pct, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        """, (fixture_id, home, away, over_odds_prematch, under_live, dynamic_stake, profit_abs, profit_pct))
                    except Exception as e:
                        logging.error(f"Error insert surebet: {e}")
                else:
                    msg = f"Surebet ignorado {home} vs {away}: profit {profit_pct_base*100:.2f}% < mínimo {min_profit*100:.2f}%."
                    send_telegram(msg)
                    log_alert(match_id_db, "surebet_ignorado", msg, profit_pct_base, None)
            else:
                # Cobertura basada en la apuesta real prematch
                hedge_amount = compute_hedge_amount(stake_over_prematch, over_odds_prematch, under_live)
                msg = (
                    f"⚠️ Sin surebet {home} vs {away} (min {minute}). Cobertura sugerida: apostar {hedge_amount:.2f} {CURRENCY} "
                    f"al UNDER 2.5 @ {under_live} en {bookmaker_live}."
                )
                send_telegram(msg)
                log_alert(match_id_db, "cobertura", msg, None, None)

                # Registrar cobertura (no colocada)
                try:
                    db_exec("""
                        INSERT INTO matches (event_id, home_team, away_team, commence_time, bookmaker, market, selection, odds, stake, created_at, updated_at, bet_placed)
                        VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s,NOW(),NOW(),FALSE)
                    """, (fixture_id, home, away, bookmaker_live, "over_under", "under_2.5", under_live, hedge_amount))
                except Exception as e:
                    logging.error(f"Error insert cobertura: {e}")

# ---------------------------------
# CICLO PRINCIPAL
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now()
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        send_telegram("Heartbeat: activo (Sportmonks v3) prematch + live.")
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
    logging.info("Script iniciado correctamente (Sportmonks v3, soccer).")
    last_insert_date = None

    # Inserción inicial
    try:
        run_cycle_prematch("ARRANQUE")
        last_insert_date = datetime.now().date()
    except Exception as e:
        logging.error(f"Error en inserción inicial: {e}")

    while True:
        now = datetime.now()
        try:
            if (last_insert_date is None or last_insert_date != now.date()) and now.hour == INSERT_HOUR:
                run_cycle_prematch("DIARIO")
                last_insert_date = now.date()
        except Exception as e:
            logging.error(f"Error en inserción diaria: {e}")

        try:
            #monitor_live_and_notify()
            heartbeat()
        except Exception as e:
            logging.error(f"Error en monitoreo: {e}")

        time.sleep(POLL_SECONDS)

# ---------------------------------
# FLASK (Render Web Service)
# ---------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Surebet bot (Sportmonks v3) is running."

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    t = threading.Thread(target=main, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
