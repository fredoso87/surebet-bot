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
import pytz
from urllib.parse import urlparse, parse_qs
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

LIMA_TZ = pytz.timezone("America/Lima")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("surebet_sportmonks.log"),
        logging.StreamHandler()
    ]
)

# ---------------------------------
# UTILIDAD: imprimir todas las casas de apuesta
# ---------------------------------
# ---------------------------------
# UTILIDAD: imprimir todas las casas de apuesta
# ---------------------------------
def print_bookmakers():
    bookmakers = load_bookmakers_map()
    logging.info("📋 Casas de apuesta disponibles (Sportmonks API):")
    for bk_id, bk_name in sorted(bookmakers.items()):
        logging.info(f"ID={bk_id} → {bk_name}")

# ---------------------------------
# BOOKMAKERS CONFIG
# ---------------------------------
def load_bookmakers_map():
    all_bookmakers = []
    page = 1
    per_page = 25  # puedes subirlo si el API lo permite

    while True:
        url = "https://api.sportmonks.com/v3/odds/bookmakers"
        params = {"api_token": SPORTMONKS_TOKEN, "page": page, "per_page": per_page}

        # Construir la URL completa para log
        full_url = f"{url}?api_token={SPORTMONKS_TOKEN}&page={page}&per_page={per_page}"
        logging.info(f"🌐 Consumiento API bookmakers → {full_url}")

        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            logging.error(f"❌ Error obteniendo bookmakers (page={page}): {e}")
            break

        data = payload.get("data", [])
        pagination = payload.get("meta", {}).get("pagination", {})

        logging.info(f"📄 Respuesta API page={page}: count={pagination.get('count')} "
                     f"per_page={pagination.get('per_page')} current_page={pagination.get('current_page')} "
                     f"has_more={pagination.get('has_more')} next_page={pagination.get('next_page')}")

        all_bookmakers.extend(data)
        logging.info(f"✅ Bookmakers acumulados tras page={page}: {len(all_bookmakers)}")

        # Condición de corte
        if not pagination.get("has_more"):
            break

        # Avanza a la siguiente página usando next_page si existe
        next_page_url = pagination.get("next_page")
        if next_page_url:
            qs = parse_qs(urlparse(next_page_url).query)
            page = int(qs.get("page", [page + 1])[0])
        else:
            page += 1

    bookmaker_map = {
        bk.get("id"): bk.get("name")
        for bk in all_bookmakers
        if bk.get("id") is not None and bk.get("name") is not None
    }

    logging.info(f"🎯 Bookmakers cargados: {len(bookmaker_map)} casas de apuesta (total crudo: {len(all_bookmakers)})")
    return bookmaker_map

   
BOOKMAKER_MAP = load_bookmakers_map()
BOOKMAKER_IDS = [1, 2, 9, 5, 20, 21, 24, 16, 26, 28, 22, 33, 35, 39]

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
# UTILIDADES SUREBET
# ---------------------------------
def compute_surebet_stakes(odds_over, odds_under, stake_total):
    try:
        inv_over = 1.0 / float(odds_over)
        inv_under = 1.0 / float(odds_under)
        implied_sum = inv_over + inv_under
        if implied_sum <= 0:
            return 999.0, 0.0, 0.0, 0.0, 0.0
        stake_over = stake_total * (inv_over / implied_sum)
        stake_under = stake_total * (inv_under / implied_sum)
        payout_over = stake_over * float(odds_over)
        payout_under = stake_under * float(odds_under)
        profit_abs = min(payout_over, payout_under) - stake_total
        profit_pct = profit_abs / stake_total if stake_total > 0 else 0.0
        return implied_sum, stake_over, stake_under, profit_abs, profit_pct
    except Exception as e:
        logging.error(f"Error compute_surebet_stakes: {e}")
        return 999.0, 0.0, 0.0, 0.0, 0.0

# ---------------------------------
# PREMATCH: mejores Over/Under 2.5 (marketId=7) + surebet prematch
# ---------------------------------
def fetch_prematch_over25():
    hoy = datetime.now(LIMA_TZ).date()
    manana = hoy + timedelta(days=1)
    base_url = f"{SPORTMONKS_BASE}/fixtures/between/{hoy.isoformat()}/{manana.isoformat()}"
    per_page = 50
    page = 1
    all_fixtures = []

    while True:
        try:
            url = f"{base_url}?api_token={SPORTMONKS_TOKEN}&per_page={per_page}&page={page}&include=participants"
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logging.error(f"Error obteniendo fixtures (page={page}): {e}")
            break

        all_fixtures.extend(data.get("data", []))
        pagination = data.get("meta", {}).get("pagination", {})
        if not pagination.get("has_more"):
            break
        page += 1

    resultados = []
    for fixture in all_fixtures:
        fixture_id = fixture.get("id")
        participants = fixture.get("participants", [])
        if len(participants) < 2:
            continue

        local = participants[0].get("name")
        visitante = participants[1].get("name")
        fecha_hora_raw = fixture.get("starting_at")

        try:
            dt = datetime.fromisoformat(fecha_hora_raw.replace("Z", "+00:00"))
            dt_lima = dt.astimezone(LIMA_TZ)
            # ➕ sumar 5 horas
            dt_lima_plus5 = dt_lima + timedelta(hours=5)
            fecha_hora_str = dt_lima_plus5.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            fecha_hora_str = datetime.now(LIMA_TZ).strftime("%d/%m/%Y %H:%M:%S")

        odds_data = sportmonks_request(f"/odds/pre-match/fixtures/{fixture_id}/markets/7")

        mejor_over = None
        casa_over = None
        mejor_under = None
        casa_under = None

        for outcome in odds_data.get("data", []):
            bookmaker_id = outcome.get("bookmaker_id")
            label = (outcome.get("label") or "").lower()
            total_line = outcome.get("total")
            cuota = outcome.get("value")

            if BOOKMAKER_IDS and bookmaker_id not in BOOKMAKER_IDS:
                # Aquí pintas el nombre usando el mapa
                logging.info(f"❌ Casa descartada: ID={bookmaker_id}, Nombre={BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id))}")
                continue

            try:
                cuota = float(cuota)
            except Exception:
                continue

            if label == "over" and total_line in {"2.5"}:
                if mejor_over is None or cuota > mejor_over:
                    mejor_over = cuota
                    casa_over = BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id))

            if label == "under" and total_line in {"2.5"}:
                if mejor_under is None or cuota > mejor_under:
                    mejor_under = cuota
                    casa_under = BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id))

        resultados.append({
            "evento": fixture_id,
            "local": local,
            "visitante": visitante,
            "fecha_hora": fecha_hora_str,   # dd/mm/yyyy HH24:MI:SS (Lima +5h)
            "cuota_over": mejor_over,
            "casa_over": casa_over,
            "cuota_under": mejor_under,
            "casa_under": casa_under
        })

    return resultados

# ---------------------------------
# INSERT DB: guarda mejores over/under, casas, surebet y stakes con BASE_STAKE
# ---------------------------------
def insert_matches(rows):
    ids = []
    for row in rows:
        cuota_over = row.get("cuota_over")
        cuota_under = row.get("cuota_under")

        surebet_flag = False
        stake_over = None
        stake_under = None
        profit_abs = None
        profit_pct = None

        if cuota_over and cuota_under and valid_odds(cuota_over) and valid_odds(cuota_under):
            implied_sum, s_over, s_under, p_abs, p_pct = compute_surebet_stakes(cuota_over, cuota_under, BASE_STAKE)
            if implied_sum < 1.0:
                surebet_flag = True
            stake_over = s_over
            stake_under = s_under
            profit_abs = p_abs
            profit_pct = p_pct

        q = """
        INSERT INTO matches (
            event_id, home_team, away_team, commence_time,
            odds_over, bookmaker_over,
            odds_under, bookmaker_under,
            surebet, stake_over, stake_under, profit_abs, profit_pct,
            market, selection, created_at, updated_at, bet_placed, track_live
        )
        VALUES (
            %s, %s, %s,
            to_timestamp(%s, 'DD/MM/YYYY HH24:MI:SS') AT TIME ZONE 'America/Lima',
            %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, NOW(), NOW(), TRUE, FALSE
        )
        ON CONFLICT (event_id, market, selection)
        DO UPDATE SET
            odds_over = EXCLUDED.odds_over,
            bookmaker_over = EXCLUDED.bookmaker_over,
            odds_under = EXCLUDED.odds_under,
            bookmaker_under = EXCLUDED.bookmaker_under,
            surebet = EXCLUDED.surebet,
            stake_over = EXCLUDED.stake_over,
            stake_under = EXCLUDED.stake_under,
            profit_abs = EXCLUDED.profit_abs,
            profit_pct = EXCLUDED.profit_pct,
            updated_at = NOW()
        RETURNING id
        """

        vals = (
            row["evento"],
            row["local"],
            row["visitante"],
            row["fecha_hora"],   # string dd/mm/yyyy HH24:MI:SS (Lima +5h)
            row.get("cuota_over"),
            row.get("casa_over"),
            row.get("cuota_under"),
            row.get("casa_under"),
            surebet_flag,
            stake_over,
            stake_under,
            profit_abs,
            profit_pct,
            "over_under",
            "over_2.5"
        )

        try:
            res = db_exec(q, vals, fetch=True)
            if res:
                ids.append(res[0]["id"])
        except Exception as e:
            logging.error(f"DB insert error (event_id={row.get('evento')}): {e}")
    return ids

# ---------------------------------
# LIVE: inplay odds marketId=4 (Match Goals, línea 2.5) por fixture_id
# ---------------------------------
def fetch_live_under25(fixtures_ids):
    eventos = []
    for fixture_id in fixtures_ids:
        odds_data = sportmonks_request(f"/odds/inplay/fixtures/{fixture_id}/markets/4")

        for outcome in odds_data.get("data", []):
            bookmaker_id = outcome.get("bookmaker_id")
            label = (outcome.get("label") or "").lower()
            total_line = outcome.get("total")
            cuota = outcome.get("value")

            if BOOKMAKER_IDS and bookmaker_id not in BOOKMAKER_IDS:
                continue

            if label == "under" and str(total_line) == "2.5":
                try:
                    cuota = float(cuota)
                except Exception:
                    continue

                if valid_odds(cuota):
                    eventos.append({
                        "event_id": fixture_id,
                        "bookmaker_name": BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id)),
                        "linea_total": "2.5",
                        "cuota_under25": cuota
                    })
    return eventos

# ---------------------------------
# MONITOREO LIVE + NOTIFY (usa track_live=TRUE en matches)
# ---------------------------------
def monitor_live_and_notify():
    rows = db_exec("""
        SELECT id, event_id, home_team, away_team, odds_over, odds_under, stake_over, stake_under
        FROM matches
        WHERE track_live=TRUE
          AND market='over_under'
          AND selection='over_2.5'
          AND bet_placed=TRUE
    """, fetch=True)

    if not rows:
        logging.info("No hay partidos con track_live=TRUE para monitorear.")
        return

    fixture_ids = [r["event_id"] for r in rows]
    prematch_index = {r["event_id"]: r for r in rows}

    live_events = fetch_live_under25(fixture_ids)
    if not live_events:
        logging.info("No se obtuvieron odds UNDER 2.5 en vivo para los fixtures marcados.")
        return

    for ev in live_events:
        fixture_id = ev["event_id"]
        under_live = float(ev["cuota_under25"])
        bookmaker_live_name = ev["bookmaker_name"]

        pm = prematch_index.get(fixture_id)
        if not pm:
            continue

        match_id_db = pm["id"]
        home = pm.get("home_team") or ""
        away = pm.get("away_team") or ""
        over_odds_prematch = float(pm.get("odds_over") or 0)

        implied_sum, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
            over_odds_prematch, under_live, BASE_STAKE
        )

        if implied_sum < 1.0:
            msg = (
                f"🔥 Surebet LIVE {home} vs {away}.\n"
                f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live_name}).\n"
                f"Stake base {BASE_STAKE:.2f} {CURRENCY} ⇒ Over: {s_over:.2f}, Under: {s_under:.2f}.\n"
                f"Profit esperado: {profit_abs:.2f} {CURRENCY} ({profit_pct*100:.2f}%)."
            )
            send_telegram(msg)
            try:
                db_exec("""
                    INSERT INTO alerts (match_id, kind, message, profit_pct, profit_abs, created_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                """, (match_id_db, "surebet_live", msg, profit_pct, profit_abs))
            except Exception as e:
                logging.error(f"Error insert alert surebet_live: {e}")
        else:
            msg = (
                f"ℹ️ Sin surebet LIVE {home} vs {away}. Under 2.5 @ {under_live} ({bookmaker_live_name}). "
                f"Suma inversas: {implied_sum:.4f}."
            )
            send_telegram(msg)

# ---------------------------------
# CICLO PRINCIPAL
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now(LIMA_TZ)
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        send_telegram("Heartbeat: activo (prematch mkt7 + live mkt4).")
        _last_heartbeat = now

def run_cycle_prematch(tag):
    rows = fetch_prematch_over25()
    ids = []
    try:
        ids = insert_matches(rows)
    except Exception as e:
        logging.error(f"Error insert prematch: {e}")
    logging.info(f"[{tag}] Prematch Over/Under 2.5 procesados: {len(ids)}")
    send_telegram(f"[{tag}] Prematch Over/Under 2.5 en DB: {len(ids)}")

def main():
    logging.info("Script iniciado (Sportmonks v3 football).")
    last_insert_date = None

    try:
        print_bookmakers()
        run_cycle_prematch("ARRANQUE")
        last_insert_date = datetime.now(LIMA_TZ).date()
    except Exception as e:
        logging.error(f"Error en inserción inicial: {e}")

    while True:
        now = datetime.now(LIMA_TZ)
        try:
            if (last_insert_date is None or last_insert_date != now.date()) and now.hour == INSERT_HOUR:
                run_cycle_prematch("DIARIO")
                last_insert_date = now.date()
        except Exception as e:
            logging.error(f"Error en inserción diaria: {e}")

        try:
            monitor_live_and_notify()
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
