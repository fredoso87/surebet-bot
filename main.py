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
# BOOKMAKERS CONFIG
# ---------------------------------
def load_bookmakers_map():
    """
    Devuelve un diccionario {id: nombre} con todas las casas de apuestas desde el catálogo de Odds.
    """
    all_bookmakers = []
    page = 1
    while True:
        url = "https://api.sportmonks.com/v3/odds/bookmakers"
        params = {"api_token": SPORTMONKS_TOKEN, "page": page}
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logging.error(f"Error obteniendo bookmakers (page={page}): {e}")
            break

        all_bookmakers.extend(data.get("data", []))
        pagination = data.get("meta", {}).get("pagination", {})
        if not pagination.get("has_more"):
            break
        page += 1

    # Mapa id -> nombre
    return {bk.get("id"): bk.get("name") for bk in all_bookmakers if bk.get("id") is not None}

BOOKMAKER_MAP = load_bookmakers_map()

# Filtro opcional por IDs. Vacío = no filtrar.
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
# UTILIDADES SUREBET / COBERTURA
# ---------------------------------
def compute_surebet_stakes(odds_over, odds_under, stake_total):
    """
    Retorna: (implied_sum, stake_over, stake_under, profit_abs, profit_pct)
    """
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

def compute_hedge_amount(stake_over, odds_over, odds_under):
    """
    Monto de cobertura al Under para igualar payoff del Over aproximado.
    """
    try:
        target_payout = float(stake_over) * float(odds_over)
        hedge = target_payout / float(odds_under) - float(stake_over)
        return max(0.0, hedge)
    except Exception as e:
        logging.error(f"Error compute_hedge_amount: {e}")
        return 0.0

def min_profit_by_bookmaker(bookmaker_name: str) -> float:
    """
    Umbral mínimo de profit por casa (personalizable).
    Retorna porcentaje mínimo (ej. 0.01 = 1%).
    """
    if not bookmaker_name:
        return 0.01
    name = bookmaker_name.lower()
    if "pinnacle" in name:
        return 0.006
    if "bet365" in name:
        return 0.008
    return 0.01

def log_alert(match_id, kind, message, profit_pct=None, profit_abs=None):
    try:
        db_exec("""
            INSERT INTO alerts (match_id, kind, message, profit_pct, profit_abs, created_at)
            VALUES (%s,%s,%s,%s,%s,NOW())
        """, (match_id, kind, message, profit_pct, profit_abs))
    except Exception as e:
        logging.error(f"Error insert alert: {e}")

# ---------------------------------
# PREMATCH: odds pre-match marketId=7 (Goal Line / Over/Under)
# ---------------------------------
def fetch_prematch_over25():
    hoy = datetime.utcnow().date()
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
        fecha_hora = fixture.get("starting_at")

        # Pre-match odds con marketId=7
        odds_data = sportmonks_request(f"/odds/pre-match/fixtures/{fixture_id}/markets/7")

        mejor_cuota = None
        mejor_casa = None
        mejor_total = None

        for outcome in odds_data.get("data", []):
            bookmaker_id = outcome.get("bookmaker_id")
            label = (outcome.get("label") or "").lower()
            total_line = outcome.get("total")
            cuota = outcome.get("value")

            # Filtro de casas por IDs (si se configuró)
            if BOOKMAKER_IDS and bookmaker_id not in BOOKMAKER_IDS:
                continue

            # Buscamos Over 2.5 (algunas feeds usan "2/2.5")
            if label == "over" and total_line in {"2.5", "2/2.5"}:
                try:
                    cuota = float(cuota)
                except Exception:
                    continue
                if mejor_cuota is None or cuota > mejor_cuota:
                    mejor_cuota = cuota
                    mejor_casa = BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id))  # nombre, fallback ID
                    mejor_total = total_line

        if mejor_cuota and valid_odds(mejor_cuota):
            resultados.append({
                "evento": fixture_id,
                "local": local,
                "visitante": visitante,
                "fecha_hora": fecha_hora,
                "casa": mejor_casa,            # nombre de la casa
                "linea_total": mejor_total,
                "cuota_over25": mejor_cuota,
                "stake": BASE_STAKE
            })
    return resultados

# ---------------------------------
# INSERT DB (bet_placed=TRUE, track_live=FALSE por defecto)
# ---------------------------------
def insert_matches(rows):
    ids = []
    for row in rows:
        q = """
        INSERT INTO matches (event_id, home_team, away_team, commence_time, bookmaker, market, selection, odds, stake, created_at, updated_at, bet_placed, track_live)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),TRUE,FALSE)
        ON CONFLICT (event_id, bookmaker, market, selection)
        DO UPDATE SET odds = EXCLUDED.odds,
                      stake = EXCLUDED.stake,
                      updated_at = NOW()
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
            row["casa"],                 # nombre de la casa
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
# LIVE: inplay odds marketId=4 (Match Goals, línea 2.5) por fixture_id
# ---------------------------------
def fetch_live_under25(fixtures_ids):
    """
    Consulta odds inplay del mercado 4 (Match Goals) para cada fixture_id
    y extrae UNDER 2.5 con el nombre de la casa.
    """
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

            if label == "under" and total_line == "2.5":
                try:
                    cuota = float(cuota)
                except Exception:
                    continue

                if valid_odds(cuota):
                    eventos.append({
                        "event_id": fixture_id,
                        "bookmaker_name": BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id)),
                        "linea_total": total_line,
                        "cuota_under25": cuota
                    })
    return eventos

# ---------------------------------
# MONITOREO LIVE + NOTIFY (usa track_live=TRUE en matches)
# ---------------------------------
def monitor_live_and_notify():
    """
    - Obtiene fixtures marcados con track_live=TRUE (prematch Over 2.5).
    - Consulta Under 2.5 inplay (marketId=4) por fixture.
    - Evalúa surebet y cobertura; notifica vía Telegram y registra alertas/surebets.
    """
    # 1) Recuperar fixtures a monitorear
    rows = db_exec("""
        SELECT id, event_id, home_team, away_team, odds, stake, bookmaker
        FROM matches
        WHERE track_live=TRUE
          AND selection='over_2.5'
          AND bet_placed=TRUE
    """, fetch=True)

    if not rows:
        logging.info("No hay partidos con track_live=TRUE para monitorear.")
        return

    fixture_ids = [r["event_id"] for r in rows]
    prematch_index = {r["event_id"]: r for r in rows}

    # 2) Consultar Under 2.5 live (mercado 4) para esos fixtures
    live_events = fetch_live_under25(fixture_ids)
    if not live_events:
        logging.info("No se obtuvieron odds UNDER 2.5 en vivo para los fixtures marcados.")
        return

    # 3) Evaluar cada evento live
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
        over_odds_prematch = float(pm["odds"])
        stake_over_prematch = float(pm["stake"] or BASE_STAKE)

        # 3.a) Intento de surebet con stake dinámico
        implied_sum, s_over_base, s_under_base, profit_abs_base, profit_pct_base = compute_surebet_stakes(
            over_odds_prematch, under_live, BASE_STAKE
        )

        if implied_sum < 1.0:
            min_profit = min_profit_by_bookmaker(bookmaker_live_name or "")
            if profit_pct_base >= min_profit:
                scale = max(1.0, (profit_pct_base / min_profit) ** 0.5)
                dynamic_stake = min(MAX_STAKE, BASE_STAKE * scale)

                _, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
                    over_odds_prematch, under_live, dynamic_stake
                )

                msg = (
                    f"🔥 Surebet {home} vs {away}.\n"
                    f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live_name}).\n"
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
                continue
            else:
                msg = f"Surebet ignorado {home} vs {away}: profit {profit_pct_base*100:.2f}% < mínimo {min_profit*100:.2f}%."
                send_telegram(msg)
                log_alert(match_id_db, "surebet_ignorado", msg, profit_pct_base, None)

        # 3.b) Cobertura si no hay surebet suficiente
        hedge_amount = compute_hedge_amount(stake_over_prematch, over_odds_prematch, under_live)
        if hedge_amount > 0:
            msg = (
                f"⚠️ Cobertura sugerida {home} vs {away}: apostar {hedge_amount:.2f} {CURRENCY} "
                f"al UNDER 2.5 @ {under_live} ({bookmaker_live_name})."
            )
            send_telegram(msg)
            log_alert(match_id_db, "cobertura", msg, None, None)

            # Registrar cobertura propuesta (no colocada) como fila informativa
            try:
                db_exec("""
                    INSERT INTO matches (event_id, home_team, away_team, commence_time, bookmaker, market, selection, odds, stake, created_at, updated_at, bet_placed, track_live)
                    VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s,NOW(),NOW(),FALSE,FALSE)
                """, (fixture_id, home, away, bookmaker_live_name, "over_under", "under_2.5", under_live, hedge_amount))
            except Exception as e:
                logging.error(f"Error insert cobertura propuesta: {e}")

# ---------------------------------
# CICLO PRINCIPAL
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now()
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
    logging.info(f"[{tag}] Prematch Over 2.5 insertados/actualizados: {len(ids)}")
    send_telegram(f"[{tag}] Prematch Over 2.5 en DB: {len(ids)}")

def main():
    logging.info("Script iniciado (Sportmonks v3 football).")
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
            # Inserción diaria a la hora configurada
            if (last_insert_date is None or last_insert_date != now.date()) and now.hour == INSERT_HOUR:
                run_cycle_prematch("DIARIO")
                last_insert_date = now.date()
        except Exception as e:
            logging.error(f"Error en inserción diaria: {e}")

        try:
            # Monitoreo live: requiere track_live=TRUE en los partidos a seguir
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
