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
import unicodedata
import schedule
import json

# ---------------------------------
# CONFIG
# ---------------------------------
TG_TOKEN = "8252990863:AAEAN1qEh8xCwKT6-61rA1lp8nSHrHSFQLc"
TG_CHAT  = "1206397833"

PG_HOST = "dpg-d4b25nggjchc73f7d1o0-a"
PG_PORT = 5432
PG_DB   = "surebet_db"
PG_USER = "surebet_db_user"
PG_PASS = "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU"

BASE_STAKE = 100.0
CURRENCY = "USD"
LIMA_TZ = pytz.timezone("America/Lima")

ODDS_API_KEY = "31a69749ed08a2878f08fc58d8c564bd411d14caaa81b8517515b7cc929cf683"
EVENTS_BASE = "https://api2.odds-api.io/v3/events"
ODDS_BASE   = "https://api2.odds-api.io/v3/odds"
SPORT = "football"
STATUS_DEFAULT = "pending"

LIVE_BOOKMAKERS = ["Apuesta Total", "Betano"]
COVERAGE_RATIO = 0.7
#EVENT_LIMIT = 50  # máximo eventos por ciclo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("surebet_oddsapi.log"),
        logging.StreamHandler()
    ]
)

# ---------------------------------
# UTILIDADES
# ---------------------------------
def normalize_text(text: str) -> str:
    if not text:
        return text
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

def valid_odds(odds):
    try:
        return odds is not None and float(odds) > 1.01
    except Exception:
        return False

def iso_to_lima_dt(raw_iso: str) -> datetime:
    """Convierte ISO UTC (con 'Z') a datetime aware en Lima."""
    if not raw_iso:
        return datetime.now(LIMA_TZ)
    try:
        dt_utc = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
        return dt_utc.astimezone(LIMA_TZ)
    except Exception as e:
        logging.error(f"Error parseando ISO {raw_iso}: {e}")
        return datetime.now(LIMA_TZ)

def compute_surebet_stakes(odds_over, odds_under, stake_total):
    try:
        inv_over = 1.0 / float(odds_over)
        inv_under = 1.0 / float(odds_under)
        implied_sum = inv_over + inv_under
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
# API REQUESTS
# ---------------------------------
def fetch_events(from_date: str, to_date: str, status: str = STATUS_DEFAULT):
    """
    Endpoint events v3: usa fromYYYY-MM-DD y toYYYY-MM-DD sin '='.
    """
    url = f"{EVENTS_BASE}?apiKey={ODDS_API_KEY}&sport={SPORT}&from{from_date}&to{to_date}&status={status}"
    try:
        r = requests.get(url, timeout=25)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("data", [])
        return []
    except Exception as e:
        logging.error(f"Error consultando events from-to: {e}")
        return []

def chunk_list(lst, size=10):
    """Divide una lista en trozos de tamaño size."""
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def fetch_odds_multi(event_ids: list, bookmakers: list):
    """
    Consulta cuotas en vivo para múltiples eventIds en lotes de 10.
    Devuelve un dict {eventId: odds_obj}.
    """
    results = {}
    for batch in chunk_list(event_ids, 10):
        url = f"{ODDS_BASE}/multi"
        params = {
            "apiKey": ODDS_API_KEY,
            "eventIds": ",".join(str(eid) for eid in batch),
            "bookmakers": ",".join(bookmakers),
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }
        try:
            r = requests.get(url, params=params, timeout=25)
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, dict):
                data = payload.get("data", {})
                if isinstance(data, dict):
                    results.update(data)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if hasattr(e, "response") and e.response is not None else None
            logging.error(f"HTTP {status} en odds/multi batch {batch}: {e}")
        except Exception as e:
            logging.error(f"Error consultando odds/multi batch {batch}: {e}")
    return results

# ---------------------------------
# EXTRACCIÓN DE OVER/UNDER 2.5
# ---------------------------------
def extract_best_totals_25_v3(bookmakers_dict):
    """
    Recorre mercados 'Totals' y toma la mejor cuota para hdp=2.5.
    Retorna: (mejor_over, casa_over, mejor_under, casa_under)
    """
    mejor_over, casa_over = None, None
    mejor_under, casa_under = None, None
    if not isinstance(bookmakers_dict, dict):
        return mejor_over, casa_over, mejor_under, casa_under
    for bk_name, markets in bookmakers_dict.items():
        for market in markets:
            if (market.get("name") or "").lower() == "totals":
                for outcome in market.get("odds", []):
                    if outcome.get("hdp") == 2.5:
                        try:
                            cuota_over = float(outcome.get("over"))
                            cuota_under = float(outcome.get("under"))
                        except Exception:
                            continue
                        if valid_odds(cuota_over):
                            if mejor_over is None or cuota_over > mejor_over:
                                mejor_over, casa_over = cuota_over, bk_name
                        if valid_odds(cuota_under):
                            if mejor_under is None or cuota_under > mejor_under:
                                mejor_under, casa_under = cuota_under, bk_name
    return mejor_over, casa_over, mejor_under, casa_under

# ---------------------------------
# PREMATCH
# ---------------------------------
def fetch_prematch_over25():
    """
    Carga todos los eventos del día y cuotas por lotes (odds/multi con chunks de 10),
    calcula hora Lima y prepara filas para inserción.
    """
    today = datetime.now(LIMA_TZ).date().isoformat()
    events = fetch_events(today, today, STATUS_DEFAULT)
    resultados = []

    # Usar todos los eventos
    selected_events = events
    event_ids = [ev.get("id") for ev in selected_events if ev.get("id") is not None]

    # Llamada por lotes (chunks de 10 eventIds)
    odds_multi = fetch_odds_multi(event_ids, LIVE_BOOKMAKERS)

    for ev in selected_events:
        ev_id = ev.get("id")
        home = normalize_text(ev.get("home"))
        away = normalize_text(ev.get("away"))
        commence_dt_lima = iso_to_lima_dt(ev.get("date"))

        odds_obj = odds_multi.get(str(ev_id), {})
        mejor_over, casa_over, mejor_under, casa_under = None, None, None, None
        if isinstance(odds_obj, dict) and "bookmakers" in odds_obj:
            mejor_over, casa_over, mejor_under, casa_under = extract_best_totals_25_v3(odds_obj["bookmakers"])

        resultados.append({
            "evento": ev_id,
            "local": home,
            "visitante": away,
            "commence_dt_lima": commence_dt_lima,
            "cuota_over": mejor_over,
            "casa_over": casa_over,
            "cuota_under": mejor_under,
            "casa_under": casa_under,
            "created_at": commence_dt_lima,
            "latest_bookmaker_update": commence_dt_lima
        })
    return resultados

# ---------------------------------
# INSERT DB
# ---------------------------------
def insert_matches(rows):
    """
    Inserta/actualiza filas en matches usando commence_time en hora Lima (datetime aware).
    Asume clave única (event_id, market, selection).
    """
    ids = []
    for row in rows:
        cuota_over = row.get("cuota_over")
        cuota_under = row.get("cuota_under")

        surebet_flag = False
        stake_over = None
        stake_under = None
        profit_abs = None
        profit_pct = None
        umbral_surebet = None
        cobertura_stake = None
        cobertura_resultado = None

        if cuota_over and cuota_under and valid_odds(cuota_over) and valid_odds(cuota_under):
            implied_sum, s_over, s_under, p_abs, p_pct = compute_surebet_stakes(
                cuota_over, cuota_under, BASE_STAKE
            )
            surebet_flag = implied_sum < 1.0
            stake_over = s_over
            stake_under = s_under
            profit_abs = p_abs
            profit_pct = p_pct
            try:
                umbral_surebet = cuota_over / (cuota_over - 1)
                cobertura_stake = (100.0 * cuota_over) / cuota_under
                cobertura_resultado = 100.0 * (cuota_over - 1) - cobertura_stake
            except Exception as e:
                logging.error(f"Error calculando umbral/cobertura: {e}")

        q = """
        INSERT INTO matches (
            event_id, home_team, away_team, commence_time,
            odds_over, bookmaker_over,
            odds_under, bookmaker_under,
            surebet, stake_over, stake_under, profit_abs, profit_pct,
            umbral_surebet, cobertura_stake, cobertura_resultado,
            market, selection, created_at, updated_at, bet_placed, track_live
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            TRUE, FALSE
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
            umbral_surebet = EXCLUDED.umbral_surebet,
            cobertura_stake = EXCLUDED.cobertura_stake,
            cobertura_resultado = EXCLUDED.cobertura_resultado,
            updated_at = EXCLUDED.updated_at
        RETURNING id
        """

        vals = (
            row["evento"],
            row["local"],
            row["visitante"],
            row["commence_dt_lima"],  # datetime aware Lima
            row.get("cuota_over"),
            row.get("casa_over"),
            row.get("cuota_under"),
            row.get("casa_under"),
            surebet_flag,
            stake_over,
            stake_under,
            profit_abs,
            profit_pct,
            umbral_surebet,
            cobertura_stake,
            cobertura_resultado,
            "over_under",
            "over_2.5",
            row.get("created_at"),
            row.get("latest_bookmaker_update"),
        )

        try:
            res = db_exec(q, vals, fetch=True)
            if res:
                ids.append(res[0]["id"])
        except Exception as e:
            logging.error(f"DB insert error (event_id={row.get('evento')}): {e}")
    return ids

# ---------------------------------
# Cobertura minimax
# ---------------------------------
def cobertura_minimax_over_under(stake_over, cuota_over, cuota_under):
    """
    Calcula el stake óptimo en Under para minimizar la pérdida máxima dado stake en Over.
    Retorna (stake_under_opt, loss_max).
    """
    try:
        if stake_over <= 0 or cuota_over <= 1 or cuota_under <= 1:
            return 0.0, None
        stake_under_opt = (stake_over * cuota_over) / cuota_under
        ganancia_over = (stake_over * cuota_over) - stake_over - stake_under_opt
        ganancia_under = (stake_under_opt * cuota_under) - stake_under_opt - stake_over
        loss_max = min(ganancia_over, ganancia_under)
        return stake_under_opt, loss_max
    except Exception as e:
        logging.error(f"Error en cobertura_minimax_over_under: {e}")
        return 0.0, None

# ---------------------------------
# MONITOR LIVE (usa odds/multi con chunks de 10)
# ---------------------------------
def monitor_live_and_notify():
    """
    Monitorea partidos con apuesta prematch Over 2.5 (track_live=TRUE).
    Usa una sola llamada por lote a odds/multi para todos los eventIds (chunks de 10).
    Evalúa surebet en vivo; si no hay, sugiere cobertura minimax.
    """
    rows = db_exec("""
        SELECT id, event_id, home_team, away_team, odds_over, stake_over, commence_time
        FROM matches
        WHERE track_live=TRUE
          AND market='over_under'
          AND selection='over_2.5'
          AND bet_placed=TRUE
    """, fetch=True)

    if not rows:
        logging.info("No hay partidos con track_live=TRUE para monitorear.")
        return

    event_ids = [str(r["event_id"]) for r in rows if r.get("event_id") is not None]
    odds_multi = fetch_odds_multi(event_ids, LIVE_BOOKMAKERS)

    for pm in rows:
        match_id_db = pm["id"]
        event_id = str(pm["event_id"])
        home = normalize_text(pm.get("home_team") or "")
        away = normalize_text(pm.get("away_team") or "")
        over_odds_prematch = float(pm.get("odds_over") or 0)
        stake_over = float(pm.get("stake_over") or 0)

        # Calcular minuto del partido (contexto)
        commence_dt = pm.get("commence_time")
        if isinstance(commence_dt, datetime):
            commence_lima = commence_dt.astimezone(LIMA_TZ)
        else:
            commence_lima = datetime.now(LIMA_TZ)
        match_minute = max(0, int((datetime.now(LIMA_TZ) - commence_lima).total_seconds() / 60))

        # Obtener cuotas del eventId desde la respuesta multi
        odds_obj = odds_multi.get(event_id, {})
        mejor_over, casa_over, mejor_under, casa_under = None, None, None, None
        if isinstance(odds_obj, dict) and "bookmakers" in odds_obj:
            mejor_over, casa_over, mejor_under, casa_under = extract_best_totals_25_v3(odds_obj["bookmakers"])

        # Validación mínima
        if not (mejor_under and over_odds_prematch > 1):
            logging.info(f"[{event_id}] Sin Under válido o Over prematch inválido. Saltando.")
            continue

        # Evaluar surebet live
        implied_sum, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
            over_odds_prematch, mejor_under, BASE_STAKE
        )

        if implied_sum < 1.0:
            msg = (
                f"🔥 Surebet LIVE {home} vs {away} (min {match_minute}).\n"
                f"Over 2.5 prematch @ {over_odds_prematch} | Under 2.5 live @ {mejor_under} ({casa_under}).\n"
                f"Stake base {BASE_STAKE:.2f} {CURRENCY} ⇒ Over: {s_over:.2f}, Under: {s_under:.2f}.\n"
                f"Profit esperado: {profit_abs:.2f} {CURRENCY} ({profit_pct*100:.2f}%).\n"
                f"Bookmakers: {', '.join(LIVE_BOOKMAKERS)}"
            )
            send_telegram(msg)
            try:
                db_exec("""
                    INSERT INTO alerts (match_id, kind, message, profit_pct, profit_abs, created_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                """, (match_id_db, "surebet_live", msg, profit_pct, profit_abs))
            except Exception as e:
                logging.error(f"Error insert alert surebet_live (match_id={match_id_db}): {e}")
        else:
            # Cobertura minimax si no hay surebet
            if stake_over > 0 and over_odds_prematch > 1 and mejor_under > 1:
                stake_under_opt, loss_max = cobertura_minimax_over_under(stake_over, over_odds_prematch, mejor_under)
                stake_under_partial = round(stake_under_opt * COVERAGE_RATIO, 2) if stake_under_opt else 0.0
                msg = (
                    f"🛡️ Cobertura minimax {home} vs {away} (min {match_minute}).\n"
                    f"Over 2.5 prematch: stake {stake_over:.2f} @ {over_odds_prematch}.\n"
                    f"Under 2.5 live: @ {mejor_under} ({casa_under}).\n"
                    f"⇒ Stake Under óptimo: {stake_under_opt:.2f} {CURRENCY} (pérdida máxima ≈ {loss_max:.2f} {CURRENCY}).\n"
                    f"Alternativa parcial ({int(COVERAGE_RATIO*100)}%): {stake_under_partial:.2f} {CURRENCY}.\n"
                    f"Bookmakers: {', '.join(LIVE_BOOKMAKERS)}"
                )
                send_telegram(msg)

# ---------------------------------
# CICLOS Y SCHEDULER
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now(LIMA_TZ)
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        _last_heartbeat = now

def run_cycle_prematch(tag):
    logging.info(f"[{tag}] Iniciando PREMATCH (from-to hoy, status={STATUS_DEFAULT}")
    rows = fetch_prematch_over25()
    ids = []
    try:
        ids = insert_matches(rows)
        logging.info(f"[{tag}] Insertados en BD: {len(ids)}")
    except Exception as e:
        logging.error(f"Error insert prematch: {e}")
    logging.info(f"[{tag}] Prematch Over/Under 2.5 procesados: {len(ids)}")

def job_prematch():
    now = datetime.now(LIMA_TZ)
    logging.info(f"[PREMATCH] Disparado job_prematch a las {now.strftime('%d/%m/%Y %H:%M:%S')}")
    run_cycle_prematch("CADA_15_MIN")
    logging.info(f"[PREMATCH] Ejecutado ciclo prematch a las {now.strftime('%d/%m/%Y %H:%M:%S')}")

def job_monitor():
    now = datetime.now(LIMA_TZ)
    logging.info(f"[MONITOR] Disparado job_monitor a las {now.strftime('%d/%m/%Y %H:%M:%S')}")
    monitor_live_and_notify()
    heartbeat()
    logging.info(f"[MONITOR] Ejecutado monitoreo a las {now.strftime('%d/%m/%Y %H:%M:%S')}")

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def main():
    logging.info("Script iniciado (Events + Odds/multi con chunks de 10, hora Lima correcta, monitoreo y prematch).")
    # Ejecuta prematch inmediatamente
    run_threaded(job_prematch)
    # Schedulers
    schedule.every(15).minutes.do(run_threaded, job_prematch)
    schedule.every(5).minutes.do(run_threaded, job_monitor)  # Intervalo prudente para evitar 429

    while True:
        schedule.run_pending()
        time.sleep(1)

# ---------------------------------
# FLASK
# ---------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Surebet bot (Odds-API.io v3 multi chunks=10) is running."

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    t = threading.Thread(target=main, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
