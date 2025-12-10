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

# Odds-API.io config
ODDS_API_KEY = "31a69749ed08a2878f08fc58d8c564bd411d14caaa81b8517515b7cc929cf683"
EVENTS_BASE = "https://api2.odds-api.io/v3/events"
ODDS_BASE   = "https://api2.odds-api.io/v3/odds"
SPORT = "football"
STATUS_DEFAULT = "pending"  # pending, live, settled (o combinados "pending,live")

# Bookmakers configurables para LIVE odds
LIVE_BOOKMAKERS = ["Bet365", "Unibet"]

# Cobertura parcial
COVERAGE_RATIO = 0.7

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

def iso_to_dt(raw_iso: str):
    """
    Convierte un ISO con 'Z' a datetime aware en zona Lima.
    """
    if not raw_iso:
        return datetime.now(LIMA_TZ)
    try:
        dt_utc = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
        return dt_utc.astimezone(LIMA_TZ)
    except Exception as e:
        logging.error(f"Error parseando ISO {raw_iso}: {e}")
        return datetime.now(LIMA_TZ)

def adjust_iso_minus_hours(raw_iso: str, hours: int):
    """
    Resta 'hours' al ISO (campo 'date' del response) y retorna string Lima 'DD/MM/YYYY HH:MM:SS'.
    """
    base_dt = iso_to_dt(raw_iso)
    adjusted = base_dt - timedelta(hours=hours)
    return adjusted.strftime("%d/%m/%Y %H:%M:%S")

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
# Odds-API.io REQUESTS
# ---------------------------------
def fetch_events(from_date: str, to_date: str, status: str = STATUS_DEFAULT):
    """
    Endpoint requiere fromYYYY-MM-DD y toYYYY-MM-DD (sin '=').
    Devuelve lista de eventos.
    """
    url = f"{EVENTS_BASE}?apiKey={ODDS_API_KEY}&sport={SPORT}&from{from_date}&to{to_date}&status={status}"
    try:
        r = requests.get(url, timeout=25)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # fallback por si algún día cambia
            return data.get("data", [])
        else:
            return []
    except Exception as e:
        logging.error(f"Error consultando events from-to: {e}")
        return []

def fetch_odds_live(event_id: str, bookmakers: list):
    """
    Trae cuotas (live o prematch según disponibilidad) para un eventId,
    filtrando por la lista configurable de bookmakers.
    """
    params = {
        "apiKey": ODDS_API_KEY,
        "eventId": event_id,
        "bookmakers": ",".join(bookmakers),
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    try:
        r = requests.get(ODDS_BASE, params=params, timeout=25)
        r.raise_for_status()
        # algunos endpoints devuelven {"data":[...]}, otros directamente lista
        payload = r.json()
        if isinstance(payload, dict):
            return payload.get("data", [])
        return payload if isinstance(payload, list) else []
    except Exception as e:
        logging.error(f"Error consultando odds eventId={event_id}: {e}")
        return []

# ---------------------------------
# EXTRACCIÓN DE OVER/UNDER 2.5
# ---------------------------------
def extract_best_totals_25(match_obj):
    """
    Busca las mejores cuotas Over/Under 2.5:
    data[] -> bookmakers[] -> markets[key='totals'] -> outcomes(name in over/under, point=2.5)
    """
    mejor_over, casa_over = None, None
    mejor_under, casa_under = None, None

    for bookmaker in match_obj.get("bookmakers", []):
        bk_name = normalize_text(bookmaker.get("title"))
        for market in bookmaker.get("markets", []):
            if market.get("key") == "totals":
                for outcome in market.get("outcomes", []):
                    name = (outcome.get("name") or "").lower()
                    point = outcome.get("point")
                    price = outcome.get("price")
                    try:
                        cuota = float(price)
                    except Exception:
                        continue
                    if not valid_odds(cuota):
                        continue
                    if name == "over" and point == 2.5:
                        if mejor_over is None or cuota > mejor_over:
                            mejor_over, casa_over = cuota, bk_name
                    if name == "under" and point == 2.5:
                        if mejor_under is None or cuota > mejor_under:
                            mejor_under, casa_under = cuota, bk_name
    return mejor_over, casa_over, mejor_under, casa_under

# ---------------------------------
# INSERT DB
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

        umbral_surebet = None
        cobertura_stake = None
        cobertura_resultado = None

        if cuota_over and cuota_under and valid_odds(cuota_over) and valid_odds(cuota_under):
            implied_sum, s_over, s_under, p_abs, p_pct = compute_surebet_stakes(
                cuota_over, cuota_under, BASE_STAKE
            )
            if implied_sum < 1.0:
                surebet_flag = True
            stake_over = s_over
            stake_under = s_under
            profit_abs = p_abs
            profit_pct = p_pct

            try:
                umbral_surebet = cuota_over / (cuota_over - 1)
                cobertura_stake = (100 * cuota_over) / cuota_under
                cobertura_resultado = 100 * (cuota_over - 1) - cobertura_stake
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
            %s, %s, %s,
            to_timestamp(%s, 'DD/MM/YYYY HH24:MI:SS') AT TIME ZONE 'America/Lima',
            %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            to_timestamp(%s, 'DD/MM/YYYY HH24:MI:SS') AT TIME ZONE 'America/Lima',
            to_timestamp(%s, 'DD/MM/YYYY HH24:MI:SS') AT TIME ZONE 'America/Lima',
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
            row["fecha_hora"],  # ajustada -5h del campo 'date'
            row.get("cuota_over"),
            row.get("casa_over"),
            row.get("cuota_under"),
            row.get("casa_under"),
            surebet_flag,
            stake_over,
            stake_under,
            profit_abs,
            profit_pct,
            row.get("umbral_surebet"),
            row.get("cobertura_stake"),
            row.get("cobertura_resultado"),
            "over_under",
            "over_2.5",
            row.get("created_at"),
            row.get("latest_bookmaker_update")
        )

        try:
            res = db_exec(q, vals, fetch=True)
            if res:
                ids.append(res[0]["id"])
        except Exception as e:
            logging.error(f"DB insert error (event_id={row.get('evento')}): {e}")
    return ids

# ---------------------------------
# PREMATCH: events from-to dinámico + odds por eventId + ajuste date -5h
# ---------------------------------
def fetch_prematch_over25():
    """
    1) from-to se calcula dinámicamente (hoy) en zona Lima.
    2) Trae events (status pending).
    3) Para cada event, consulta odds con eventId y LIVE_BOOKMAKERS.
    4) Ajusta el campo 'date' restando 5 horas antes de insertar.
    """
    today = datetime.now(LIMA_TZ).date().isoformat()
    events = fetch_events(today, today, STATUS_DEFAULT)
    resultados = []

    for ev in events:
        ev_id = ev.get("id")
        home = normalize_text(ev.get("home"))
        away = normalize_text(ev.get("away"))

        # Campo 'date' del response (ISO). Restar 5 horas ANTES de insertar.
        date_iso = ev.get("date")  # e.g. "2025-12-10T17:15:00Z"
        fecha_hora_str = adjust_iso_minus_hours(date_iso, 5)

        # Traer odds del evento usando bookmakers configurables
        odds_payload = fetch_odds_live(str(ev_id), LIVE_BOOKMAKERS)

        # Extraer mejores cuotas Over/Under 2.5
        mejor_over, casa_over, mejor_under, casa_under = None, None, None, None
        for match_odds in odds_payload:
            o_over, o_bk_over, o_under, o_bk_under = extract_best_totals_25(match_odds)
            if o_over is not None and (mejor_over is None or o_over > mejor_over):
                mejor_over, casa_over = o_over, o_bk_over
            if o_under is not None and (mejor_under is None or o_under > mejor_under):
                mejor_under, casa_under = o_under, o_bk_under

        # Cálculos auxiliares
        umbral_surebet = None
        cobertura_stake = None
        cobertura_resultado = None
        if mejor_over and mejor_under:
            try:
                umbral_surebet = mejor_over / (mejor_over - 1)
                cobertura_stake = (100 * mejor_over) / mejor_under
                cobertura_resultado = 100 * (mejor_over - 1) - cobertura_stake
            except Exception as e:
                logging.error(f"Error calculando umbral/cobertura (prematch): {e}")

            # Alertas prematch si hay surebet > $5
            inv_sum = (1/mejor_over) + (1/mejor_under)
            if inv_sum < 1:
                stake_over = BASE_STAKE * (1/mejor_over) / inv_sum
                stake_under = BASE_STAKE * (1/mejor_under) / inv_sum
                ganancia = min(stake_over * mejor_over, stake_under * mejor_under) - BASE_STAKE
                if ganancia > 5.0:
                    mensaje = (
                        f"🔥 Surebet Prematch!\n"
                        f"{home} vs {away}\n"
                        f"Fecha (ajustada -5h): {fecha_hora_str}\n"
                        f"Over 2.5: {mejor_over} ({casa_over}) → {stake_over:.2f}\n"
                        f"Under 2.5: {mejor_under} ({casa_under}) → {stake_under:.2f}\n"
                        f"Ganancia: {ganancia:.2f} con stake {BASE_STAKE}\n"
                        f"Bookmakers: {', '.join(LIVE_BOOKMAKERS)}"
                    )
                    send_telegram(mensaje)

        created_str = fecha_hora_str
        updated_str = fecha_hora_str

        resultados.append({
            "evento": ev_id,
            "local": home,
            "visitante": away,
            "fecha_hora": fecha_hora_str,  # ajustada -5h
            "cuota_over": mejor_over,
            "casa_over": casa_over,
            "cuota_under": mejor_under,
            "casa_under": casa_under,
            "created_at": created_str,
            "latest_bookmaker_update": updated_str,
            "umbral_surebet": umbral_surebet,
            "cobertura_stake": cobertura_stake,
            "cobertura_resultado": cobertura_resultado
        })

    return resultados

# ---------------------------------
# Cobertura minimax
# ---------------------------------
def cobertura_minimax_over_under(stake_over, cuota_over, cuota_under):
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
# MONITOR LIVE usando eventId + bookmakers
# ---------------------------------
def monitor_live_and_notify():
    # Partidos con track_live=TRUE y apuesta colocada al Over 2.5
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

    for pm in rows:
        match_id_db = pm["id"]
        event_id = str(pm["event_id"])
        home = normalize_text(pm.get("home_team") or "")
        away = normalize_text(pm.get("away_team") or "")
        over_odds_prematch = float(pm.get("odds_over") or 0)
        stake_over = float(pm.get("stake_over") or 0)

        # Minuto aproximado desde commence_time (guardado con la hora ajustada)
        commence_dt = pm.get("commence_time")
        if isinstance(commence_dt, datetime):
            commence_lima = commence_dt.astimezone(LIMA_TZ)
        else:
            commence_lima = datetime.now(LIMA_TZ)
        match_minute = max(0, int((datetime.now(LIMA_TZ) - commence_lima).total_seconds() / 60))

        # Traer odds para este evento y bookmakers configurables
        live_odds_payload = fetch_odds_live(event_id, LIVE_BOOKMAKERS)

        # Extraer mejores cuotas actualizadas (under live, over best)
        mejor_over, casa_over, mejor_under, casa_under = None, None, None, None
        for match_odds in live_odds_payload:
            o_over, o_bk_over, o_under, o_bk_under = extract_best_totals_25(match_odds)
            if o_over is not None and (mejor_over is None or o_over > mejor_over):
                mejor_over, casa_over = o_over, o_bk_over
            if o_under is not None and (mejor_under is None or o_under > mejor_under):
                mejor_under, casa_under = o_under, o_bk_under

        # Necesitamos al menos el Under live y nuestro Over prematch
        if not (mejor_under and over_odds_prematch > 1):
            continue

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
            db_exec("""
                INSERT INTO alerts (match_id, kind, message, profit_pct, profit_abs, created_at)
                VALUES (%s,%s,%s,%s,%s,NOW())
            """, (match_id_db, "surebet_live", msg, profit_pct, profit_abs))
        else:
            # Sugerir cobertura si no hay surebet
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
# CICLOS
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now(LIMA_TZ)
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        _last_heartbeat = now

def run_cycle_prematch(tag):
    logging.info("Iniciando carga PREMATCH (from-to dinámico hoy, status=pending)")
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
    logging.info("Script iniciado (Odds-API.io events from-to dinámico + live odds por eventId/bookmakers).")
    # Ejecuta prematch inmediatamente
    run_threaded(job_prematch)
    # Schedulers
    schedule.every(15).minutes.do(run_threaded, job_prematch)
    schedule.every(2).minutes.do(run_threaded, job_monitor)

    while True:
        schedule.run_pending()
        time.sleep(1)

# ---------------------------------
# FLASK
# ---------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Surebet bot (Odds-API.io) is running."

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    t = threading.Thread(target=main, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
