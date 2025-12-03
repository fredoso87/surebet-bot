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
import threading
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
def normalize_text(text: str) -> str:
    """
    Normaliza un string eliminando acentos y caracteres especiales
    que no son representables en WIN1252.
    Ejemplo: 'Łódź' -> 'Lodz', 'São Paulo' -> 'Sao Paulo'
    """
    if not text:
        return text
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

# ---------------------------------
# BOOKMAKERS CONFIG
# ---------------------------------
def load_bookmakers_map():
    all_bookmakers = []
    page = 1

    while True:
        url = "https://api.sportmonks.com/v3/odds/bookmakers"
        params = {"api_token": SPORTMONKS_TOKEN, "page": page}

        full_url = f"{url}?api_token={SPORTMONKS_TOKEN}&page={page}"
        #logging.info(f"🌐 Consumiento API bookmakers → {full_url}")

        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            logging.error(f"❌ Error obteniendo bookmakers (page={page}): {e}")
            break

        data = payload.get("data", [])
        pagination = payload.get("pagination", {})  # 👈 directo en raíz, no en meta

        #logging.info(f"📄 Respuesta API page={page}: count={pagination.get('count')} "
        #             f"per_page={pagination.get('per_page')} current_page={pagination.get('current_page')} "
        #             f"has_more={pagination.get('has_more')} next_page={pagination.get('next_page')}")

        all_bookmakers.extend(data)
        #logging.info(f"✅ Bookmakers acumulados tras page={page}: {len(all_bookmakers)}")

        # Condición de corte
        if not pagination or not pagination.get("has_more"):
            break

        # Avanza a la siguiente página
        page += 1

    bookmaker_map = {
        bk.get("id"): bk.get("name")
        for bk in all_bookmakers
        if bk.get("id") is not None and bk.get("name") is not None
    }

    logging.info(f"🎯 Bookmakers cargados: {len(bookmaker_map)} casas de apuesta (total crudo: {len(all_bookmakers)})")
    return bookmaker_map
    
BOOKMAKER_IDS = [212,127,152,83,84,28,26,24,16,9,2,8,35,18,20,21,123,91,216,215,1,5,24,22,33,35,39]
BOOKMAKER_MAP = load_bookmakers_map()

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
        # Validaciones iniciales
        if not odds_over or not odds_under:
            return 999.0, 0.0, 0.0, 0.0, 0.0
        if float(odds_over) <= 1.0 or float(odds_under) <= 1.0:
            # cuotas inválidas (no pueden ser <=1)
            return 999.0, 0.0, 0.0, 0.0, 0.0

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

def parse_datetime_safe(raw):
    """
    Convierte un string de fecha del response de odds_data a formato Lima (+5 horas).
    Soporta formatos con 'Z' y sin 'Z'.
    """
    if not raw:
        return None
    try:
        if "Z" in raw:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        return (dt.astimezone(LIMA_TZ) + timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")
    except Exception as e:
        logging.error(f"Error parseando fecha {raw}: {e}")
        return None

# ---------------------------------
# PREMATCH: mejores Over/Under 2.5 (marketId=7) + surebet prematch
# ---------------------------------
def fetch_prematch_over25():
    hoy = datetime.now(LIMA_TZ).date()
    manana = hoy + timedelta(days=3)
    base_url = f"{SPORTMONKS_BASE}/fixtures/between/{hoy.isoformat()}/{manana.isoformat()}"
    page = 1
    all_fixtures = []

    while True:
        try:
            url = f"{base_url}?api_token={SPORTMONKS_TOKEN}&page={page}&include=participants"
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logging.error(f"Error obteniendo fixtures (page={page}): {e}")
            break

        all_fixtures.extend(data.get("data", []))
        pagination = data.get("pagination", {})
        logging.info(f"✅ Fixtures acumulados tras page={page}: {len(all_fixtures)}")

        if not pagination or not pagination.get("has_more"):
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

        # fecha del partido
        fecha_hora_raw = fixture.get("starting_at")
        try:
            dt = datetime.fromisoformat(fecha_hora_raw.replace("Z", "+00:00"))
            fecha_hora_str = (dt.astimezone(LIMA_TZ) + timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            fecha_hora_str = (datetime.now(LIMA_TZ) + timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")

        odds_data = sportmonks_request(f"/odds/pre-match/fixtures/{fixture_id}/markets/7")

        mejor_over, casa_over, created_over, updated_over = None, None, None, None
        mejor_under, casa_under, created_under, updated_under = None, None, None, None

        for outcome in odds_data.get("data", []):
            bookmaker_id = outcome.get("bookmaker_id")
            label = (outcome.get("label") or "").lower()
            total_line = outcome.get("total")
            cuota = outcome.get("value")

            if BOOKMAKER_IDS and bookmaker_id not in BOOKMAKER_IDS:
                continue

            try:
                cuota = float(cuota)
            except Exception:
                continue

            if label == "over" and total_line in {"2.5"}:
                if mejor_over is None or cuota > mejor_over:
                    mejor_over = cuota
                    casa_over = BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id))
                    created_over = parse_datetime_safe(outcome.get("created_at"))
                    updated_over = parse_datetime_safe(outcome.get("latest_bookmaker_update"))

            if label == "under" and total_line in {"2.5"}:
                if mejor_under is None or cuota > mejor_under:
                    mejor_under = cuota
                    casa_under = BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id))
                    created_under = parse_datetime_safe(outcome.get("created_at"))
                    updated_under = parse_datetime_safe(outcome.get("latest_bookmaker_update"))

        # elegir qué timestamps usar (si hay over y under, prioriza el más reciente)
        created_str = created_over or created_under
        updated_str = updated_over or updated_under

        umbral_surebet, cobertura_stake, cobertura_resultado = None, None, None
        if mejor_over and mejor_under:
            umbral_surebet = mejor_over / (mejor_over - 1)
            cobertura_stake = (100 * mejor_over) / mejor_under
            cobertura_resultado = 100 * (mejor_over - 1) - cobertura_stake

        resultados.append({
            "evento": fixture_id,
            "local": normalize_text(local),
            "visitante": normalize_text(visitante),
            "fecha_hora": fecha_hora_str,
            "cuota_over": mejor_over,
            "casa_over": normalize_text(casa_over),
            "cuota_under": mejor_under,
            "casa_under": normalize_text(casa_under),
            "created_at": created_str,
            "latest_bookmaker_update": updated_str,
            "umbral_surebet": umbral_surebet,
            "cobertura_stake": cobertura_stake,
            "cobertura_resultado": cobertura_resultado,
            "stopped": odds_data.get("stopped")
        })

        # ALERTA TELEGRAM extendida
        if mejor_over and mejor_under:
            inv_sum = (1/mejor_over) + (1/mejor_under)
            if inv_sum < 1:
                stake_over = BASE_STAKE * (1/mejor_over) / inv_sum
                stake_under = BASE_STAKE * (1/mejor_under) / inv_sum
                ganancia = min(stake_over * mejor_over, stake_under * mejor_under) - BASE_STAKE
                if ganancia > 5.0:
                    mensaje = (
                        f"🔥 Surebet Prematch encontrado!\n"
                        f"{local} vs {visitante}\n"
                        f"Fecha: {fecha_hora_str}\n"
                        f"Over 2.5: {mejor_over} ({casa_over}) → Apostar {stake_over:.2f}\n"
                        f"Under 2.5: {mejor_under} ({casa_under}) → Apostar {stake_under:.2f}\n"
                        f"Ganancia asegurada: {ganancia:.2f} con stake {BASE_STAKE}\n\n"
                        f"📊 Umbral de surebet (Under mínimo): {umbral_surebet:.2f}\n"
                        f"💰 Cobertura con 100 soles en Over: Apostar {cobertura_stake:.2f} al Under\n"
                        f"Resultado neto asegurado: {cobertura_resultado:.2f} soles"
                    )
                    if odds_data.get("stopped") is False:
                        send_telegram(mensaje)
                        logging.info(f"Alerta enviada por Telegram: {mensaje}")
                    else:
                        logging.info(f"⏸️ Mercado detenido (stopped=True) para fixture {fixture_id}, alerta NO enviada.")

    return resultados

# ---------------------------------
# INSERT DB: guarda mejores over/under, casas, surebet y stakes con BASE_STAKE
# ---------------------------------

def insert_matches(rows):
    ids = []
    for row in rows:
        cuota_over = row.get("cuota_over")
        cuota_under = row.get("cuota_under")
        stopped = row.get("stopped")  # 👈 lo leemos pero no lo guardamos en BD

        surebet_flag = False
        stake_over = None
        stake_under = None
        profit_abs = None
        profit_pct = None

        # nuevos campos
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

            # cálculo de umbral y cobertura
            try:
                umbral_surebet = cuota_over / (cuota_over - 1)
                cobertura_stake = (100 * cuota_over) / cuota_under
                cobertura_resultado = 100 * (cuota_over - 1) - cobertura_stake
            except Exception as e:
                logging.error(f"Error calculando umbral/cobertura: {e}")

        # 👇 Si el mercado está detenido, el surebet se fuerza a False
        if stopped is True:
            surebet_flag = False

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
            row["fecha_hora"],
            row.get("cuota_over"),
            row.get("casa_over"),
            row.get("cuota_under"),
            row.get("casa_under"),
            surebet_flag,   # 👈 ya condicionado por stopped
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

def fetch_fixture_details(fixture_id):
    """
    Consulta el estado y minuto actual de un fixture en Sportmonks.
    Devuelve (state_id, match_minute).
    """
    # 👇 Ojo: aquí NO ponemos /football porque SPORTMONKS_BASE ya lo incluye
    fixture_data = sportmonks_request(f"/fixtures/{fixture_id}", params={"include": "periods"})
    data = fixture_data.get("data", {})

    state_id = data.get("state_id")

    match_minute = 0
    periods = data.get("periods", [])
    if periods:
        try:
            match_minute = int(periods[-1].get("minutes", 0))
        except Exception:
            match_minute = 0

    return state_id, match_minute

def fetch_fixture_scores(fixture_id):
    """
    Consulta los goles actuales de un fixture en Sportmonks.
    Devuelve (home_score, away_score).
    """
    fixture_data = sportmonks_request(f"/fixtures/{fixture_id}", params={"include": "scores"})
    data = fixture_data.get("data", {})

    home_score, away_score = 0, 0
    for s in data.get("scores", []):
        if s.get("description") == "CURRENT":
            if s["score"]["participant"] == "home":
                home_score = s["score"]["goals"]
            elif s["score"]["participant"] == "away":
                away_score = s["score"]["goals"]

    return home_score, away_score


# Config opcional para cobertura parcial (0.0 a 1.0). Ej: 0.7 = 70% del minimax.
COVERAGE_RATIO = 0.7

def cobertura_minimax_over_under(stake_over, cuota_over, cuota_under):
    """
    Calcula la cobertura minimax para un mercado Over/Under 2.5.
    Devuelve (stake_under_opt, loss_max).

    Parámetros:
        stake_over (float): Stake ya apostado al Over 2.5.
        cuota_over (float): Cuota del Over 2.5 (prematch).
        cuota_under (float): Cuota del Under 2.5 (live).

    Retorna:
        stake_under_opt (float): Stake óptimo a apostar en el Under 2.5.
        loss_max (float): Pérdida máxima asegurada con esa cobertura.
    """
    try:
        # Validaciones básicas
        if stake_over <= 0 or cuota_over <= 1 or cuota_under <= 1:
            return 0.0, None

        # Fórmula de cobertura minimax:
        # Queremos que el beneficio neto en ambos escenarios sea lo más equilibrado posible.
        # Stake_under = (stake_over * cuota_over) / cuota_under
        stake_under_opt = (stake_over * cuota_over) / cuota_under

        # Ganancia si gana Over
        ganancia_over = (stake_over * cuota_over) - stake_over - stake_under_opt

        # Ganancia si gana Under
        ganancia_under = (stake_under_opt * cuota_under) - stake_under_opt - stake_over

        # La pérdida máxima es el mínimo de ambas ganancias (si es negativo)
        loss_max = min(ganancia_over, ganancia_under)

        return stake_under_opt, loss_max

    except Exception as e:
        logging.error(f"Error en cobertura_minimax_over_under: {e}")
        return 0.0, None
def monitor_live_and_notify():
    # --- BLOQUE ORIGINAL: partidos con track_live=TRUE ---
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
    else:
        fixture_ids = [r["event_id"] for r in rows]
        prematch_index = {r["event_id"]: r for r in rows}
        live_events = fetch_live_under25(fixture_ids)
        if not live_events:
            logging.info("No se obtuvieron odds UNDER 2.5 en vivo para los fixtures marcados.")
        else:
            for ev in live_events:
                fixture_id = ev["event_id"]
                under_live = float(ev.get("cuota_under25") or 0)
                bookmaker_live_name = ev.get("bookmaker_name") or ""

                # Estado y minuto
                state_id, match_minute = fetch_fixture_details(fixture_id)
                home_score, away_score = fetch_fixture_scores(fixture_id)

                # Desactivar por estado (3–13)
                if state_id is not None and 3 <= state_id <= 13:
                    logging.info(f"Partido {fixture_id} con state_id={state_id}, se desactiva track_live.")
                    db_exec("UPDATE matches SET track_live=FALSE WHERE event_id=%s", (fixture_id,))
                    continue

                # Ignorar después del minuto 30
                if match_minute > 30:
                    logging.info(f"Partido {fixture_id} minuto {match_minute}, se deja de monitorear.")
                    db_exec("UPDATE matches SET track_live=FALSE WHERE event_id=%s", (fixture_id,))
                    continue

                pm = prematch_index.get(fixture_id)
                if not pm:
                    continue

                match_id_db = pm["id"]
                home = pm.get("home_team") or ""
                away = pm.get("away_team") or ""
                over_odds_prematch = float(pm.get("odds_over") or 0)
                stake_over = float(pm.get("stake_over") or 0)

                # ⚽️ Gol temprano
                if (home_score + away_score) > 0 and match_minute <= 30:
                    msg = (
                        f"⚽️ GOL temprano en {home} vs {away} (min {match_minute}).\n"
                        f"Marcador actual: {home_score}-{away_score}.\n"
                        f"👉 Considerar CASHOUT."
                    )
                    send_telegram(msg)

                    if stake_over > 0 and over_odds_prematch > 1 and under_live > 1:
                        stake_under_opt, loss_max = cobertura_minimax_over_under(
                            stake_over, over_odds_prematch, under_live
                        )
                        if stake_under_opt > 0:
                            msg_alt = (
                                f"⚖️ Si NO haces cashout:\n"
                                f"⇒ Apostar {stake_under_opt:.2f} {CURRENCY} al Under 2.5 @ {under_live} ({bookmaker_live_name}).\n"
                                f"Pérdida máxima ≈ {loss_max:.2f} {CURRENCY}."
                            )
                            send_telegram(msg_alt)

                    db_exec("UPDATE matches SET track_live=FALSE WHERE event_id=%s", (fixture_id,))
                    continue

                # Lógica de surebet con BASE_STAKE
                implied_sum, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
                    over_odds_prematch, under_live, BASE_STAKE
                )

                if implied_sum < 1.0:
                    msg = (
                        f"🔥 Surebet LIVE {home} vs {away} (min {match_minute}).\n"
                        f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live_name}).\n"
                        f"Stake base {BASE_STAKE:.2f} {CURRENCY} ⇒ Over: {s_over:.2f}, Under: {s_under:.2f}.\n"
                        f"Profit esperado: {profit_abs:.2f} {CURRENCY} ({profit_pct*100:.2f}%)."
                    )
                    send_telegram(msg)
                    db_exec("""
                        INSERT INTO alerts (match_id, kind, message, profit_pct, profit_abs, created_at)
                        VALUES (%s,%s,%s,%s,%s,NOW())
                    """, (match_id_db, "surebet_live", msg, profit_pct, profit_abs))
                else:
                    if stake_over > 0 and over_odds_prematch > 1 and under_live > 1:
                        stake_under_opt, loss_max = cobertura_minimax_over_under(stake_over, over_odds_prematch, under_live)
                        stake_under_partial = round(stake_under_opt * COVERAGE_RATIO, 2) if stake_under_opt else 0.0
                        msg = (
                            f"🛡️ Cobertura minimax {home} vs {away} (min {match_minute}).\n"
                            f"Over 2.5 prematch: stake {stake_over:.2f} @ {over_odds_prematch}.\n"
                            f"Under 2.5 live: @ {under_live} ({bookmaker_live_name}).\n"
                            f"⇒ Stake Under óptimo: {stake_under_opt:.2f} {CURRENCY} (pérdida máxima ≈ {loss_max:.2f} {CURRENCY}).\n"
                            f"Alternativa parcial ({int(COVERAGE_RATIO*100)}%): {stake_under_partial:.2f} {CURRENCY}."
                        )
                        send_telegram(msg)

    # --- NUEVO BLOQUE: buscar partidos de HOY por commence_time ---
    hoy = datetime.now(LIMA_TZ).date()
    partidos_hoy = db_exec("""
        SELECT id, event_id, home_team, away_team, odds_over, odds_under
        FROM matches
        WHERE DATE(commence_time) = CURDATE()
        AND commence_time BETWEEN DATE_SUB(NOW(), INTERVAL 2 HOUR) AND DATE_ADD(NOW(), INTERVAL 1 HOUR)
        AND market='over_under';

    """, (hoy,), fetch=True)

    if not partidos_hoy:
        logging.info("No hay partidos de hoy en la BD para buscar surebets.")
        return

    fixture_ids_hoy = [r["event_id"] for r in partidos_hoy]
    live_events_hoy = fetch_live_under25(fixture_ids_hoy)

    for ev in live_events_hoy:
        fixture_id = ev["event_id"]
        under_live = float(ev.get("cuota_under25") or 0)
        bookmaker_live_name = ev.get("bookmaker_name") or ""

        state_id, match_minute = fetch_fixture_details(fixture_id)
        if state_id is None or state_id not in (2,):  # 2 = Live
            continue

        pm = next((r for r in partidos_hoy if r["event_id"] == fixture_id), None)
        if not pm:
            continue

        home = pm.get("home_team") or ""
        away = pm.get("away_team") or ""
        over_odds_prematch = float(pm.get("odds_over") or 0)

        implied_sum, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
            over_odds_prematch, under_live, BASE_STAKE
        )

        if implied_sum < 1.0:
            msg = (
                f"🔥 Surebet HOY en vivo {home} vs {away} (min {match_minute}).\n"
                f"Over 2.5 prematch @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live_name}).\n"
                f"Stake base {BASE_STAKE:.2f} {CURRENCY} ⇒ Over: {s_over:.2f}, Under: {s_under:.2f}.\n"
                f"Profit esperado: {profit_abs:.2f} {CURRENCY} ({profit_pct*100:.2f}%)."
            )
            send_telegram(msg)
            db_exec("""
                INSERT INTO alerts (match_id, kind, message, profit_pct, profit_abs, created_at)
                VALUES (%s,%s,%s,%s,%s,NOW())
            """, (pm["id"], "surebet_live_today", msg, profit_pct, profit_abs))
# CICLO PRINCIPAL
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now(LIMA_TZ)
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        #send_telegram("Heartbeat: activo (prematch mkt7 + live mkt4).")
        _last_heartbeat = now

def run_cycle_prematch(tag):
    logging.info(f"Iniciando la carga de FIXTURE pre-match")
    rows = fetch_prematch_over25()
    ids = []
    try:
        ids = insert_matches(rows)
        logging.info(f"[{tag}] Se esta insertando a la BD : {len(ids)}")
    except Exception as e:
        logging.error(f"Error insert prematch: {e}")
    logging.info(f"[{tag}] Prematch Over/Under 2.5 procesados: {len(ids)}")
    #send_telegram(f"[{tag}] Prematch Over/Under 2.5 en DB: {len(ids)}")

def job_prematch():
    now = datetime.now(LIMA_TZ)
    now_plus5 = now + timedelta(hours=5)
    logging.info(f"[PREMATCH] Disparado job_prematch a las {now_plus5.strftime('%d/%m/%Y %H:%M:%S')}")
    run_cycle_prematch("CADA_15_MIN")
    logging.info(f"[PREMATCH] Ejecutado ciclo prematch a las {now_plus5.strftime('%d/%m/%Y %H:%M:%S')}")

def job_monitor():
    now = datetime.now(LIMA_TZ)
    now_plus5 = now + timedelta(hours=5)
    logging.info(f"[MONITOR] Disparado job_monitor a las {now_plus5.strftime('%d/%m/%Y %H:%M:%S')}")
    monitor_live_and_notify()
    heartbeat()
    logging.info(f"[MONITOR] Ejecutado monitoreo a las {now_plus5.strftime('%d/%m/%Y %H:%M:%S')}")

def run_threaded(job_func):
    """Ejecuta cada job en un hilo independiente"""
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def main():
    logging.info("Script iniciado (Sportmonks v3 football).")
    # Ejecutar prematch inmediatamente al inicio
    run_threaded(job_prematch)
    # 👇 cada tarea se dispara en paralelo
    schedule.every(15).minutes.do(run_threaded, job_prematch)
    schedule.every(2).minutes.do(run_threaded, job_monitor)

    while True:
        schedule.run_pending()
        time.sleep(1)

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
