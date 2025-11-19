#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
import psycopg2.extras

# ---------------------------------
# CONFIG (usando tus credenciales)
# ---------------------------------
SPORTRADAR_API_KEY = "xnCeW896IpZvYU3i8bSziTU9i4AthfjDn3Oa18Ie"
TG_TOKEN = "8252990863:AAEAN1qEh8xCwKT6-61rA1lp8nSHrHSFQLc"
TG_CHAT  = "1206397833"
PG_HOST = "dpg-d4b25nggjchc73f7d1o0-a"
PG_PORT = 5432
PG_DB   = "surebet_db"
PG_USER = "surebet_db_user"
PG_PASS = "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU"  # reemplaza por tu contraseña real

BASE_STAKE = 500.0
CURRENCY   = "PEN"
INSERT_HOUR = 10
MIN_PROFIT_PERCENT_BIG = 0.02
MIN_PROFIT_PERCENT_SMALL = 0.03
MAX_STAKE = 1000.0
POLL_SECONDS = 300  # 5 min

BIG_BOOKMAKERS = {"Bet365", "William Hill", "Pinnacle", "Unibet", "Betfair"}

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
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT, "text": message}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning(f"Telegram error: {r.text}")
    except Exception as e:
        logging.error(f"Telegram exception: {e}")

# ---------------------------------
# UTILS
# ---------------------------------
def safe_request(url, params=None, retries=3, delay=5):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=25)
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

def min_profit_by_bookmaker(bookmaker: str):
    return MIN_PROFIT_PERCENT_BIG if bookmaker in BIG_BOOKMAKERS else MIN_PROFIT_PERCENT_SMALL

def compute_surebet_stakes(over_odds: float, under_odds: float, stake: float):
    implied_sum = (1.0 / over_odds) + (1.0 / under_odds)
    s_over = stake * (1.0 / over_odds) / implied_sum
    s_under = stake * (1.0 / under_odds) / implied_sum
    profit_abs = (stake / implied_sum) - stake
    profit_pct = profit_abs / stake if stake > 0 else 0.0
    return implied_sum, s_over, s_under, profit_abs, profit_pct

def compute_hedge_amount(stake_over: float, over_odds: float, under_odds: float):
    return (stake_over * over_odds) / under_odds if under_odds and under_odds > 0 else 0.0

def log_alert(match_id, alert_type, message, profit_pct=None, profit_amount=None):
    q = """
    INSERT INTO alerts (match_id, alert_type, message, profit_percent, profit_amount, created_at)
    VALUES (%s, %s, %s, %s, %s, NOW())
    """
    db_exec(q, (match_id, alert_type, message, profit_pct, profit_amount), fetch=False)

def confirm_bet(event_id: str):
    q = "UPDATE matches SET bet_placed=TRUE, updated_at=NOW() WHERE event_id=%s"
    db_exec(q, (event_id,), fetch=False)
    logging.info(f"Apuesta confirmada manualmente para evento {event_id}")

# ---------------------------------
# PREMATCH: Odds Comparison Pre-Match v2 (soccer)
# ---------------------------------
def fetch_prematch_over25():
    url = f"https://api.sportradar.com/oddscomparison-prematch/trial/v2/en/sports.json?api_key={SPORTRADAR_API_KEY}"
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
                    if mname in {"total", "totals", "over/under"}:
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
        res = db_exec(q, vals, fetch=True)
        if res:
            ids.append(res[0]["id"])
    return ids

# ---------------------------------
# LIVE: Odds Comparison Live v2 (soccer)
# ---------------------------------
def fetch_live_under25():
    url = f"https://api.sportradar.com/oddscomparison-live/trial/v2/en/sports.json?api_key={SPORTRADAR_API_KEY}"
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
                status = ev.get("sport_event_status", {}) or {}
                minute = int(status.get("match_time", 0) or 0)
                score_home = int(status.get("home_score", 0) or 0)
                score_away = int(status.get("away_score", 0) or 0)

                # Mejor Under 2.5 live entre todos los bookmakers
                best_odds = None
                best_bookmaker = None
                for market in ev.get("markets", []):
                    mname = (market.get("name") or "").lower()
                    if mname in {"total", "totals", "over/under"}:
                        for outcome in market.get("outcomes", []):
                            name = (outcome.get("name") or "").lower()
                            total = outcome.get("total")
                            odds = outcome.get("odds")
                            bookmaker = outcome.get("bookmaker")
                            if name.startswith("under") and total == 2.5 and valid_odds(odds):
                                oddsv = float(odds)
                                if best_odds is None or oddsv > best_odds:
                                    best_odds = oddsv
                                    best_bookmaker = bookmaker

                if best_odds:
                    results.append({
                        "event_id": event_id,
                        "home_team": home,
                        "away_team": away,
                        "minute": minute,
                        "score_home": score_home,
                        "score_away": score_away,
                        "bookmaker": best_bookmaker or "Sportradar",
                        "odds": best_odds
                    })
    return results

# ---------------------------------
# MONITOREO
# ---------------------------------
_last_heartbeat = None

def heartbeat():
    global _last_heartbeat
    now = datetime.now()
    if _last_heartbeat is None or (now - _last_heartbeat) >= timedelta(minutes=30):
        msg = "Heartbeat: script activo y monitoreando."
        send_telegram(msg)
        log_alert(None, "heartbeat", msg, None, None)
        _last_heartbeat = now

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
        q = "SELECT id, odds FROM matches WHERE event_id=%s AND bet_placed=TRUE AND selection='over_2.5' LIMIT 1"
        res = db_exec(q, (fixture_id,), fetch=True)
        if not res:
            continue
        match_id_db = res[0]["id"]
        over_odds_prematch = float(res[0]["odds"])

        # CASHOUT temprano si hay gol <= 20'
        if minute <= 20 and total_goals >= 1:
            msg = f"Gol temprano en {home} vs {away} (min {minute}, {score_home}-{score_away}). CASHOUT sugerido."
            send_telegram(msg)
            log_alert(match_id_db, "cashout", msg, None, None)
            continue

        # Mejor Under 2.5 live del feed
        under_live = float(ev["odds"])
        bookmaker_live = ev["bookmaker"]

        # Estrategia: a partir de 20' y 0-0 → evaluar surebet o cobertura
        if minute >= 20 and total_goals == 0 and valid_odds(over_odds_prematch) and valid_odds(under_live):
            implied_sum, s_over_base, s_under_base, profit_abs_base, profit_pct_base = compute_surebet_stakes(
                over_odds_prematch, under_live, BASE_STAKE
            )

            if implied_sum < 1.0:
                min_profit = min_profit_by_bookmaker(bookmaker_live or "")
                if profit_pct_base >= min_profit:
                    # Stake dinámico conservador
                    scale = max(1.0, (profit_pct_base / min_profit) ** 0.5)
                    dynamic_stake = min(MAX_STAKE, BASE_STAKE * scale)

                    _, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
                        over_odds_prematch, under_live, dynamic_stake
                    )

                    msg = (
                        f"Surebet {home} vs {away} (min {minute}, {score_home}-{score_away}).\n"
                        f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live}).\n"
                        f"Stake: {dynamic_stake:.2f} {CURRENCY} ⇒ Over: {s_over:.2f}, Under: {s_under:.2f}.\n"
                        f"Profit esperado: {profit_abs:.2f} {CURRENCY} ({profit_pct*100:.2f}%)."
                    )
                    send_telegram(msg)
                    log_alert(match_id_db, "surebet", msg, profit_pct, profit_abs)
                else:
                    msg = f"Surebet ignorado {home} vs {away}: profit {profit_pct_base*100:.2f}% < mínimo {min_profit*100:.2f}%."
                    send_telegram(msg)
                    log_alert(match_id_db, "surebet_ignorado", msg, profit_pct_base, None)
            else:
                # No hay surebet, proponer cobertura del Over con Under
                hedge_amount = compute_hedge_amount(BASE_STAKE, over_odds_prematch, under_live)
                msg = (
                    f"Sin surebet {home} vs {away} (min {minute}). Cobertura sugerida: apostar {hedge_amount:.2f} {CURRENCY} "
                    f"al UNDER 2.5 @ {under_live} en {bookmaker_live}."
                )
                send_telegram(msg)
                log_alert(match_id_db, "cobertura", msg, None, None)

# ---------------------------------
# CICLO PRINCIPAL
# ---------------------------------
def main():
    logging.info("Script iniciado correctamente (OddsComparison v2, soccer).")
    last_insert_date = None
    while True:
        now = datetime.now()
        try:
            # Inserción diaria pre-match (mejor Over 2.5 por evento)
            if (last_insert_date is None or last_insert_date != now.date()) and now.hour == INSERT_HOUR:
                rows = fetch_prematch_over25()
                ids = insert_matches(rows)
                logging.info(f"Insertados/actualizados {len(ids)} partidos pre-match (Over 2.5).")
                send_telegram(f"Insertados/actualizados {len(ids)} partidos pre-match en DB.")
                last_insert_date = now.date()
        except Exception as e:
            logging.error(f"Error en inserción diaria: {e}")

        try:
            monitor_live_and_notify()
            heartbeat()
        except Exception as e:
            logging.error(f"Error en monitoreo: {e}")

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
