#!/usr/bin/env python3
# script_prematch.py
# Detecta y registra Prematch Over 2.5 (guarda en ou_prematch_tickets)

import os, time, logging, requests, psycopg2
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Config (desde env o valores por defecto) ---
API_KEY = os.getenv("API_KEY", "TU_API_KEY")
API_URL = "https://api.the-odds-api.com/v4/sports/{sport}/odds/"
REGION = os.getenv("REGION", "us")
MARKETS = os.getenv("MARKETS", "totals,h2h")  # totales + h2h
PREMATCH_STAKE = float(os.getenv("PREMATCH_STAKE", 500))
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", 5))
SIMULATE = os.getenv("SIMULATE", "True").lower() in ("1","true","yes")

PG_HOST = os.getenv("PG_HOST", "dpg-d4b25nggjchc73f7d1o0-a")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB   = os.getenv("PG_DB", "surebet_db")
PG_USER = os.getenv("PG_USER", "surebet_db_user")
PG_PASS = os.getenv("PG_PASS", "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU")

TG_TOKEN = os.getenv("TG_TOKEN", "8252990863:AAEAN1qEh8xCwKT6-61rA1lp8nSHrHSFQLc")
TG_CHAT  = os.getenv("TG_CHAT", "1206397833")

# SPORTS: la lista que decidimos (OPCION 3). Puedes editar o pasar por env.
SPORTS = os.getenv("SPORTS", None)
if not SPORTS:
    SPORTS = [
        "soccer_netherlands_eredivisie",
        "soccer_netherlands_eerste_divisie",
        "soccer_germany_bundesliga",
        "soccer_germany_bundesliga2",
        "soccer_germany_3_liga",
        "soccer_sweden_allsvenskan",
        "soccer_sweden_superettan",
        "soccer_norway_eliteserien",
        "soccer_norway_first_division",
        "soccer_denmark_superliga",
        "soccer_japan_j_league",
        "soccer_japan_j2_league",
        "soccer_usa_mls",
        "soccer_switzerland_superleague",
        "soccer_austria_bundesliga",
        "soccer_turkey_super_league",
        "soccer_belgium_first_division_a",
        "soccer_finland_veikkausliiga",
        "soccer_portugal_primeira_liga",
        "soccer_ukraine_premier_league"
    ]
else:
    SPORTS = SPORTS.split(",")

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- DB helper ---
def get_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS, connect_timeout=10)

# --- Telegram ---
def send_tg(text):
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": TG_CHAT, "text": text})
    except Exception as e:
        logging.debug("Telegram error: %s", e)

# --- Fetch events for a sport (TheOddsAPI) ---
def fetch_events(sport):
    url = API_URL.format(sport=sport)
    params = {"apiKey": API_KEY, "regions": REGION, "markets": MARKETS, "oddsFormat": "decimal"}
    try:
        r = requests.get(url, params=params, timeout=20, verify=False)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error("fetch_events error %s: %s", sport, e)
        return []

# --- Insert prematch ticket ---
def insert_prematch_ticket(event, best_over_price, best_book):
    sql = """
    INSERT INTO ou_prematch_tickets
    (event_id, home_team, away_team, league, kickoff_time, market, odds, stake, expected_profit, created_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) RETURNING ticket_id
    """
    expected_profit = round(PREMATCH_STAKE * (best_over_price - 1), 2)
    conn = get_conn()
    cur = conn.cursor()
    try:
        kickoff = event.get("commence_time")
        cur.execute(sql, (
            event.get("id"),
            event.get("home_team"),
            event.get("away_team"),
            event.get("sport_title"),
            kickoff,
            "+2.5",
            best_over_price,
            PREMATCH_STAKE,
            expected_profit
        ))
        tid = cur.fetchone()[0]
        conn.commit()
        logging.info("Inserted prematch ticket %s %s vs %s @ %s (book %s)", tid, event.get("home_team"), event.get("away_team"), best_over_price, best_book)
        return tid
    except Exception as e:
        logging.error("insert_prematch_ticket error: %s", e)
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()

# --- Main loop: busca la mejor cuota over 2.5 por evento y la registra ---
def main():
    logging.info("Prematch scanner started. SIMULATE=%s", SIMULATE)
    while True:
        for sport in SPORTS:
            logging.info("Scanning sport: %s", sport)
            events = fetch_events(sport)
            for ev in events:
                # Requerimos market totals y bookies
                bks = ev.get("bookmakers", []) or []
                best_over = 0.0
                best_book = None
                best_outcome = None
                for bk in bks:
                    for m in bk.get("markets", []):
                        if m.get("key") != "totals":
                            continue
                        for out in m.get("outcomes", []):
                            # outcome: {name: "Over", price: 1.8, point: 2.5}
                            try:
                                if out.get("name","").lower().startswith("over") and float(out.get("point",0)) == 2.5:
                                    price = float(out.get("price", 0))
                                    if price > best_over:
                                        best_over = price
                                        best_book = bk.get("title")
                                        best_outcome = out
                            except:
                                continue
                # Si cumple threshold (evita insertar cuotas muy bajas)
                if best_over and best_over >= 1.7:
                    # Insertar ticket (si no existe ya)
                    # Evitamos duplicados simples consultando por event_id y kickoff cercano
                    try:
                        conn = get_conn()
                        cur = conn.cursor()
                        cur.execute("SELECT ticket_id FROM ou_prematch_tickets WHERE event_id=%s LIMIT 1", (ev.get("id"),))
                        exists = cur.fetchone()
                        cur.close()
                        conn.close()
                        if exists:
                            logging.debug("Prematch ticket already exists for event %s", ev.get("id"))
                            continue
                    except Exception as e:
                        logging.error("DB check error: %s", e)

                    tid = insert_prematch_ticket(ev, best_over, best_book)
                    msg = f"PREMATCH +2.5 DETECTADO\n{ev.get('home_team')} vs {ev.get('away_team')}\nCuota mejor Over2.5: {best_over} ({best_book})\nStake: {PREMATCH_STAKE}\nTicket: {tid}"
                    logging.info(msg)
                    send_tg(msg)
        logging.info("Sleeping %s minutes...", INTERVAL_MINUTES)
        time.sleep(INTERVAL_MINUTES * 60)

if __name__ == "__main__":
    main()
