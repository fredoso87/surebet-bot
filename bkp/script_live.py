#!/usr/bin/env python3
# script_live.py
# Monitorea tickets prematch abiertos, busca -2.5 en vivo y crossbook OU

import os, time, logging, requests, psycopg2
from datetime import datetime, timezone
from dateutil import parser as dtparser
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Config ---
API_KEY = os.getenv("API_KEY", "TU_API_KEY")
API_URL = "https://api.the-odds-api.com/v4/sports/{sport}/odds/"
REGION = os.getenv("REGION", "us")
MARKETS = os.getenv("MARKETS", "totals,h2h")

PG_HOST = os.getenv("PG_HOST", "dpg-d4b25nggjchc73f7d1o0-a")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB   = os.getenv("PG_DB", "surebet_db")
PG_USER = os.getenv("PG_USER", "surebet_db_user")
PG_PASS = os.getenv("PG_PASS", "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU")

TG_TOKEN = os.getenv("TG_TOKEN", "8252990863:AAEAN1qEh8xCwKT6-61rA1lp8nSHrHSFQLc")
TG_CHAT  = os.getenv("TG_CHAT", "1206397833")

PREMATCH_STAKE = float(os.getenv("PREMATCH_STAKE", 500))
LIVE_POLL_INTERVAL = int(os.getenv("LIVE_POLL_INTERVAL", 30))
MIN_PROFIT_SOL = float(os.getenv("MIN_PROFIT_SOL", 5.0))
MIN_PROFIT_PCT = float(os.getenv("MIN_PROFIT_PCT", 0.01))
SIMULATE = os.getenv("SIMULATE", "True").lower() in ("1","true","yes")

# Which sports to request for lookup (to reduce calls, you can pass a narrow list)
SPORTS = os.getenv("SPORTS", None)
if not SPORTS:
    SPORTS = ["soccer_netherlands_eredivisie","soccer_germany_bundesliga","soccer_usa_mls","soccer_spain_la_liga"]
else:
    SPORTS = SPORTS.split(",")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- DB ---
def get_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS, connect_timeout=10)

# --- Telegram ---
def send_tg(text):
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": TG_CHAT, "text": text})
    except Exception as e:
        logging.debug("Telegram error: %s", e)

# --- Fetch all open prematch tickets (status open OR created recently) ---
def fetch_open_tickets():
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Asumimos tabla ou_prematch_tickets; si tienes status, filtra por status='open'
        cur.execute("""
            SELECT ticket_id, event_id, home_team, away_team, league, kickoff_time, odds, stake, created_at
            FROM ou_prematch_tickets
            WHERE created_at >= NOW() - INTERVAL '2 days'
        """)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        result = [dict(zip(cols, r)) for r in rows]
        return result
    except Exception as e:
        logging.error("fetch_open_tickets error: %s", e)
        return []
    finally:
        cur.close()
        conn.close()

# --- Fetch fresh event by scanning sports (TheOddsAPI lacks single-event endpoint reliably) ---
def find_event_by_id(event_id):
    for sport in SPORTS:
        url = API_URL.format(sport=sport)
        params = {"apiKey": API_KEY, "regions": REGION, "markets": MARKETS, "oddsFormat": "decimal"}
        try:
            r = requests.get(url, params=params, timeout=20, verify=False)
            if r.status_code != 200:
                continue
            for ev in r.json():
                if ev.get("id") == event_id:
                    return ev
        except Exception as e:
            logging.debug("find_event_by_id error: %s", e)
    return None

# --- util compute stakes for 2-way OU ---
def compute_stakes(over_price, under_price, total_stake):
    try:
        inv_over = 1.0 / float(over_price)
        inv_under = 1.0 / float(under_price)
        idx = inv_over + inv_under
        if idx >= 1.0:
            return None
        stake_over = (total_stake * inv_over) / idx
        stake_under = (total_stake * inv_under) / idx
        payout_over = stake_over * over_price
        payout_under = stake_under * under_price
        profit = round(min(payout_over, payout_under) - total_stake, 2)
        return round(stake_over,2), round(stake_under,2), profit
    except Exception as e:
        logging.error("compute_stakes error: %s", e)
        return None

# --- Insert live opportunity (hedge) ---
def insert_live_opportunity(ticket_id, event_id, minute_est, under_price, stake_under, profit):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO ou_live_opportunities (ticket_id, event_id, current_minute, live_under_line, live_under_odds, hedge_stake, guaranteed_profit, detected_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (ticket_id, event_id, minute_est, "-2.5", under_price, stake_under, profit))
        conn.commit()
    except Exception as e:
        logging.error("insert_live_opportunity error: %s", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# --- Insert crossbook surebet ---
def insert_crossbook(event_id, home, away, league, minute_est, over_price, over_book, under_price, under_book, stake_o, stake_u, total_stake, profit):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO ou_surebets_crossbook
            (event_id, home_team, away_team, league, minute, line_over, odds_over, bookmaker_over, line_under, odds_under, bookmaker_under, stake_over, stake_under, total_stake, guaranteed_profit, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (event_id, home, away, league, minute_est, "+2.5", over_price, over_book, "-2.5", under_price, under_book, stake_o, stake_u, total_stake, profit))
        conn.commit()
    except Exception as e:
        logging.error("insert_crossbook error: %s", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# --- Find cross-book pairs for a given event object ---
def find_crossbook_pairs(event, target_line=2.5):
    pairs = []
    for bk in event.get("bookmakers", []):
        book = bk.get("title")
        for m in bk.get("markets", []):
            if m.get("key") != "totals":
                continue
            for out in m.get("outcomes", []):
                try:
                    name = out.get("name","").lower()
                    point = float(out.get("point", 0))
                    price = float(out.get("price", 0))
                except:
                    continue
                if point != target_line:
                    continue
                if name.startswith("over"):
                    pairs.append(("over", price, book))
                elif name.startswith("under"):
                    pairs.append(("under", price, book))
    # Build combos cross-book: choose one over from book A and one under from book B != A
    combos = []
    overs = [p for p in pairs if p[0]=="over"]
    unders = [p for p in pairs if p[0]=="under"]
    for o in overs:
        for u in unders:
            if o[2] == u[2]:
                continue
            combos.append({"over_price": o[1], "over_book": o[2], "under_price": u[1], "under_book": u[2]})
    return combos

# --- Main monitor loop ---
def main():
    logging.info("Live monitor started. SIMULATE=%s", SIMULATE)
    while True:
        tickets = fetch_open_tickets()
        logging.info("Open tickets to check: %d", len(tickets))
        for t in tickets:
            event_id = t["event_id"]
            ev = find_event_by_id(event_id)
            if not ev:
                logging.debug("Event %s not found in API snapshots.", event_id)
                continue

            # Heurística: consider event live if commence_time <= now OR bookmakers have last_update
            is_live = False
            try:
                kickoff = t.get("kickoff_time")
                if kickoff:
                    try:
                        dt_k = dtparser.isoparse(str(kickoff))
                        if dt_k.tzinfo is None:
                            dt_k = dt_k.replace(tzinfo=timezone.utc)
                        if dt_k <= datetime.now(timezone.utc):
                            is_live = True
                    except:
                        pass
            except:
                pass
            # fallback: if any bookmaker has last_update field, assume live feed
            for bk in ev.get("bookmakers", []):
                if bk.get("last_update"):
                    is_live = True
                    break

            if not is_live:
                logging.debug("Event %s not live yet.", event_id)
                continue

            # Buscar combos cross-book
            combos = find_crossbook_pairs(ev, target_line=2.5)
            if not combos:
                logging.debug("No crossbook combos for event %s", event_id)
                continue

            # Evaluar combos y elegir mejor profit
            best = None
            best_profit = -9999
            for c in combos:
                res = compute_stakes(c["over_price"], c["under_price"], PREMATCH_STAKE)
                if not res:
                    continue
                so, su, profit = res
                if profit >= MIN_PROFIT_SOL and (profit / PREMATCH_STAKE) >= MIN_PROFIT_PCT:
                    if profit > best_profit:
                        best_profit = profit
                        best = dict(c, stake_over=so, stake_under=su, profit=profit)
            if best:
                # Insert crossbook surebet
                minute_est = 0
                try:
                    minute_est = 0  # we don't have real minute; if you have scoreboard, put it here
                except:
                    minute_est = 0
                insert_crossbook(ev.get("id"), ev.get("home_team"), ev.get("away_team"), ev.get("sport_title"),
                                 minute_est, best["over_price"], best["over_book"], best["under_price"], best["under_book"],
                                 best["stake_over"], best["stake_under"], PREMATCH_STAKE, best["profit"])
                # Insert live opportunity (hedge) row too — stake_under is what we'd place in live book
                insert_live_opportunity(t["ticket_id"], ev.get("id"), minute_est, best["under_price"], best["stake_under"], best["profit"])
                msg = (f"✅ SUREBET CROSS-BOOK DETECTADA\n{ev.get('home_team')} vs {ev.get('away_team')}\n"
                       f"Over 2.5 @ {best['over_price']} ({best['over_book']})\n"
                       f"Under 2.5 @ {best['under_price']} ({best['under_book']})\n"
                       f"Stake Over: S/ {best['stake_over']} | Stake Under: S/ {best['stake_under']}\n"
                       f"Profit aproximado: S/ {best['profit']}")
                logging.info(msg)
                send_tg(msg)
            else:
                logging.debug("No profitable crossbook for event %s", event_id)

        logging.info("Sleeping %s seconds before next live poll...", LIVE_POLL_INTERVAL)
        time.sleep(LIVE_POLL_INTERVAL)

if __name__ == "__main__":
    main()
