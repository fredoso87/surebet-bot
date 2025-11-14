import requests
import psycopg2
from datetime import datetime, timezone
from flask import Flask

# ======================================
# 🔧 CONFIGURACIÓN
# ======================================
ODDS_API_KEY = "2a5684033edc1582d1e7befd417fda79"
SPORTS = ["soccer", "basketball", "tennis"]
REGION = "eu"
PROFIT_THRESHOLD = 1.0
BET_AMOUNT = 500  # soles
# INTERVAL_MINUTES ya no se usa, el ciclo se ejecuta por request

PG_USER = "surebet_db_user"
PG_PASS = "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU"
PG_HOST = "dpg-d4b25nggjchc73f7d1o0-a"
PG_PORT = "5432"
PG_DB = "surebet_db"

# ======================================
# 🔍 FUNCIONES
# ======================================

def get_odds_from_oddsapi(sport, markets):
    results = []
    for market in markets:
        url = (
            f"https://api.the-odds-api.com/v4/sports/{sport}/odds/"
            f"?regions={REGION}&markets={market}&oddsFormat=decimal&apiKey={ODDS_API_KEY}"
        )
        try:
            response = requests.get(url, timeout=30, verify=False)
            if response.status_code == 200:
                data = response.json()
                for ev in data:
                    ev["market_type"] = market
                results.extend(data)
            else:
                print(f"⚠️ Error HTTP {response.status_code} para {sport} ({market})")
        except Exception as e:
            print(f"⚠️ Error al obtener cuotas de {sport} ({market}): {e}")
    return results

def find_surebets(events):
    surebets = []
    for ev in events:
        try:
            home = ev.get("home_team", "")
            away = ev.get("away_team", "")
            sport = ev.get("sport_key", "unknown")
            market_type = ev.get("market_type", "")
            commence_time_str = ev.get("commence_time", None)

            # Determinar live/programado
            if commence_time_str:
                event_time = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
                live_status = "live" if event_time <= datetime.now(timezone.utc) else "scheduled"
            else:
                live_status = "unknown"

            bookmakers = ev.get("bookmakers", [])
            best_odds = {}

            for bm in bookmakers:
                bm_name = bm.get("title", "")
                markets = bm.get("markets", [])
                for market in markets:
                    for outcome in market.get("outcomes", []):
                        name = outcome.get("name", "")
                        price = float(outcome.get("price", 0))
                        if name not in best_odds or price > best_odds[name]["price"]:
                            best_odds[name] = {"price": price, "bookmaker": bm_name}

            if len(best_odds) >= 2:
                inv_sum = sum(1 / v["price"] for v in best_odds.values())
                if inv_sum < 1:
                    profit = (1 / inv_sum - 1) * 100

                    if profit >= PROFIT_THRESHOLD:
                        # Calcular apuestas proporcionales
                        outcomes = list(best_odds.keys())
                        bet_team1 = round(BET_AMOUNT / best_odds[outcomes[0]]["price"] / inv_sum, 2)
                        bet_team2 = round(BET_AMOUNT / best_odds[outcomes[1]]["price"] / inv_sum, 2)

                        surebets.append({
                            "sport": sport,
                            "team1": home,
                            "team2": away,
                            "market": market_type,
                            "profit_percent": round(profit, 2),
                            "details": best_odds,
                            "found_time": datetime.now(timezone.utc),
                            "bet_team1": bet_team1,
                            "bet_team2": bet_team2,
                            "live_status": live_status
                        })

        except Exception as e:
            print(f"⚠️ Error procesando evento: {e}")

    return surebets

def insert_surebets_postgres(surebets):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS,
            port=PG_PORT
        )
        cursor = conn.cursor()

        for sb in surebets:
            try:
                outcomes = list(sb["details"].values())
                cursor.execute("""
                    INSERT INTO surebets (
                        sport, team1, team2, market, profit_percent,
                        bookmaker1, odd1, bookmaker2, odd2,
                        bet_team1, bet_team2, live_status, found_time
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    sb["sport"],
                    sb["team1"],
                    sb["team2"],
                    sb["market"],
                    sb["profit_percent"],
                    outcomes[0]["bookmaker"],
                    outcomes[0]["price"],
                    outcomes[1]["bookmaker"],
                    outcomes[1]["price"],
                    sb["bet_team1"],
                    sb["bet_team2"],
                    sb["live_status"],
                    sb["found_time"]
                ))
            except Exception as e:
                print(f"⚠️ Error al insertar registro: {e}")

        conn.commit()
        print(f"✅ {len(surebets)} arbitrajes insertados correctamente en PostgreSQL.")

    except Exception as e:
        print(f"⚠️ Error PostgreSQL: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    print(f"\n[{datetime.now()}] 🔍 Iniciando búsqueda de surebets...")
    all_surebets = []

    for sport in SPORTS:
        print(f"[{datetime.now()}] Analizando {sport.upper()}...")
        events = get_odds_from_oddsapi(sport, ["h2h", "totals"])
        surebets = find_surebets(events)
        if surebets:
            print(f"💰 {len(surebets)} surebets encontradas en {sport}.")
            all_surebets.extend(surebets)
        else:
            print(f"— No se encontraron surebets en {sport}.")

    if all_surebets:
        insert_surebets_postgres(all_surebets)
    else:
        print("Sin resultados rentables este ciclo.")

# ======================================
# 🌐 FLASK SERVER (PARA RENDER WEB SERVICE GRATIS)
# ======================================

app = Flask(__name__)

@app.get("/")
def home():
    main()  # ejecuta el ciclo al abrir la URL
    print("acabo ciclo.")
    return "Surebet bot running on Render"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
