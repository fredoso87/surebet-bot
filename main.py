import requests
import psycopg2
import time
from datetime import datetime, timezone
from flask import Flask
import threading

# ======================================
# 🔧 CONFIGURACIÓN DIRECTA (SIN ENV VARS)
# ======================================

ODDS_API_KEY = "2a5684033edc1582d1e7befd417fda79"  # TheOddsAPI
SPORTS = ["soccer", "basketball", "tennis"]
REGION = "eu"
PROFIT_THRESHOLD = 1.0
INTERVAL_MINUTES = 10

# PostgreSQL (Render)
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
                        surebets.append({
                            "sport": sport,
                            "team1": home,
                            "team2": away,
                            "market": ev.get("sport_title", ""),
                            "profit_percent": round(profit, 2),
                            "details": best_odds,
                            "found_time": datetime.now(timezone.utc)
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
                cursor.execute("""
                    INSERT INTO surebets (
                        sport, team1, team2, market, profit_percent,
                        bookmaker1, odd1, bookmaker2, odd2, found_time
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    sb["sport"],
                    sb["team1"],
                    sb["team2"],
                    sb["market"],
                    sb["profit_percent"],
                    list(sb["details"].values())[0]["bookmaker"],
                    list(sb["details"].values())[0]["price"],
                    list(sb["details"].values())[1]["bookmaker"],
                    list(sb["details"].values())[1]["price"],
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
# 🌐 FLASK SERVER (PARA RENDER WEB SERVICE)
# ======================================

app = Flask(__name__)

@app.get("/")
def home():
    return "Surebet bot running on Render"

def start_bot():
    while True:
        main()
        print(f"⏳ Esperando {INTERVAL_MINUTES} minutos antes del próximo ciclo...\n")
        time.sleep(INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    # Iniciar bot en segundo plano
    threading.Thread(target=start_bot, daemon=True).start()

    # Servidor web que Render necesita
    app.run(host="0.0.0.0", port=10000)
