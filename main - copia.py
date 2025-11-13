import requests
import oracledb
from datetime import datetime, timezone
import time
import urllib3

# Desactiva advertencias SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================================
# CONFIGURACIÓN
# ================================
ODDS_API_KEY = "2a5684033edc1582d1e7befd417fda79"  # <-- coloca aquí tu API key real de https://the-odds-api.com
ORACLE_USER = "system"
ORACLE_PASS = "Indra123"
ORACLE_DSN = "127.0.0.1:1521/ORCL"
PROFIT_THRESHOLD = 1.0
INTERVAL_MINUTES = 5
REGION = "eu"

SPORTS = [
    {"name": "soccer", "markets": ["h2h", "totals"]},
    {"name": "tennis", "markets": ["h2h"]},
    {"name": "basketball", "markets": ["h2h", "totals"]}
]

# ================================
# FUNCIÓN: Obtener cuotas desde OddsAPI con control SSL
# ================================
def get_odds_from_oddsapi(sport, markets):
    results = []
    for market in markets:
        url = (
            f"https://api.the-odds-api.com/v4/sports/{sport}/odds/"
            f"?regions={REGION}&markets={market}&oddsFormat=decimal&apiKey={ODDS_API_KEY}"
        )
        try:
            # Intentar conexión normal
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.SSLError:
            # Si falla SSL, reintentar sin verificación
            print(f"⚠️ Advertencia SSL — usando conexión sin verificación para {url}")
            resp = requests.get(url, timeout=30, verify=False)
        except Exception as e:
            print(f"⚠️ Error al obtener cuotas de {sport} ({market}): {e}")
            continue

        try:
            data = resp.json()
            for match in data:
                match["market_type"] = market
            results.extend(data)
        except Exception as e:
            print(f"⚠️ Error al procesar JSON de {sport} ({market}): {e}")
    return results


# ================================
# FUNCIÓN: Calcular apuestas
# ================================
def calc_stakes(odds):
    inv_sum = sum(1 / o for o in odds)
    total = 100
    stakes = [(total / (o * inv_sum)) for o in odds]
    profits = [stakes[i] * odds[i] for i in range(len(odds))]
    gain = min(profits) - total
    profit_percent = round((gain / total) * 100, 3)
    return stakes, profit_percent


# ================================
# FUNCIÓN: Detectar arbitrajes
# ================================
def find_surebets(data, sport):
    surebets = []
    for ev in data:
        try:
            home = ev.get("home_team", "")
            away = ev.get("away_team", "")
            match_id = ev.get("id", "")
            market = ev.get("market_type", "")
            bookmakers = ev.get("bookmakers", [])
            if not bookmakers:
                continue

            best_home = best_draw = best_away = 0
            bm_home = bm_draw = bm_away = ""

            for bm in bookmakers:
                key = bm.get("key", "")
                markets = bm.get("markets", [])
                if not markets:
                    continue

                outcomes = markets[0].get("outcomes", [])
                for out in outcomes:
                    name = out.get("name", "")
                    price = out.get("price", 0)
                    if not price:
                        continue

                    if market == "h2h":
                        if name.lower() in [home.lower(), "home"]:
                            if price > best_home:
                                best_home, bm_home = price, key
                        elif name.lower() in [away.lower(), "away"]:
                            if price > best_away:
                                best_away, bm_away = price, key
                        elif name.lower() == "draw":
                            if price > best_draw:
                                best_draw, bm_draw = price, key

                    elif market == "totals":
                        if "over" in name.lower() and price > best_home:
                            best_home, bm_home = price, key
                        elif "under" in name.lower() and price > best_away:
                            best_away, bm_away = price, key

            odds = []
            if best_home:
                odds.append(best_home)
            if market == "h2h" and best_draw:
                odds.append(best_draw)
            if best_away:
                odds.append(best_away)

            if len(odds) >= 2:
                stakes, profit_percent = calc_stakes(odds)
                if profit_percent > PROFIT_THRESHOLD:
                    recommended = "1" if best_home >= max(best_away, best_draw or 0) else (
                        "2" if best_away >= max(best_home, best_draw or 0) else "X"
                    )
                    surebets.append({
                        "sport": sport,
                        "match_id": match_id,
                        "home_team": home,
                        "away_team": away,
                        "market": market,
                        "odd_home": best_home,
                        "odd_draw": best_draw if market == "h2h" else None,
                        "odd_away": best_away,
                        "bm_home": bm_home,
                        "bm_draw": bm_draw,
                        "bm_away": bm_away,
                        "profit_percent": profit_percent,
                        "stake_home": stakes[0],
                        "stake_draw": stakes[1] if market == "h2h" and len(stakes) == 3 else None,
                        "stake_away": stakes[-1],
                        "recommended_result": recommended,
                        "found_time": datetime.now(timezone.utc)
                    })
        except Exception as e:
            print(f"⚠️ Error procesando evento {ev.get('id')}: {e}")
    return surebets


# ================================
# FUNCIÓN: Insertar en Oracle
# ================================
def insert_surebets_to_oracle(surebets):
    if not surebets:
        return

    try:
        conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)
        cursor = conn.cursor()

        sql = """
        INSERT INTO surebets (
            sport, match_id, home_team, away_team, market,
            odd_home, odd_draw, odd_away,
            bm_home, bm_draw, bm_away,
            profit_percent, stake_home, stake_draw, stake_away,
            recommended_result, found_time
        ) VALUES (
            :sport, :match_id, :home_team, :away_team, :market,
            :odd_home, :odd_draw, :odd_away,
            :bm_home, :bm_draw, :bm_away,
            :profit_percent, :stake_home, :stake_draw, :stake_away,
            :recommended_result, :found_time
        )
        """

        for sb in surebets:
            cursor.execute(sql, sb)

        conn.commit()
        print(f"✅ {len(surebets)} surebets insertadas correctamente en Oracle.")
    except Exception as e:
        print(f"⚠️ Error al insertar en Oracle: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ================================
# CICLO PRINCIPAL
# ================================
def main():
    print("\n🚀 Iniciando bot de arbitraje (OddsAPI + Oracle + SSL safe)")
    print("Deportes configurados:")
    for s in SPORTS:
        print(f"  - {s['name']} ({', '.join(s['markets'])})")

    while True:
        for s in SPORTS:
            sport = s["name"]
            markets = s["markets"]
            print(f"\n[{datetime.now()}] 🔍 Analizando {sport.upper()}...")

            data = get_odds_from_oddsapi(sport, markets)
            print(f"Eventos obtenidos: {len(data)}")

            surebets = find_surebets(data, sport)
            print(f"Surebets detectadas: {len(surebets)}")

            for sb in surebets:
                print(f"🏆 {sb['home_team']} vs {sb['away_team']} ({sb['market']}) | {sb['profit_percent']}%")
            insert_surebets_to_oracle(surebets)

        print(f"\n⏳ Esperando {INTERVAL_MINUTES} minutos antes del próximo ciclo...\n")
        time.sleep(INTERVAL_MINUTES * 60)


# ================================
# EJECUCIÓN
# ================================
if __name__ == "__main__":
    main()
