import requests
import psycopg2
import logging
from datetime import datetime

# ------------------------
# Configuración logging
# ------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# ------------------------
# Configuración DB
# ------------------------
PG_HOST = "dpg-d4b25nggjchc73f7d1o0-a"
PG_PORT = 5432
PG_DB = "surebet_db"
PG_USER = "surebet_db_user"
PG_PASS = "bphDIBxCdPckefLT0SIOpB2WCEtiCCMU"

# ------------------------
# Configuración API
# ------------------------
API_KEY = "2a5684033edc1582d1e7befd417fda79"
API_URL = "https://api.the-odds-api.com/v4/sports/{sport}/odds/"

# ------------------------
# Monto a apostar
# ------------------------
TOTAL_INVERSIÓN = 500  # S/. 500 por cada surebet


# ========================
# Conexión a PostgreSQL
# ========================
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cursor = conn.cursor()
    logging.info("Conexión a PostgreSQL exitosa.")
except Exception as e:
    logging.error(f"Error conectando a PostgreSQL: {e}")
    raise


# ========================
# FUNCIONES PRINCIPALES
# ========================

def obtener_eventos(sport_key):
    """Obtiene eventos de la API."""
    url = API_URL.format(sport=sport_key)
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": "h2h,totals",
        "oddsFormat": "decimal"
    }

    try:
        r = requests.get(url, params=params, verify=False)
        r.raise_for_status()
        eventos = r.json()
        logging.info(f"{len(eventos)} eventos obtenidos de {sport_key}")
        return eventos
    except Exception as e:
        logging.error(f"Error obteniendo eventos de {sport_key}: {e}")
        return []


def calcular_surebet(outcomes, total_inversion):
    """
    Calcula stakes para una surebet.
    Funciona con 2 o 3 outcomes.
    Retorna (stakes, profit)
    """

    try:
        inv_sum = sum([1 / o["price"] for o in outcomes])
        stakes = [(total_inversion / o["price"]) / inv_sum for o in outcomes]

        payouts = [stakes[i] * outcomes[i]["price"] for i in range(len(outcomes))]
        profit = round(min(payouts) - total_inversion, 2)

        stakes = [round(s, 2) for s in stakes]

        return stakes, profit

    except Exception as e:
        logging.error(f"Error calculando surebet: {e}")
        return None, None


def insertar_surebet(evento, market, outcomes, stakes, profit):
    """Inserta surebet en la BD."""

    sql = """
    INSERT INTO surebets (
        event_id, sport_key, sport_title, commence_time,
        home_team, away_team, market_type,
        outcome1_name, outcome1_odds, stake1,
        outcome2_name, outcome2_odds, stake2,
        outcome3_name, outcome3_odds, stake3,
        total_stake, profit, status
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,
              %s,%s,%s,
              %s,%s,%s,
              %s,%s,%s)
    """

    valores = (
        evento["id"], evento["sport_key"], evento["sport_title"], evento["commence_time"],
        evento.get("home_team", ""), evento.get("away_team", ""), market["key"],

        outcomes[0]["name"], outcomes[0]["price"], stakes[0],
        outcomes[1]["name"], outcomes[1]["price"], stakes[1],

        outcomes[2]["name"] if len(outcomes) == 3 else None,
        outcomes[2]["price"] if len(outcomes) == 3 else None,
        stakes[2] if len(outcomes) == 3 else None,

        TOTAL_INVERSIÓN, profit, "scheduled"
    )

    try:
        cursor.execute(sql, valores)
        conn.commit()
        logging.info(f"Surebet insertada correctamente. Profit: S/ {profit}")
    except Exception as e:
        logging.error(f"Error insertando en BD: {e}")


def procesar_evento(evento):
    """Analiza mercados y detecta surebets."""

    if "bookmakers" not in evento:
        return

    for bookmaker in evento["bookmakers"]:
        for market in bookmaker["markets"]:

            outcomes = market["outcomes"]

            # Filtrar solo mercados válidos para surebets:
            if len(outcomes) not in (2, 3):
                continue

            stakes, profit = calcular_surebet(outcomes, TOTAL_INVERSIÓN)

            if stakes is None:
                continue

            # Solo insertar si realmente hay ganancia (>0)
            if profit > 0:
                insertar_surebet(evento, market, outcomes, stakes, profit)


# ========================
# MAIN
# ========================
if __name__ == "__main__":

    deportes = [
    "basketball_ncaab",
    "basketball_euroleague",
    "tennis",
    "soccer_epl",
    "soccer_spain_la_liga",
    "soccer_italy_serie_a",
    "soccer_germany_bundesliga",
    "soccer_france_ligue_one",
    "soccer_brazil_campeonato",
    "soccer_conmebol_libertadores",
    "soccer_conmebol_sudamericana",
    "soccer_usa_mls"
]


    for sport in deportes:
        eventos = obtener_eventos(sport)

        for evento in eventos:
            procesar_evento(evento)

    cursor.close()
    conn.close()
    logging.info("Proceso finalizado.")
