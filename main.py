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
API_KEY = "tu_api_key"
API_URL = "https://api.the-odds-api.com/v4/sports/{sport}/odds/"

# ------------------------
# Monto a apostar
# ------------------------
APUESTA_SOLES = 500

# ------------------------
# Conexión a PostgreSQL
# ------------------------
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

# ------------------------
# Funciones
# ------------------------
def obtener_eventos(sport_key):
    """Obtiene eventos de la API de odds"""
    url = API_URL.format(sport=sport_key)
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": "totals,h2h",
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

def calcular_apuestas(outcomes, monto_total):
    """Calcula cuánto apostar a cada resultado según la cuota"""
    try:
        total_inverse = sum([1/o['price'] for o in outcomes])
        apuestas = [round((monto_total / o['price']) / total_inverse, 2) for o in outcomes]
        return apuestas
    except Exception as e:
        logging.error(f"Error calculando apuestas: {e}")
        return [0 for _ in outcomes]

def insertar_surebet(evento):
    """Inserta un evento de surebet en PostgreSQL"""
    status = "live" if 'live' in evento else "scheduled"
    outcomes = evento['bookmakers'][0]['markets'][0]['outcomes']
    apuestas = calcular_apuestas(outcomes, APUESTA_SOLES)

    sql = """
    INSERT INTO surebets (
        event_id, sport_key, sport_title, commence_time, home_team, away_team,
        market_type, status, apuesta_total, resultado1, cuota1, apuesta1, resultado2, cuota2, apuesta2
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    valores = (
        evento['id'],
        evento['sport_key'],
        evento['sport_title'],
        evento['commence_time'],
        evento['home_team'],
        evento['away_team'],
        evento['market_type'],
        status,
        APUESTA_SOLES,
        outcomes[0]['name'],
        outcomes[0]['price'],
        apuestas[0],
        outcomes[1]['name'],
        outcomes[1]['price'],
        apuestas[1]
    )

    logging.info(f"Insertando evento: {valores}")
    try:
        cursor.execute(sql, valores)
        conn.commit()
        logging.info("Evento insertado correctamente.")
    except Exception as e:
        logging.error(f"Error al insertar registro: {e}")

# ------------------------
# Main
# ------------------------
if __name__ == "__main__":
    deportes = ["basketball_ncaab", "basketball_euroleague", "tennis"]
    for sport in deportes:
        logging.info(f"Analizando {sport.upper()}...")
        eventos = obtener_eventos(sport)
        if not eventos:
            logging.info(f"No se encontraron eventos para {sport}.")
            continue
        for evento in eventos:
            # Aquí podrías agregar lógica de filtrado por surebet, profit mínimo, etc.
            insertar_surebet(evento)

    cursor.close()
    conn.close()
    logging.info("Proceso finalizado.")
