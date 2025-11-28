def fetch_prematch_over25():
    hoy = datetime.now(LIMA_TZ).date()
    manana = hoy + timedelta(days=3)
    base_url = f"{SPORTMONKS_BASE}/fixtures/between/{hoy.isoformat()}/{manana.isoformat()}"
    page = 1
    all_fixtures = []

    while True:
        try:
            url = f"{base_url}?api_token={SPORTMONKS_TOKEN}&page={page}&include=participants"
            logging.info(f"URL de la API pre-match={url}")
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
        
        fecha_hora_raw = fixture.get("starting_at")
        try:
            dt = datetime.fromisoformat(fecha_hora_raw.replace("Z", "+00:00"))
            dt_lima = dt.astimezone(LIMA_TZ)
            dt_lima_plus5 = dt_lima + timedelta(hours=5)
            fecha_hora_str = dt_lima_plus5.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            fecha_hora_str = (datetime.now(LIMA_TZ) + timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")

        created_raw = fixture.get("created_at")
        try:
            dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            dt_lima = dt.astimezone(LIMA_TZ)
            dt_lima_plus5 = dt_lima + timedelta(hours=5)
            created_str = dt_lima_plus5.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            created_str = (datetime.now(LIMA_TZ) + timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")

        updated_raw = fixture.get("latest_bookmaker_update")
        try:
            dt = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
            dt_lima = dt.astimezone(LIMA_TZ)
            dt_lima_plus5 = dt_lima + timedelta(hours=5)
            updated_str = dt_lima_plus5.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            updated_str = (datetime.now(LIMA_TZ) + timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")

        odds_data = sportmonks_request(f"/odds/pre-match/fixtures/{fixture_id}/markets/7")

        mejor_over = None
        casa_over = None
        mejor_under = None
        casa_under = None

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

            if label == "under" and total_line in {"2.5"}:
                if mejor_under is None or cuota > mejor_under:
                    mejor_under = cuota
                    casa_under = BOOKMAKER_MAP.get(bookmaker_id, str(bookmaker_id))

        # 👇 Calcular umbral y cobertura si hay cuotas válidas
        umbral_surebet = None
        cobertura_stake = None
        cobertura_resultado = None
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
            "cobertura_resultado": cobertura_resultado
        })

        # 👇 ALERTA TELEGRAM extendida
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
                    send_telegram(mensaje)
                    logging.info(f"Alerta enviada por Telegram: {mensaje}")

    return resultados
