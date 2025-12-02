def monitor_live_and_notify():
    rows = db_exec("""
        SELECT id, event_id, home_team, away_team, odds_over, odds_under, stake_over, stake_under,
               last_home_score, last_away_score
        FROM matches
        WHERE track_live=TRUE
          AND market='over_under'
          AND selection='over_2.5'
          AND bet_placed=TRUE
    """, fetch=True)

    if not rows:
        logging.info("No hay partidos con track_live=TRUE para monitorear.")
        return

    fixture_ids = [r["event_id"] for r in rows]
    prematch_index = {r["event_id"]: r for r in rows}

    live_events = fetch_live_under25(fixture_ids)
    if not live_events:
        logging.info("No se obtuvieron odds UNDER 2.5 en vivo para los fixtures marcados.")
        return

    for ev in live_events:
        fixture_id = ev["event_id"]
        under_live = float(ev.get("cuota_under25") or 0)
        bookmaker_live_name = ev.get("bookmaker_name") or ""

        # Estado y minuto desde periods
        fixture_periods = sportmonks_request(
            f"/football/fixtures/{fixture_id}",
            params={"include": "periods"}
        ).get("data", {})

        state_id = fixture_periods.get("state_id")
        match_minute = 0
        for p in fixture_periods.get("periods", []):
            if p.get("minute") is not None:
                match_minute = p.get("minute")
                break

        # Marcador actual desde scores
        fixture_scores = sportmonks_request(
            f"/football/fixtures/{fixture_id}",
            params={"include": "scores"}
        ).get("data", {})

        home_score, away_score = 0, 0
        for s in fixture_scores.get("scores", []):
            if s.get("description") == "CURRENT":
                if s["score"]["participant"] == "home":
                    home_score = s["score"]["goals"]
                elif s["score"]["participant"] == "away":
                    away_score = s["score"]["goals"]

        # Desactivar por estado (3–13)
        if state_id is not None and 3 <= state_id <= 13:
            logging.info(f"Partido {fixture_id} con state_id={state_id}, se desactiva track_live.")
            try:
                db_exec("UPDATE matches SET track_live=FALSE WHERE event_id=%s", (fixture_id,))
            except Exception as e:
                logging.error(f"Error desactivando track_live: {e}")
            continue

        # Ignorar después del minuto 20
        if match_minute > 20:
            logging.info(f"Partido {fixture_id} minuto {match_minute}, se deja de monitorear.")
            continue

        pm = prematch_index.get(fixture_id)
        if not pm:
            continue

        match_id_db = pm["id"]
        home = pm.get("home_team") or ""
        away = pm.get("away_team") or ""
        over_odds_prematch = float(pm.get("odds_over") or 0)
        stake_over = float(pm.get("stake_over") or 0)

        # Alerta de gol temprano (≤20)
        last_home = pm.get("last_home_score") or 0
        last_away = pm.get("last_away_score") or 0
        if (home_score > last_home or away_score > last_away) and match_minute <= 20:
            msg = (
                f"⚽️ GOL temprano en {home} vs {away} (min {match_minute}).\n"
                f"Marcador actual: {home_score}-{away_score}.\n"
                f"👉 Considerar CASHOUT."
            )
            send_telegram(msg)

        # Lógica de surebet con BASE_STAKE
        implied_sum, s_over, s_under, profit_abs, profit_pct = compute_surebet_stakes(
            over_odds_prematch, under_live, BASE_STAKE
        )

        # Umbral de surebet (Under mínimo)
        umbral_surebet = None
        if over_odds_prematch > 1:
            try:
                umbral_surebet = over_odds_prematch / (over_odds_prematch - 1)
            except ZeroDivisionError:
                umbral_surebet = None

        if implied_sum < 1.0:
            msg = (
                f"🔥 Surebet LIVE {home} vs {away} (min {match_minute}).\n"
                f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live_name}).\n"
                f"Stake base {BASE_STAKE:.2f} {CURRENCY} ⇒ Over: {s_over:.2f}, Under: {s_under:.2f}.\n"
                f"Profit esperado: {profit_abs:.2f} {CURRENCY} ({profit_pct*100:.2f}%)."
            )
            send_telegram(msg)
            try:
                db_exec("""
                    INSERT INTO alerts (match_id, kind, message, profit_pct, profit_abs, created_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                """, (match_id_db, "surebet_live", msg, profit_pct, profit_abs))
            except Exception as e:
                logging.error(f"Error insert alert surebet_live: {e}")
        else:
            # No hay surebet: sugerir cobertura minimax y alternativa parcial
            if stake_over > 0 and over_odds_prematch > 1 and under_live > 1:
                stake_under_opt, loss_max = cobertura_minimax_over_under(stake_over, over_odds_prematch, under_live)
                stake_under_partial = round(stake_under_opt * COVERAGE_RATIO, 2) if stake_under_opt else 0.0

                if loss_max is not None and stake_under_opt > 0:
                    msg = (
                        f"🛡️ Cobertura minimax {home} vs {away} (min {match_minute}).\n"
                        f"Over 2.5 prematch: stake {stake_over:.2f} @ {over_odds_prematch}.\n"
                        f"Under 2.5 live: @ {under_live} ({bookmaker_live_name}).\n"
                        f"⇒ Stake Under óptimo: {stake_under_opt:.2f} {CURRENCY} (pérdida máxima ≈ {loss_max:.2f} {CURRENCY}).\n"
                        f"Alternativa parcial ({int(COVERAGE_RATIO*100)}%): {stake_under_partial:.2f} {CURRENCY} "
                        f"para conservar upside."
                    )
                else:
                    msg = (
                        f"ℹ️ Sin surebet y no se pudo calcular cobertura minimax por datos inválidos.\n"
                        f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live}."
                    )
                send_telegram(msg)
            else:
                # Mensaje informativo sin cálculo de cobertura (por cuotas/stake inválidos)
                base_msg = (
                    f"ℹ️ Sin surebet LIVE {home} vs {away} (min {match_minute}).\n"
                    f"Over 2.5 pre @ {over_odds_prematch} | Under 2.5 live @ {under_live} ({bookmaker_live_name}).\n"
                    f"Suma inversas: {implied_sum:.4f}."
                )
                if umbral_surebet is not None:
                    base_msg += f" Umbral de surebet (Under mínimo): {umbral_surebet:.2f}."
                else:
                    base_msg += " Umbral de surebet no disponible (cuota inválida)."
                send_telegram(base_msg)
