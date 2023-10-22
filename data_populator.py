import orjson
import traceback
from connection import execute_write, MySQLDatabase


def write_league_data(conn, league_data):
    sql = """
    INSERT INTO `leagues` (
        league_id,
        league_name,
        league_slug,
        tournament_id,
        region
    )
    VALUES (
        %(league_id)s,
        %(league_name)s,
        %(league_slug)s,
        %(tournament_id)s,
        %(region)s
    )
    """
    params = {
        'league_id': league_data['league_id'],
        'league_name': league_data['league_name'],
        'league_slug': league_data['league_slug'],
        'tournament_id': league_data['tournament_id'],
        'region': league_data['region']
    }
    return execute_write(conn, sql, params, auto_commit=False)


def write_tournament_data(conn, tournament_data):
    sql = """
    INSERT INTO `tournaments` (
        tournament_id,
        league_id,
        tournament_name,
        tournament_slug,
        start_date,
        end_date,
        section,
        stage_name,
        stage_slug,
        game_id,
        blue_team_id,
        red_team_id,
        game_winner,
        match_winner
    )
    VALUES (
        %(tournament_id)s,
        %(league_id)s,
        %(tournament_name)s,
        %(tournament_slug)s,
        %(start_date)s,
        %(end_date)s,
        %(section)s,
        %(stage_name)s,
        %(stage_slug)s,
        %(game_id)s,
        %(blue_team_id)s,
        %(red_team_id)s,
        %(game_winner)s,
        %(match_winner)s
    )
    """
    params = {
        'tournament_id': tournament_data['tournament_id'],
        'league_id': tournament_data['league_id'],
        'tournament_name': tournament_data['tournament_name'],
        'tournament_slug': tournament_data['tournament_slug'],
        'start_date': tournament_data['start_date'],
        'end_date': tournament_data['end_date'],
        'section': tournament_data['section'],
        'stage_name': tournament_data['stage_name'],
        'stage_slug': tournament_data['stage_slug'],
        'game_id': tournament_data['game_id'],
        'blue_team_id': tournament_data['blue_team_id'],
        'red_team_id': tournament_data['red_team_id'],
        'game_winner': tournament_data['game_winner'],
        'match_winner': tournament_data['match_winner']
    }
    return execute_write(conn, sql, params, auto_commit=False)


def write_player_data(conn, player_data):
    sql = """
    INSERT INTO `players` (
        player_id,
        player_name,
        player_team_id
    )
    VALUES (
        %(player_id)s,
        %(player_name)s,
        %(player_team_id)s
    )
    """
    params = {
        'player_id': player_data['player_id'],
        'player_name': player_data['player_name'],
        'player_team_id': player_data['player_team_id']
    }
    return execute_write(conn, sql, params, auto_commit=False)


def write_team_data(conn, team_data):
    sql = """
    INSERT INTO `teams` (
        team_id,
        team_name,
        team_slug,
        team_acronym
    )
    VALUES (
        %(team_id)s,
        %(team_name)s,
        %(team_slug)s,
        %(team_acronym)s
    )
    """
    params = {
        'team_id': team_data['team_id'],
        'team_name': team_data['team_name'],
        'team_slug': team_data['team_slug'],
        'team_acronym': team_data['team_acronym']
    }
    return execute_write(conn, sql, params, auto_commit=False)


def write_mapping_data(conn, mapping_data):
    sql = """
    INSERT INTO `mapping_data` (
        game_id,
        platform_game_id,
        blue_team_id,
        red_team_id,
        participant_id,
        participant_player_id
    )
    VALUES (
        %(game_id)s,
        %(platform_game_id)s,
        %(blue_team_id)s,
        %(red_team_id)s,
        %(participant_id)s,
        %(participant_player_id)s
    )
    """
    params = {
        'game_id': mapping_data['game_id'],
        'platform_game_id': mapping_data['platform_game_id'],
        'blue_team_id': mapping_data['blue_team_id'],
        'red_team_id': mapping_data['red_team_id'],
        'participant_id': mapping_data['participant_id'],
        'participant_player_id': mapping_data['participant_player_id']
    }
    return execute_write(conn, sql, params, auto_commit=False)


def populate_table_leagues(conn, non_processed):
    f = None
    if not non_processed:
        f = open('esports-data/leagues.json', 'rb')
        leagues = orjson.loads(f.read())
    else:
        leagues = non_processed
    for league in leagues:
        try:
            for tournament in league['tournaments']:
                league_data = {
                    'league_id': league['id'],
                    'league_name': league['name'],
                    'league_slug': league['slug'],
                    'tournament_id': tournament['id'],
                    'region': league['region']
                }
                write_league_data(conn, league_data)
        except Exception:
            print(traceback.format_exc())
            non_processed.append(league)
    if f:
        f.close()
    print(f"Done populating leagues, total leagues: {len(leagues)}, non_processed: {len(non_processed)}")
    return 'leagues', non_processed


def populate_table_tournaments(conn, non_processed):
    f = None
    if not non_processed:
        f = open('esports-data/tournaments.json', 'rb')
    tournaments = orjson.loads(f.read()) if not non_processed else non_processed
    for tournament in tournaments:
        try:
            tournament_data = {
                'tournament_id': tournament['id'],
                'league_id': tournament['leagueId'],
                'tournament_name': tournament['name'],
                'tournament_slug': tournament['slug'],
                'start_date': tournament['startDate'],
                'end_date': tournament['endDate'],
            }
            for stage in tournament['stages']:
                stage_data = {
                    'stage_name': stage['name'],
                    'stage_slug': stage['slug'],
                }
                for section in stage['sections']:
                    section_data = {
                        'section': section['name'],
                    }
                    for match in section['matches']:
                        match_data = {
                            'match_id': match['id'],
                        }
                        for team in match['teams']:
                            try:
                                if team['result']['outcome'].lower() == 'win':
                                    match_data['match_winner'] = team['id']
                            except Exception:
                                match_data['match_winner'] = None
                        for game in match['games']:
                            if game['state'] == 'unneeded':
                                continue
                            game_data = {
                                'game_id': game['id'],
                            }
                            for team in game['teams']:
                                try:
                                    if team['result']['outcome'].lower() == 'win':
                                        game_data['game_winner'] = team['id']
                                except Exception:
                                    game_data['game_winner'] = None
                                if team['side'] == 'blue':
                                    game_data['blue_team_id'] = team['id']
                                elif team['side'] == 'red':
                                    game_data['red_team_id'] = team['id']
                            tournament_data = {
                                **tournament_data,
                                **stage_data,
                                **section_data,
                                **match_data,
                                **game_data,
                            }
                            write_tournament_data(conn, tournament_data)
        except Exception:
            print(traceback.format_exc())
            non_processed.append(tournament)
    if f:
        f.close()
    print("Done populating tournaments, total tournaments: {}, non_processed: {}".format(len(tournaments), len(non_processed)))
    return 'tournaments', non_processed


def populate_table_players(conn, non_processed):
    f = None
    if not non_processed:
        f = open('esports-data/players.json', 'rb')
    players = orjson.loads(f.read()) if not non_processed else non_processed
    for player in players:
        try:
            player_data = {
                'player_id': player['player_id'],
                'player_name': player['handle'],
                'player_team_id': player['home_team_id'],
            }
            write_player_data(conn, player_data)
        except Exception:
            print(traceback.format_exc())
            non_processed.append(player)
    if f:
        f.close()
    print("Done populating players, total players: {}, non_processed: {}".format(len(players), len(non_processed)))
    return 'players', non_processed


def populate_table_teams(conn, non_processed):
    f = None
    if not non_processed:
        f = open('esports-data/teams.json', 'rb')
    teams = orjson.loads(f.read()) if not non_processed else non_processed
    for team in teams:
        try:
            team_data = {
                'team_id': team['team_id'],
                'team_name': team['name'],
                'team_slug': team['slug'],
                'team_acronym': team['acronym'],
            }
            write_team_data(conn, team_data)
        except Exception:
            non_processed.append(team)
            print(traceback.format_exc())
    if f:
        f.close()
    print("Done populating teams, total teams: {}, non_processed: {}".format(len(teams), len(non_processed)))
    return 'teams', non_processed


def populate_table_mapping_data(conn, non_processed):
    f = None
    if not non_processed:
        f = open('esports-data/mapping_data.json', 'rb')
    mapping_data = orjson.loads(f.read()) if not non_processed else non_processed
    for mapping in mapping_data:
        try:
            mapping_data = {
                'game_id': mapping['esportsGameId'],
                'platform_game_id': mapping['platformGameId'],
                "blue_team_id": mapping['teamMapping']['100'],
                "red_team_id": mapping['teamMapping']['200'],
            }
            for participant in mapping['participantMapping']:
                mapping_data['participant_id'] = participant
                mapping_data['participant_player_id'] = mapping['participantMapping'][participant]
                write_mapping_data(conn, mapping_data)
        except Exception:
            non_processed.append(mapping)
            print(traceback.format_exc())
    if f:
        f.close()
    print("Done populating mapping data, total mapping data: {}, non_processed: {}".format(len(mapping_data), len(non_processed)))
    return 'mapping_data', non_processed


def populate_tables():
    non_processed_data = {}
    with open('non_processed.json', 'r') as f:
        non_processed_data = orjson.loads(f.read())
    db_conn_mapping = {}
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            db_conn_mapping = {
                'leagues': MySQLDatabase().get_connection(),
                'tournaments': MySQLDatabase().get_connection(),
                'players': MySQLDatabase().get_connection(),
                'teams': MySQLDatabase().get_connection(),
                'mapping_data': MySQLDatabase().get_connection(),
            }
            futures = []
            futures.append(executor.submit(populate_table_leagues, db_conn_mapping.get('leagues'), non_processed_data['leagues']))
            futures.append(executor.submit(populate_table_tournaments, db_conn_mapping.get('tournaments'), non_processed_data['tournaments']))
            futures.append(executor.submit(populate_table_players, db_conn_mapping.get('players'), non_processed_data['players']))
            futures.append(executor.submit(populate_table_teams, db_conn_mapping.get('teams'), non_processed_data['teams']))
            futures.append(executor.submit(populate_table_mapping_data, db_conn_mapping.get('mapping_data'), non_processed_data['mapping_data']))
            for future in concurrent.futures.as_completed(futures):
                try:
                    table, non_processed = future.result()
                    non_processed_data[table] = non_processed
                    db_conn_mapping[table].commit()
                except Exception:
                    print(traceback.format_exc())
        with open('non_processed.json', 'w') as f:
            f.write(orjson.dumps(non_processed_data).decode('utf-8'))
    except Exception:
        print(traceback.format_exc())
    finally:
        for key in db_conn_mapping:
            if db_conn_mapping[key]:
                db_conn_mapping[key].close()


if __name__ == '__main__':
    populate_tables()
