import os
import traceback
from connection import MySQLDatabase, execute_read, execute_write
from downloader import download_gzip_and_write_to_json
import orjson
import datetime
import concurrent.futures
import time

COUNTER = 1
TOTAL = 1


def create_game_data_table(conn):
    sql = """
        CREATE TABLE IF NOT EXISTS `game_data` (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            game_id VARCHAR(255) NOT NULL,
            platform_game_id VARCHAR(255) NOT NULL,
            event_type VARCHAR(255) NOT NULL,
            participant_id VARCHAR(255) DEFAULT NULL,
            participant_team_id VARCHAR(255) DEFAULT NULL,
            game_winner VARCHAR(255) DEFAULT NULL,
            game_date DATE DEFAULT NULL,
            INDEX (game_id),
            INDEX (participant_id ASC),
            INDEX (game_id, participant_id ASC),
            INDEX (event_type ASC)
        );
    """
    return execute_write(conn, sql, {})


def get_game_ids(conn):
    sql = """
        SELECT DISTINCT t1.game_id, platform_game_id FROM tournaments t1
        JOIN mapping_data t2 ON t1.game_id = t2.game_id
        WHERE t1.game_state = 'completed';
    """
    return execute_read(conn, sql, {})


def game_exists(conn, game_id):
    sql = """
        SELECT COUNT(*) as count FROM game_data WHERE game_id = %(game_id)s;
    """
    exc_res, result = execute_read(conn, sql, {"game_id": game_id}, only_one=True)
    return result["count"] >= 11


def process(game_id, platform_game_id):
    """
    Download game file from S3 -> populate game data in MySQL -> delete game file
    """
    global COUNTER
    global TOTAL
    conn = None
    try:
        conn = MySQLDatabase().get_connection()
        if game_exists(conn, game_id):
            COUNTER += 1
            if COUNTER % 75 == 0:
                print(f"{int(COUNTER/TOTAL * 100)} games processed")
            if os.path.exists(f"{directory}/{platform_game_id.replace(':', '_')}.json"):
                os.unlink(f"{directory}/{platform_game_id.replace(':', '_')}.json")
            return
        download_gzip_and_write_to_json(f"games/{platform_game_id}")
        populate_game_data(conn, game_id, platform_game_id)
        conn.commit()
        os.unlink(f"{directory}/{platform_game_id.replace(':', '_')}.json")
    except Exception:
        conn.rollback()
        print(traceback.format_exc())
    finally:
        if conn is not None:
            conn.close()


def populate_game_info(conn, game_id, event):
    sql = """
        INSERT INTO game_data (game_id, platform_game_id, event_type, participant_id, participant_team_id)
        VALUES (
            %(game_id)s,
            %(platform_game_id)s,
            %(event_type)s,
            %(participant_id)s,
            %(participant_team_id)s
        )"""
    params = {
        "game_id": game_id,
        "platform_game_id": event['platform_game_id'],
        "event_type": event['event_type'],
        "participant_id": event['participant_id'],
        "participant_team_id": event['team_id'],
    }
    return execute_write(conn, sql, params)


def populate_game_end(conn, game_id, event):
    sql = """
        INSERT INTO game_data (game_id, game_winner, game_date)
        VALUES (
            %(game_id)s,
            %(game_winner)s,
            %(game_date)s
        )
    """
    params = {
        "game_id": game_id,
        "game_winner": event['game_winner'],
        "game_date": event['game_date'],
    }
    return execute_write(conn, sql, params)


def populate_game_data(conn, game_id, platform_game_id):
    with open(f"{directory}/{platform_game_id.replace(':', '_')}.json", "r") as f:
        game_events = orjson.loads(f.read())
        for event in game_events:
            if event['eventType'] == 'game_info':
                event_data = {
                    "game_id": game_id,
                    "platform_game_id": platform_game_id,
                    "event_type": event['eventType'],
                }
                for participant in event['participants']:
                    participant_data = {
                        'participant_id': participant['participantID'],
                        'team_id': participant['teamID'],
                    }
                    event_data = {**event_data, **participant_data}
                    populate_game_info(conn, game_id, event_data)
            elif event['eventType'] == 'game_end':
                event_data = {
                    "game_id": game_id,
                    "platform_game_id": platform_game_id,
                    "game_winner": event['winningTeam'],
                    "game_date": datetime.datetime.fromisoformat(event['eventTime'].replace('Z', '')).date(),
                }
                populate_game_end(conn, game_id, event_data)
            else:
                continue


if __name__ == '__main__':
    try:
        conn = MySQLDatabase().get_connection()
        create_game_data_table(MySQLDatabase().get_connection())
        directory = "games"
        if not os.path.exists(directory):
            os.makedirs(directory)
        exc_res, res = get_game_ids(conn)
        TOTAL = len(res)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for obj in res:
                futures.append(executor.submit(process, obj['game_id'], obj['platform_game_id']))
            concurrent.futures.wait(futures)
        os.rmdir(directory)
    except Exception:
        print(traceback.format_exc())
    finally:
        if conn is not None:
            conn.close()
