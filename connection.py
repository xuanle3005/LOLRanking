import pymysql
import os
import traceback

ENDPOINT = "lolesport.cv2hmfnelsdh.us-east-2.rds.amazonaws.com"
PORT = 3306
USER = "admin"
REGION = "us-east-2c"
DBNAME = "lolesport"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


class MySQLDatabase(object):
    def __init__(self, **kwargs):
        self.conn = None

    def get_connection(self):
        if not self.is_open(self.conn):
            self.conn = self.__connect()

        return self.conn

    def is_open(self, connection):
        try:
            with connection.cursor():
                return True
        except AttributeError:
            pass
        except Exception:
            print(traceback.format_exc())
            pass
        return False

    def close(self):
        if self.conn:
            self.conn.close()

    def __connect(self, **kwargs):
        conn = None
        try:
            conn = pymysql.connect(host=ENDPOINT, user=USER, passwd='12345678', port=PORT, database=DBNAME, ssl_ca='us-east-2-bundle.pem')
        except Exception:
            print(traceback.format_exc())
        return conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def execute_read(conn, command, args, only_one=False):
    exc_res = None
    result = None
    cur = None
    try:
        cur = conn.cursor()
        exc_res = cur.execute(command, args)
        if only_one:
            result = cur.fetchone()
        else:
            result = cur.fetchall()
    except Exception as e:
        print("Database command failed due to {}".format(e))
    finally:
        if cur is not None:
            cur.close()
    return exc_res, result


def execute_write(conn, command, args, auto_commit=True):
    exc_res = None
    result = None
    cur = None
    try:
        cur = conn.cursor()
        exc_res = cur.execute(command, args)
        if auto_commit:
            conn.commit()
        result = cur.lastrowid
    except Exception as e:
        print(traceback.format_exc())
        print("Database command failed due to {}".format(e))
    finally:
        if cur is not None:
            cur.close()
    return exc_res, result


def create_table_leagues(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS `leagues` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        league_id VARCHAR(255) NOT NULL,
        league_name VARCHAR(255) NOT NULL,
        league_slug VARCHAR(255) NOT NULL,
        tournament_id VARCHAR(255) NOT NULL,
        region VARCHAR(255) NOT NULL,
        INDEX (league_id),
        INDEX (tournament_id ASC)
    );
    """
    return execute_write(conn, sql, {})


def create_table_tournaments(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS `tournaments` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        tournament_id VARCHAR(255) NOT NULL,
        league_id VARCHAR(255) NOT NULL,
        tournament_name VARCHAR(255) NOT NULL,
        tournament_slug VARCHAR(255) NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        stage_name VARCHAR(255) DEFAULT NULL,
        stage_slug VARCHAR(255) DEFAULT NULL,
        section VARCHAR(255) DEFAULT NULL,
        game_id VARCHAR(255) DEFAULT NULL,
        blue_team_id VARCHAR(255) DEFAULT NULL,
        red_team_id VARCHAR(255) DEFAULT NULL,
        game_winner VARCHAR(255) DEFAULT NULL,
        match_winner VARCHAR(255) DEFAULT NULL,
        INDEX (tournament_id),
        INDEX (league_id ASC),
        INDEX (game_id ASC),
        INDEX (blue_team_id ASC),
        INDEX (red_team_id ASC),
        INDEX (game_winner ASC),
        INDEX (match_winner ASC)
    );
    """
    return execute_write(conn, sql, {})


def create_table_players(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS `players` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        player_id VARCHAR(255) NOT NULL,
        player_name VARCHAR(255) NOT NULL,
        player_team_id VARCHAR(255) DEFAULT NULL,
        INDEX (player_team_id ASC),
        INDEX (player_id)
    );
    """
    return execute_write(conn, sql, {})


def create_table_teams(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS `teams` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        team_id VARCHAR(255) NOT NULL,
        team_name VARCHAR(255) NOT NULL,
        team_slug VARCHAR(255) NOT NULL,
        team_acronym VARCHAR(255) NOT NULL,
        INDEX (team_id),
        INDEX (team_acronym ASC)
    );
    """
    return execute_write(conn, sql, {})


def create_table_mapping_data(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS `mapping_data` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        game_id VARCHAR(255) NOT NULL,
        platform_game_id VARCHAR(255) NOT NULL,
        red_team_id VARCHAR(255) NOT NULL,
        blue_team_id VARCHAR(255) NOT NULL,
        participant_id VARCHAR(255) NOT NULL,
        participant_player_id VARCHAR(255) NOT NULL,
        INDEX (game_id),
        INDEX (red_team_id ASC),
        INDEX (blue_team_id ASC),
        INDEX (participant_player_id ASC),
        INDEX (game_id, participant_player_id ASC)
    );"""
    return execute_write(conn, sql, {})


def drop_table(conn, table_name):
    sql = """
    DROP TABLE IF EXISTS `{}`;
    """.format(table_name)
    return execute_write(conn, sql, {})


def drop_all_tables(conn):
    drop_table(conn, 'leagues')
    drop_table(conn, 'tournaments')
    drop_table(conn, 'players')
    drop_table(conn, 'teams')
    drop_table(conn, 'mapping_data')


def create_tables(conn):
    create_table_leagues(conn)
    create_table_tournaments(conn)
    create_table_players(conn)
    create_table_teams(conn)
    create_table_mapping_data(conn)


if __name__ == '__main__':
    conn = MySQLDatabase().get_connection()
    drop_all_tables(conn)
    create_tables(conn)
    conn.close()
