import pymysql
import os

ENDPOINT = "lolesport.cv2hmfnelsdh.us-east-2.rds.amazonaws.com"
PORT = 3306
USER = "admin"
REGION = "us-east-2c"
DBNAME = ""
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


def execute_command(command, args, only_one=False):
    conn = None
    result = None
    try:
        conn = pymysql.connect(host=ENDPOINT, user=USER, passwd='12345678', port=PORT, database=DBNAME, ssl_ca='us-east-2-bundle.pem')
        cur = conn.cursor()
        cur.execute(command, args)
        if only_one:
            result = cur.fetchone()
        else:
            result = cur.fetchall()
    except Exception as e:
        print("Database command failed due to {}".format(e))
    finally:
        if conn is not None:
            conn.close()
    return result


def create_table_leagues():
    sql = """
    CREATE TABLE IF NOT EXISTS `leagues` (
        id UNSIGNED INT AUTO
    )
    """
    return execute_command(sql)


def create_table_tournaments():
    sql = """
    CREATE TABLE IF NOT EXISTS `tournaments` (
        
    )
    """
    return execute_command(sql)


def create_table_players():
    sql = """
    CREATE TABLE IF NOT EXISTS `players` (
        
    )
    """
    return execute_command(sql)


def create_table_teams():
    sql = """
    CREATE TABLE IF NOT EXISTS `teams` (
        
    )
    """
    return execute_command(sql)
