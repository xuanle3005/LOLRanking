from connection import execute_write, execute_read, MySQLDatabase
import pymysql
import datetime as dt
import os

# date, blue_team, red_team, blue_team_wins, red_team_wins, tournament
tournament_data = []
match_type, game_id, blue_team_id, red_team_id, game_winner = '', '', '', '', ''
k_factor = 0
regions_1= 300
regions_2 = 200
regions_3 = 100

def get_tournament_data():
    conn = MySQLDatabase().get_connection()

    sql = '''
            SELECT start_date, tournament_id, game_id, blue_team_id, red_team_id, game_winner  
            FROM lolesport.tournaments
            ORDER BY start_date
        '''
    
    res = execute_read(conn, sql, {})
    tournament_data = list(res[1])
    return tournament_data


def get_tournament_game(tournament_data):
    game_id = tournament_data[2]
    blue_team_id = tournament_data[3]
    red_team_id = tournament_data[4]
    game_winner = tournament_data[5]

    return game_id, blue_team_id, red_team_id, game_winner

def get_team_region_rating(team_id):
    conn = MySQLDatabase().get_connection()
    sql = '''
            SELECT region from lolesport.update_team
            WHERE team_id =%s
          '''
    res = execute_read(conn, sql, {team_id})
    region = res[1][0][0]

    if region in ['lck', 'lpl']:
        k_factor += regions_1
    elif region in ['lcs', 'lec']:
        k_factor += regions_2
    else:
        k_factor += regions_3
    return k_factor

def get_match_type(tournament_slug):
    # return domestic or intl
    ...
def get_match_rating(match_type):
    #add up k_factor base on match_type
    ...



if __name__ == '__main__':
    get_tournament_data()
    get_team_region_rating('109981650516317055')
    # for game in range(tournament_data):
    #     get_tournament_game(game)
    #     get_team_region_rating(team_1_id)
    #     get_team_region_rating(team_2_id)
    
