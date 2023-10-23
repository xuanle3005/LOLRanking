from connection import execute_write, execute_read, MySQLDatabase
import pymysql
import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib
import datetime as dt
import os


# date, blue_team, red_team, blue_team_wins, red_team_wins, tournament
conn = MySQLDatabase().get_connection()
df = pd.read_sql_query( 'SELECT start_date, tournament_id, game_id, blue_team_id, red_team_id, game_winner  FROM lolesport.tournaments ORDER BY start_date', conn)
# print(tournament_data.head())

k_factor = 5
regions_1= 40
regions_2 = 30
regions_3 = 20
k_intl = 50
k_friendly = 10


conn = MySQLDatabase().get_connection()
df = pd.read_sql_query( 'SELECT start_date, tournament_id, game_id, blue_team_id, red_team_id, game_winner  FROM lolesport.tournaments ORDER BY start_date', conn)
print(df.head())

def k_factor(tournament_id):
    conn = MySQLDatabase().get_connection()
    sql = '''
            SELECT region from lolesport.update_team
            WHERE team_id =%s
          '''
    res = execute_read(conn, sql, {tournament_id})
    region = res[1][0][0]
    if region == 'global':
        k_factor = k_intl
    if region in ['lck', 'lpl']:
        k_factor = regions_1
    elif region in ['lcs', 'lec']:
        k_factor = regions_2
    else:
        k_factor = regions_3
    return k_factor

def expected_result(blue, red):
    dr = blue - red
    we = (1/10**(-dr/400)+1)
    return [np.round(we,3),1-np.round(we,3)]

def actual_result(blue,red):
    if blue<red:
        wr = 1
        wb = 0
    elif blue > red:
        wr = 0
        wb = 1
    elif blue == red:
        wb = 0.5
        wr = 0.5
    return [wb,wr]
def calculate_elo(elo_b, elo_r, blue_team_wins, red_team_wins, tournament_id):
    # Add regions to tournament!!!!
    k = k_factor(tournament_id)
    wb, wr = actual_result(blue_team_wins, red_team_wins)
    web,wer = expected_result(elo_b, elo_r)
    elo_bn = elo_b + k*(wb - web)
    elo_rn = elo_r + k*(wr - wer)
    return elo_bn, elo_rn


#Calculate elo for each match
current_elo = {}
for idx, row in df.iterrows():
    blue=row['blue_team_id']
    blue_region = row[...]
    red_region = red[...]
    red = row['red_team_id']
    red_wins = row[...]
    blue_wins = row[...]
    tournament_id = row[tournament_id]

    if blue not in current_elo.keys():
        if blue_region == 'lck' or blue_region == 'lpl':
            current_elo[blue] = 1500
        elif blue_region == 'lcs' or blue_region == 'lec':
            current_elo[blue] = 1300
        else:
            current_elo[blue] = 1200

    if red not in current_elo.keys():
        if red_region == 'lck' or red_region == 'lpl':
            current_elo[red] = 1500
        elif red_region == 'lcs' or red_region == 'lec':
            current_elo[red] = 1300
        else:
            current_elo[red] = 1200

    elo_b = current_elo[blue]
    elo_r = current_elo[red]
    elo_bn, elo_rn = calculate_elo(elo_b, elo_r, blue_wins, red_wins, tournament_id)
    
    current_elo[blue] = elo_bn
    current_elo[red] = elo_rn

    df.loc[idx, 'elo_b_after'] = elo_bn
    df.loc[idx, 'elo_r_after']= elo_rn
    df.loc[idx, 'elo_b_before'] = elo_b
    df.loc[idx, 'elo_r_before'] = elo_r
    
