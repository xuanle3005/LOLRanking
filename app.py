from flask import Flask, render_template
from flask import request
from flask_restful import Api, Resource
from flask_sqlalchemy import SQLAlchemy
from connection import MySQLDatabase
import pandas as pd
import pymysql
import sqlalchemy
import os
import requests
import jinja2

#Init App
app = Flask(__name__)
api = Api(app)


#Db
conn = MySQLDatabase().get_connection()

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')




@app.route('/team', methods=['GET','POST'])
def get_teams_ranking():
    num_of_teams = request.form.get("number_of_teams")
    if num_of_teams is None:
        num_of_teams = 10
    render_template('team.html', num_of_teams)
    for i in range(num_of_teams):
        team_id = request.form.get("team{}".format(i))
        tournament_id = request.form.get("tournament{}".format(i))
        print(team_id, tournament_id)
        # Something here
        df = pd.read_sql_query('')

    return





@app.route('/tournament', methods=['GET','POST'])
def handle_tournament():
    tournament_id = request.form.get('tournament_id')
    stage = request.form.get('stage_name')
    print('tournament:', tournament_id, stage)

    # something
    df = pd.read_sql_query('SELECT tournament_name, game_id, blue_team_id, red_team_id from lolesport.tournaments where tournament_id = (%s) and stage_name= (%s)', conn, params=(tournament_id, stage))
    print(df.head())
    return render_template('tournament.html', tables=[df.to_html(classes='data')], titles=df.columns.values)

@app.route('/global', methods=['GET','POST'])
def global_team():
    num_of_team = request.form.get("number_of_teams")
    if num_of_team == None:
        num_of_team = 10

    # something
    query = 'SELECT team_name from lolesport.teams LIMIT {0}'.format(num_of_team)
    df = pd.read_sql_query(query, conn)
    return render_template('global.html', tables=[df.to_html(classes='data')], titles=df.columns.values )



    
# Run Server
if __name__ == "__main__":

    app.run(debug=True)