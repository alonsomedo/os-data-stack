#https://dashboard.api-football.com/soccer/requests#
#https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/connections/mysql.html

import os
import requests
import logging
import json 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.decorators import task

from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
from pandas import json_normalize

file_name = f"/tmp/barcelona.csv"

with DAG(
    dag_id='import_soccer_teams_data',
    start_date=datetime(2023, 3, 31),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    ## The Api Key will be available only until 2023/06/01
    ## It has a limitation of 100 request per day
    ## Suggest you to create your own account: https://dashboard.api-football.com/
    @task(task_id='get_barcelona_players')
    def get_team_players(soccer_team_id):
        url = 'https://v3.football.api-sports.io/players/squads'
        params = {'team': soccer_team_id}
        headers = {
            'x-rapidapi-host': "v3.football.api-sports.io",
            'x-rapidapi-key': "43307bbe1dd253b7a85022853ecf2b51"
        }
        response = requests.get(url, params=params, headers=headers)
        data = json.loads(response.text)['response'][0]['players']    
        data = json_normalize(data)
        data.to_csv(f'/tmp/{soccer_team_id}.csv', sep='\t', index=False, header=False)
    
    @task(task_id='load_barcelona_players')
    def load_team_players(soccer_team_id):
        file_path = f'/tmp/{soccer_team_id}.csv'
        pg_hook = PostgresHook()
        pg_hook.bulk_load('ods.soccer_players', file_path)
    
    
    create_ODS_schema = PostgresOperator(
        task_id='create_ODS_schema',
        sql="""
            CREATE SCHEMA IF NOT EXISTS ods;
        """,
        runtime_parameters={"search_path": "public"}
    )
        
    
    create_players_table = PostgresOperator(
        task_id='create_soccer_players_table',
        sql="""
            CREATE TABLE IF NOT EXISTS ods.soccer_players
            (
                player_id int,
                name varchar(100),
                age int,
                number varchar(100),
                position varchar(100),
                photo varchar(200) 
            );
        """
    )
    
    clean_soccer_players_table = PostgresOperator(
        task_id='clean_soccer_players_table',
        sql="""
            DELETE FROM ods.soccer_players;
        """,
        runtime_parameters={"search_path": "public"}
    )
    
    get_team_players = get_team_players(529)
    load_team_players = load_team_players(529)
    
    for position in ['Goalkeeper','Defender','Midfielder','Attacker']:
        
        create_players_by_position_table = PostgresOperator(
        task_id=f'create_{position}_table',
        sql=f"""
            CREATE TABLE IF NOT EXISTS ods.soccer_players_{position}
            (
                player_id int,
                name varchar(100),
                age int,
                number varchar(100),
                position varchar(100),
                photo varchar(200) 
            );
            """
        )
        
        load_data_players_by_position = PostgresOperator(
        task_id=f'load_data_{position}_players',
        sql=f"""
            DELETE FROM ods.soccer_players_{position};
            INSERT INTO ods.soccer_players_{position}
            SELECT
                player_id,
                name,
                age,
                number,
                position,
                photo
            FROM ods.soccer_players
            WHERE position = '{position}';
            """
        )
    
        start >> create_ODS_schema >> create_players_table >> get_team_players >> clean_soccer_players_table
        clean_soccer_players_table >> load_team_players >> create_players_by_position_table >> load_data_players_by_position >> end