from airflow.utils.task_group import TaskGroup

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

from utils.tools import transform_load_data, load_city, load_weather

import json

def task_group():
    with TaskGroup(group_id = 'parallel_tasks', tooltip= "parallel tasks") as task_group:
        # city_task
            create_city_table = PostgresOperator(
                task_id='create_city_table',
                postgres_conn_id = "postgres_conn",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS city_look_up (
                    city VARCHAR(30) NULL,
                    country VARCHAR(30) NULL,
                    population INT NULL,
                    is_capital BOOLEAN NULL,
                    id SERIAL               
                );
                '''
            )

            truncate_city_table = PostgresOperator(
                task_id='truncate_city_table',
                postgres_conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE city_look_up;
                    '''
            )


            is_api_ninjas_ready = HttpSensor(
                task_id ='is_api_ninjas_ready',
                method = 'GET',
                http_conn_id='api_ninjas',
                endpoint='/city',
                request_params = {
                     'name': "{{ task_instance.xcom_pull(task_ids='start_pipeline') }}"
                },
                headers = {'X-Api-Key': '3FFl37vHXUzEciA5ge0JHQ==AiqxC5OuvpCvDGgn'},
                poke_interval = 15, 
                timeout = 60
            )

            extract_city_data = SimpleHttpOperator(
                task_id = 'extract_city_data',
                http_conn_id = 'api_ninjas',
                endpoint='/city',
                method = 'GET',
                data = {
                     'name': "{{ task_instance.xcom_pull(task_ids='start_pipeline') }}",
                },
                headers = {'X-Api-Key': '3FFl37vHXUzEciA5ge0JHQ==AiqxC5OuvpCvDGgn'},
                response_filter= lambda r: json.loads(r.text),
                log_response=True
            )

            city_data_to_postgres = PythonOperator(
                 task_id = 'city_data_to_postgres',
                 python_callable=load_city
            )
        
        # weather_task
            create_weather_table = PostgresOperator(
                task_id='create_weather_table',
                postgres_conn_id = "postgres_conn",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS weather_data (
                    city VARCHAR(30) NULL,
                    timezone INT NULL,
                    description VARCHAR(30)  NULL,
                    temp_celcius NUMERIC NULL,
                    feels_like_celcius NUMERIC NULL,
                    temp_min_celcius NUMERIC NULL,
                    temp_max_celcius NUMERIC NULL,
                    humidity NUMERIC NULL,
                    wind_speed NUMERIC NULL,
                    time_of_record INT NULL,
                    sunrise_time INT NULL,
                    sunset_time INT NULL,
                    id SERIAL                     
                );
                '''
            )

            truncate_weather_table = PostgresOperator(
                task_id='truncate_weather_table',
                postgres_conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE weather_data;
                    '''
            )

    
            is_weather_api_ready = HttpSensor(
                task_id ='is_weather_api_ready',
                method = 'GET',
                http_conn_id='weather_api',
                endpoint='/data/2.5/weather',
                request_params = {
                     'q': "{{ task_instance.xcom_pull(task_ids='start_pipeline') }}",
                     'appid': '1bb16afc33746b0ab065b2076b1230a9'

                },
                poke_interval = 15, 
                timeout = 60
            )

            extract_weather_data = SimpleHttpOperator(
                task_id = 'extract_weather_data',
                http_conn_id = 'weather_api',
                endpoint='/data/2.5/weather',
                method = 'GET',
                data = {
                     'q': "{{ task_instance.xcom_pull(task_ids='start_pipeline') }}",
                     'appid': '1bb16afc33746b0ab065b2076b1230a9'
                },
                response_filter= lambda r: json.loads(r.text),
                log_response=True
            )



            transform_load_weather_data = PythonOperator(
                task_id= 'transform_load_weather_data',
                python_callable=transform_load_data
            )

            load_weather_data = PythonOperator(
            task_id= 'load_weather_data',
            python_callable=load_weather
            )

        # wait_task
            wait = BashOperator(
            task_id='wait',
            bash_command="sleep 2",
            trigger_rule='all_success'
            )

            create_city_table >> truncate_city_table >> is_api_ninjas_ready  >> extract_city_data >> city_data_to_postgres >> wait
            create_weather_table >> truncate_weather_table >> is_weather_api_ready >>  extract_weather_data >> transform_load_weather_data >> load_weather_data >> wait
    return task_group


# psql -h mydatabse.ccfj9oudddql.us-east-1.rds.amazonaws.com -p 5432 -U postgres -W