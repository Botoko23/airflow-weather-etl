from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

from io import StringIO
import random
import os
import numpy as np


def upload_to_s3(filename_and_key:str, bucket_name:str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename_and_key, key=filename_and_key, bucket_name=bucket_name)
    os.remove(filename_and_key)

def data_from_s3(bucket_name: str, key: str) -> None:
    hook = S3Hook('s3_conn')
    content = hook.read_key(bucket_name=bucket_name, key=key)
    df = pd.read_csv(StringIO(content))
    random_city = random.choice(df['city'])
    print(random_city)
    return random_city
    
def kelvin_to_celcius(temp):
    return round(temp - 273.15, 2) 

def transform_load_data(task_instance):
    r = task_instance.xcom_pull(task_ids="parallel_tasks.extract_weather_data")
    data = {
    'city': r['name'],
    'timezone': r['timezone'],
    'description': r['weather'][0]['description'],
    'temp_celcius': kelvin_to_celcius(r['main']['temp']),
    'feels_like_celcius': kelvin_to_celcius(r['main']['feels_like']),
    'temp_min_celcius': kelvin_to_celcius(r['main']['temp_min']),
    'temp_max_celcius': kelvin_to_celcius(r['main']['temp_max']),
    'humidity': r['main']['humidity'],
    'windspeed': r['wind']['speed'],
    'time_of_record':r['dt'],
    'sunrise_time': r['sys']['sunrise'],
    'sunset_time': r['sys']['sunset']
    }
    
    df_data = pd.DataFrame([data])
    
    df_data.to_csv("weather_data.csv", index=False, header=False)

def load_city(task_instance):
    r = task_instance.xcom_pull(task_ids='parallel_tasks.extract_city_data')
    if r:
        r = r[0]
        data = {
            'city': r['name'],
            'country': r['country'],
            'population': r['population'],
            'is_capital': r['is_capital']
        }
        df_data = pd.DataFrame([data])
    
        df_data.to_csv("city_data.csv", index=False, header=False)
        hook = PostgresHook(postgres_conn_id= 'postgres_conn')
        hook.copy_expert(
            sql= "COPY city_look_up(city, country, population, is_capital) FROM stdin WITH DELIMITER as ','",
            filename= 'city_data.csv'
        )
    else:
        data = {
            'city': np.nan, 
            'country': np.nan,
            'population': np.nan,
            'is_capital': np.nan
        }

        df_data = pd.DataFrame([data])

        df_data.to_csv("city_data.csv", index=False, header=False)
        hook = PostgresHook(postgres_conn_id= 'postgres_conn')
        hook.copy_expert(
            sql= "COPY city_look_up(city, country, population, is_capital) FROM stdin WITH (FORMAT csv, DELIMITER ',', NULL '')",
            filename= 'city_data.csv'
        )


def load_weather():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY weather_data(city, timezone, description, temp_celcius, feels_like_celcius, temp_min_celcius, temp_max_celcius, humidity, wind_speed, time_of_record, sunrise_time, sunset_time) FROM stdin WITH DELIMITER as ','",
        filename='weather_data.csv'
    )