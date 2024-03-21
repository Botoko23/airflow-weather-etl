from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from utils.tools import  data_from_s3, upload_to_s3
from groups.load_transform_group import task_group

from datetime import timedelta, datetime
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


def save_joined_data(task_instance):
    data = task_instance.xcom_pull(task_ids="join_data")
    df = pd.DataFrame(data, columns = ['city', 'time_zone', 'description', 'temp_celcius', 'feels_like_celcius', 'temp_min_celcius', 
                                       'temp_max_celcius','humidity', 'wind_speed', 'time_of_record', 'sunrise_time', 'sunset_time',
                                       'country', 'population', 'is_capital'])
    # df.to_csv("joined_weather_data.csv", index=False
    now = datetime.now().strftime("%d%m%Y%H%M%S")
    file_name = f"weather_data_{now}.csv"
    df.to_csv(file_name, index=False)
    return file_name

with DAG('weather_dag_2',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        start_pipeline = PythonOperator(
            task_id='start_pipeline',
            python_callable=data_from_s3,
            op_kwargs={
                'bucket_name': 'weather-bucket-bvyanrsa78eh',
                'key': 'worldcities.csv'  
            }
        )

        task_group_job = task_group()


        join_data = PostgresOperator(
                task_id='join_data',
                postgres_conn_id = "postgres_conn",
                sql= '''SELECT 
                    w.city,
                    timezone,                    
                    description,
                    temp_celcius,
                    feels_like_celcius,
                    temp_min_celcius,
                    temp_max_celcius,
                    humidity,
                    wind_speed,
                    time_of_record,
                    sunrise_time,
                    sunset_time,
                    country,
                    population,
                    is_capital                  
                    FROM weather_data w
                    INNER JOIN city_look_up c
                        ON w.id = c.id                                      
                ;
                '''
            )

        save_final_data = PythonOperator(
            task_id= 'save_final_data',
            python_callable=save_joined_data
            )
        
        upload_json_to_s3 = PythonOperator(
            task_id='upload_json_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename_and_key': "{{ task_instance.xcom_pull(task_ids='save_final_data') }}",
                'bucket_name': 'weather-bucket-bvyanrsa78eh'
            }
    )

        start_pipeline >> task_group_job >>  join_data >> save_final_data >> upload_json_to_s3