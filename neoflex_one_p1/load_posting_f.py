from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
import pandas as pd
import time
import datetime


schema_name = 'ds'
table_name = 'ft_posting_f'
stage_name = 'load data'
description = ''

dag = DAG(
    dag_id='posting_f',
    start_date=days_ago(0),
    schedule_interval=None
)

def read_csv_func():
    csv_file_path = '/opt/airflow/dags/files/ft_posting_f.csv'
    ds = pd.read_csv(csv_file_path, delimiter=';', dayfirst=True)
    return ds

def start_load_data_func():
    hook = PostgresHook(postgres_conn_id='postgres_neoflex')
    engine = hook.get_sqlalchemy_engine()
    load_start = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    read_csv_func().to_sql(name=table_name, con=engine, if_exists='replace', index=False,
                           schema=schema_name)
    return load_start

def end_load_data_func():
    time.sleep(5)
    load_finish = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return load_finish


read_csv = PythonOperator(
    task_id='read_csv_to_dataset',
    python_callable=read_csv_func,
    dag=dag
)

start_load_data = PythonOperator(
    task_id='load_dataset_to_psql',
    python_callable=start_load_data_func,
    dag=dag
)

end_load_data = SQLExecuteQueryOperator(
    task_id='logging',
    conn_id='postgres_neoflex',
    sql=f"""insert into logs.load (id, schema_name, table_name, stage_name, 
                                   start_stage, end_stage, description)
                    values (nextval('logs.load_id_seq'), '{schema_name}', '{table_name}', 
                            '{stage_name}',  '{start_load_data_func()}', 
                            '{end_load_data_func()}', '{description}');""",
    dag=dag
)

read_csv >> start_load_data >> end_load_data



