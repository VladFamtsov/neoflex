from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.xcom import XCom
from airflow.utils.dates import days_ago
import pandas as pd
import time
import datetime

schema_name = 'ds'
table_name = 'md_currency_d'
stage_name = 'first load data'
description = ''
offset = datetime.timezone(datetime.timedelta(hours=3))
file_encoding='cp1252'

dag = DAG(
    dag_id=f'load_{schema_name}.any_table',
    start_date=days_ago(1),
    schedule_interval=('0 0 1 1 *'),
    catchup=False
)

def read_csv_func():
    csv_file_path = f'/opt/airflow/dags/files/{table_name}.csv'
    df = pd.read_csv(csv_file_path, delimiter=';', dayfirst=True, encoding=file_encoding)
    return df

def write_into_table_func(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_neoflex_work')
    engine = hook.get_sqlalchemy_engine()
    load_start = datetime.datetime.now(offset).strftime("%Y-%m-%d %H:%M:%S")
    read_csv_func().to_sql(con=engine, name=table_name, schema=schema_name, if_exists='replace',
                           index=False)
    kwargs['ti'].xcom_push(key='key_load_start', value=load_start)

def write_log(**kwargs):
    time.sleep(5)
    load_finish = datetime.datetime.now(offset).strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        insert into logs.load (id, schema_name, table_name, stage_name, 
                                   start_stage, end_stage, description)
                    values (nextval('logs.load_id_seq'), '{schema_name}', '{table_name}', 
                            '{stage_name}',  '{kwargs['ti'].xcom_pull(key='key_load_start')}', 
                            '{load_finish}', '{description}');
    """
    hook = PostgresHook(postgres_conn_id='postgres_neoflex_work')
    hook.run(query)

read_csv_task = PythonOperator(
    task_id='read_csv_for_ft_balance_f',
    dag=dag,
    python_callable=read_csv_func)

write_table_task = PythonOperator(
    task_id='write_data_into_table',
    dag=dag,
    python_callable=write_into_table_func
)

end_load_data_task = PythonOperator(
    task_id='logging',
    dag=dag,
    python_callable=write_log
)

read_csv_task >> write_table_task >> end_load_data_task
