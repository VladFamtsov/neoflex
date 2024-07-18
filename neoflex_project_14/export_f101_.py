from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import datetime

default_args = {'owner': 'Famtsov',
                'start_date': days_ago(1),
                'schedule_interval': "('0 0 1 1 *')",
                }
offset = datetime.timezone(datetime.timedelta(hours=3))


dag = DAG(dag_id='export_f101_to_csv',
          default_args=default_args,
          catchup=False
          )


def export_f101_to_local_csv(**kwargs):
    export_start = datetime.datetime.now(offset).strftime("%Y-%m-%d %H:%M:%S")
    csv_export_path = '/opt/airflow/dags/files/f101.csv'
    hook = PostgresHook(postgres_conn_id='postgres_neoflex_work')
    engine = hook.get_sqlalchemy_engine()
    query = """select * 
                 from dm.dm_f101_round_f;""" #Допустим, это будет разовая операция экспорта и фильтры нам не нужны
    df = pd.read_sql(con=engine, sql=query)
    df.to_csv(path_or_buf=csv_export_path, sep=',', index=False)
    export_finish = datetime.datetime.now(offset).strftime("%Y-%m-%d %H:%M:%S")
    kwargs['ti'].xcom_push(key='key_export_start', value=export_start)
    kwargs['ti'].xcom_push(key='key_export_finish', value=export_finish)


def logging_export_func(**kwargs):
    query = f"""insert into logs.load(
        id, schema_name, table_name,
        stage_name, start_stage, end_stage, description
            ) values (
            nextval('logs.load_id_seq'), 'dm', 'dm_f101_round_f',
            'export data', 
            '{kwargs['ti'].xcom_pull(key='key_export_start')}',
            '{kwargs['ti'].xcom_pull(key='key_export_finish')}',
             'export data to local csv')"""
    hook = PostgresHook(postgres_conn_id='postgres_neoflex_work')
    hook.run(query)

export_f101_task = PythonOperator(
    task_id='f101_export_task',
    dag=dag,
    python_callable=export_f101_to_local_csv
)

logging_export_task = PythonOperator(
    task_id='f101_logging_task',
    dag=dag,
    python_callable=logging_export_func
)

export_f101_task >> logging_export_task