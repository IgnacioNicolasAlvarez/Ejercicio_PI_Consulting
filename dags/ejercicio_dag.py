import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.dates import days_ago

import pandas as pd


def get_data():

    try:
        df = pd.read_csv(
            "https://gen2cluster.blob.core.windows.net/challenge/csv/nuevas_filas.csv?sv=2019-12-12&ss=b&srt=sco&sp=rx&se=2021-01-31T23:41:30Z&st=2020-12-21T15:41:30Z&spr=https&sig=HGmabI8sYoiQ1%2FXWb7alGqtL0s4ewWXkeAklUhmetqU%3D"
        )
    except Exception as e:
        pass

    print(df.head())


def load_data_mssql(**kwargs):
    odbc_hook = OdbcHook(odbc_conn_id='admin_con', database='Testing_ETL', driver='{ODBC Driver 17 for SQL Server}')
    query = 'insert into dbo.aux (num) values (1)'
    odbc_hook.run(query)



default_args = {
    'owner': 'Ignacio',
    'start_date': dt.datetime(2020, 12, 26),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}



def load_data_mssql(**kwargs):
    odbc_hook = OdbcHook(odbc_conn_id='con_ej_pi_mssql', database='Testing_ETL', driver='{ODBC Driver 17 for SQL Server}')
    query = 'insert into dbo.aux (num) values (1)'
    odbc_hook.run(query)


def extract_data_csv():

    try:
        df = pd.read_csv(
            "https://gen2cluster.blob.core.windows.net/challenge/csv/nuevas_filas.csv?sv=2019-12-12&ss=b&srt=sco&sp=rx&se=2021-01-31T23:41:30Z&st=2020-12-21T15:41:30Z&spr=https&sig=HGmabI8sYoiQ1%2FXWb7alGqtL0s4ewWXkeAklUhmetqU%3D"
        )
    except Exception as e:
        pass

    print(df.head())


with DAG('aaaaaaaaaa_v2',
         default_args=default_args,
         schedule_interval='20 * * * *',
         tags=['PI Consulting']
         ) as dag:

    load_data = PythonOperator(task_id='load_data', python_callable=load_data_mssql)

load_data

