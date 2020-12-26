import pandas as pd
from airflow.utils.dates import days_ago
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG

import datetime as dt
import os
import logging

logging.basicConfig(format='%(levelname)s - %(asctime)s: %(message)s',
                    encoding='utf-8', 
                    level=logging.INFO)


default_args = {
    'owner': 'Ignacio',
    'start_date': dt.datetime(2020, 12, 26),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


def load_to_db(query, i):
    try:
        odbc_hook = OdbcHook(odbc_conn_id='con_ej_pi_mssql',
                             database='Testing_ETL', driver='{ODBC Driver 17 for SQL Server}')
        odbc_hook.run(query)
        return i + 1
    except Exception as e:
        logging.error(f'Ejercucion de Query: {query} - {e}')
    return i


def load_csv():
    try:
        df = pd.read_csv(
            "https://gen2cluster.blob.core.windows.net/challenge/csv/nuevas_filas.csv?sv=2019-12-12&ss=b&srt=sco&sp=rx&se=2021-01-31T23:41:30Z&st=2020-12-21T15:41:30Z&spr=https&sig=HGmabI8sYoiQ1%2FXWb7alGqtL0s4ewWXkeAklUhmetqU%3D"
        )
    except Exception as e:
        logging.error(f'Request de CSV: {e}')
    return df


def transform(df, col_name):
    df[col_name] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return df


def etl_pipe(**kwargs):
    df = load_csv()
    df = transform(df, 'FECHA_COPIA')

    cols = ",".join([str(i) for i in df.columns.tolist()])
    cant = 0

    for i, row in df.iterrows():
        cant = load_to_db(
            f"INSERT INTO [dbo].[Unificado] ({cols}) VALUES {tuple(row)}", cant)
    logging.info(f'Cant Filas Registradas: {cant}')


with DAG('v5',
         default_args=default_args,
         schedule_interval='0 5 * * 1',
         tags=['PI Consulting']
         ) as dag:

    etl_pipe = PythonOperator(task_id='etl_pipe', python_callable=etl_pipe)
