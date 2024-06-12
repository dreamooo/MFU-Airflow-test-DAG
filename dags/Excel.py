from config import DB_USERNAME, DB_PASSWORD, DB_SERVERNAME
import logging
logging.basicConfig(level=logging.DEBUG)
import os
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import json


def extract_data():
    dags_folder = settings.DAGS_FOLDER
    df1 = pd.read_excel(os.path.join(dags_folder, 'mockupmaster1.xlsx'))
    df2 = pd.read_excel(os.path.join(dags_folder, 'mockupmaster2.xlsx'))
    
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()
    df2 = df2.rename(columns={'STAFFCODEs': 'STAFFCODE'})
    
    merged_df = pd.merge(df1, df2, how='right', on='STAFFCODE')
    return merged_df



def transform_data(ti):
    df = ti.xcom_pull(task_ids='extract_data')
    selected_columns = ['POSITIONIDs', 'POSITIONSTATUSs', 'STAFFCODE' ,'POSITIONNAMEs', 'ACADEMICPOSITION', 'WORKPOSITIONs','STAFFTYPE','DEPARTMENTNAMEs','DEPARTMENTID','ADMITDATE', 'EXPIREDATEs','STAFFSEXs','STAFFAGEs','DEGREELEVELs','GENERATIONs', 'GROUPEXPERIENCEs','BIRTHPROVINCENAMEs','YEAREXPERIENCEs','STAFFSTATUSs', 'SUBDEPARTMENTs','REMARK2s','Timestamp','OLDSTAFFs','PREFIXNAMEENG', 'STAFFNAMEENG','STAFFSURNAMEENG']
    transformed_df = df[selected_columns]
    transformed_df.insert(loc=transformed_df.columns.get_loc('REMARK2s')+1, column='fiscal_year_rate', value=None)
    return transformed_df.to_json(orient='records')

def load_data(df_str):
    df = pd.read_json(df_str)
    connection_string = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVERNAME}/dreamEX?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        df.to_sql('dreamEX', con=connection, if_exists='append', index=False)

with DAG(
    "dreamEX",
    start_date=datetime(2024, 3, 30),
    schedule_interval= None,
    tags=["dreamEX"]
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
    )

    load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
    op_kwargs={'df_str': '{{ ti.xcom_pull(task_ids="transform_data") }}'}
    )

    extract_task >> transform_task >> load_task