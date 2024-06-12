from config import DB_USERNAME, DB_PASSWORD, DB_SERVERNAME #here
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import json



def extract_data():
    url1 = "https://my.api.mockaroo.com/mockupdata1_forp.json?key=299b8490"
    url2 = "https://my.api.mockaroo.com/mockupdata2_forp.json?key=299b8490"
    r1 = requests.get(url1)
    r2 = requests.get(url2)
    data1 = r1.json()
    data2 = r2.json()
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
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
    #here
    connection_string = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVERNAME}/TestPiplineAPI?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        df.to_sql('dreamAPI', con=connection, if_exists='append', index=False)

with DAG(
    "API_to_MSSQL",
    start_date=datetime(2024, 6, 5),
    schedule_interval= None,
    tags=["MSSQLtest"]
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