from config import DB_USERNAME, DB_PASSWORD, DB_SERVERNAME
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Use the environment variable
KEY_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Verify the file exists
if not os.path.exists(KEY_PATH):
    raise FileNotFoundError(f"The key file was not found: {KEY_PATH}")

def extract_data_from_google_sheets():
    try:
        # Authenticate using the service account key file
        credentials = service_account.Credentials.from_service_account_file(
            KEY_PATH, 
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        service = build('sheets', 'v4', credentials=credentials)
        
        spreadsheet_id = '1fR5ndBmCXXJrgQH_mD5wxnhTO5dTe3NeKd4zPBY-2Qk'  # Replace with your actual spreadsheet ID
        range_ = 'data!A1:L1001'  # Adjust range as needed
        
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
        data = result.get('values', [])
        
        if not data:
            logging.warning('No data found in Google Sheet.')
            return None
        
        df = pd.DataFrame(data[1:], columns=data[0])
        df.columns = df.columns.str.strip()
        
        return df
    except Exception as e:
        logging.error(f'Failed to extract data from Google Sheets. Error: {str(e)}')
        return None


def load_data(df_str):
    if df_str is None:
        logging.warning('No data to load.')
        return
    
    try:
        df = pd.read_json(df_str)
        connection_string = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVERNAME}/TestPiplineAPI?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
        engine = create_engine(connection_string)
        
        with engine.begin() as connection:
            df.fillna('')  # Replace NaN with empty string
            df.to_sql('dreamAPI', con=connection, if_exists='append', index=False)
        
        logging.info('Data loaded successfully into MSSQL.')
    except Exception as e:
        logging.error(f'Failed to load data into MSSQL. Error: {str(e)}')

with DAG(
    "GoogleSheet_to_MSSQL",
    start_date=datetime(2024, 6, 5),
    schedule_interval=None,
    tags=["GoogleSheet", "MSSQL"]
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_from_google_sheets',
        python_callable=extract_data_from_google_sheets,
        dag=dag,
    )


    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'df_str': '{{ ti.xcom_pull(task_ids="transform_data") }}'}
    )

    extract_task >> load_task