import os
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Path to your service account credentials JSON file
SERVICE_ACCOUNT_FILE = '/path/to/your/service-account-file.json'
# Google Sheets ID and range
SPREADSHEET_ID = 'your-spreadsheet-id'
RANGE_NAME = 'Sheet1'  # Adjust range as needed

def extract_data_from_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client = gspread.authorize(credentials)

        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(RANGE_NAME)
        values = sheet.get_all_values()

        if not values:
            raise ValueError("No data found in the Google Sheets")

        df = pd.DataFrame(values[1:], columns=values[0])  # Assuming first row is header
        logger.debug("Extracted data from Google Sheets: %s", df.head())
        return df.to_dict('records')
    except Exception as e:
        logger.error("Error in extract_data_from_sheets: %s", e)
        raise

def transform_data(ti):
    try:
        df_dict = ti.xcom_pull(task_ids='extract_data_from_sheets')
        if not df_dict:
            raise ValueError("No data received from extract_data_from_sheets task")
        
        df = pd.DataFrame(df_dict)
        logger.debug("Transformed data: %s", df.head())
        return df.to_json(orient='records')
    except Exception as e:
        logger.error("Error in transform_data: %s", e)
        raise

def load_data_to_sql(df_str):
    try:
        df = pd.read_json(df_str)
        connection_string = 'mssql+pyodbc://sa:xxxxxxxxxxxx@xxxxxxxxxx/dreamEX?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
        engine = create_engine(connection_string)
        with engine.begin() as connection:
            df.to_sql('dreamEX', con=connection, if_exists='append', index=False)
        logger.info("Loaded data into the database successfully")
    except Exception as e:
        logger.error("Error in load_data_to_sql: %s", e)
        raise

with DAG(
    "GoogleSheetsToSQLServer",
    start_date=datetime(2024, 3, 30),
    schedule_interval=None,
    tags=["GoogleSheets", "SQLServer"]
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_from_sheets',
        python_callable=extract_data_from_sheets,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id='load_data_to_sql',
        python_callable=load_data_to_sql,
        dag=dag,
        op_kwargs={'df_str': '{{ ti.xcom_pull(task_ids="transform_data") }}'}
    )

    extract_task >> transform_task >> load_task
