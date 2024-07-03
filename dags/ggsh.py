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
import json
import numpy as np


# Use the environment variable
KEY_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Verify the file exists
if not os.path.exists(KEY_PATH):
    raise FileNotFoundError(f"The key file was not found: {KEY_PATH}")




class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if pd.isna(obj):
            return None
        return super(NpEncoder, self).default(obj)

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
        
        # Convert DataFrame to JSON string, handling NaN values
        json_data = json.dumps(df.where(pd.notnull(df), None).to_dict(orient='records'), cls=NpEncoder)
        
        logging.info(f"Successfully extracted {len(df)} rows from Google Sheets.")
        return json_data
    except Exception as e:
        logging.error(f'Failed to extract data from Google Sheets. Error: {str(e)}')
        return None
    
    
def load_data(json_data):
    if json_data is None:
        logging.warning('No data to load.')
        return
    
    try:
        df = pd.DataFrame(json.loads(json_data))
        
        # Print column names for debugging
        logging.info(f"Columns in DataFrame: {df.columns.tolist()}")
        
        connection_string = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVERNAME}/TestPiplineAPI?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
        engine = create_engine(connection_string)
        
        with engine.begin() as connection:
            # Get the existing column names from the SQL table
            existing_columns = pd.read_sql_query("SELECT TOP 0 * FROM dreamAPI", connection).columns.tolist()
            
            logging.info(f"Columns in SQL table: {existing_columns}")
            
            # Filter the DataFrame to include only the columns that exist in the SQL table
            df_filtered = df[df.columns.intersection(existing_columns)]
            
            # Replace None with empty string for string columns
            for col in df_filtered.select_dtypes(include=['object']).columns:
                df_filtered[col] = df_filtered[col].fillna('')
            
            # Insert the data
            df_filtered.to_sql('dreamAPI', con=connection, if_exists='append', index=False)
        
        logging.info(f'Successfully loaded {len(df_filtered)} rows into MSSQL.')
    except Exception as e:
        logging.error(f'Failed to load data into MSSQL. Error: {str(e)}')
        raise  # Re-raise the exception to mark the task as failed

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
        op_kwargs={'json_data': '{{ ti.xcom_pull(task_ids="extract_data_from_google_sheets") }}'},
        dag=dag,
        
    )

    extract_task  >> load_task