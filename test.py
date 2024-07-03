from googleapiclient.discovery import build
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = 'se.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

creds = None 
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes = SCOPES
)
SAMPLE_SPREADSHEET_ID = '11RhMp7nr7L4JgDM6PhWjIFnlysYUen1tnT4-l6Tvtyo'
service = build('sheets', 'v4', credentials=creds)

#call
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,range="sheet1!A1:B4").execute()

print(result)