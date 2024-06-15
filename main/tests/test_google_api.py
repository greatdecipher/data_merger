import os
import sys
import gspread
import logging
main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
from config import SHEETS_KEY
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build



logging.basicConfig(filemode = 'w', format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

class TestGoogleAPI:
    def __init__(self, sheet_key):
        self.sheets_key = sheet_key
        self.SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.sheets_key, self.SCOPES)
        self.credentials_file = 'your_credentials.json'
        self.api_name_drive = 'drive'
        self.api_version_drive = 'v3'

    def create_gspread_client(self, rate_limit_timeout=1000):
        # this is a client creator with timeout that can be changed.
        client = gspread.Client(auth=self.credentials)
        client.set_timeout(timeout=rate_limit_timeout)
        return client


if __name__ == "__main__":
    sheets_key = SHEETS_KEY
    api = TestGoogleAPI(sheet_key=sheets_key)
    client = api.create_gspread_client(rate_limit_timeout=2000)
    print(f"Client created: {client}")