import os
import sys
import gspread
import logging
main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
from config import SHEETS_KEY
from googleapiclient.discovery import build
from google.oauth2 import service_account


logging.basicConfig(filemode = 'w', format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

class TestGoogleAPI:
    def __init__(self) -> None:
        self.sheets_key = SHEETS_KEY
        self.SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
        self.credentials = service_account.Credentials.from_json_keyfile_dict(self.sheets_key, self.SCOPES)
        self.client = gspread.Client(auth=self.credentials)
        self.client.set_timeout(1000)
        self.credentials_file = 'your_credentials.json'
        self.api_name_drive = 'drive'
        self.api_version_drive = 'v3'

    def create_client(self):
        pass

