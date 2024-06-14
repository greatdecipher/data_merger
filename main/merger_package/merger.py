import os
main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
import re
import gspread
import pandas as pd
import logging
import time
import sys
import warnings
import threading
import itertools
import time
from config import SHEETS_KEY
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime


logging.basicConfig(filemode = 'w', format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

class GoogleAppsAuto():
    def __init__(self, user_email, directory_link, editor_emails, filename):
        # Google Drive API setup
        self.sheets_key = SHEETS_KEY
        self.SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
        self.credentials = service_account.Credentials.from_json_keyfile_dict(self.sheets_key, self.SCOPES)
        self.client = gspread.Client(auth=self.credentials)
        self.client.set_timeout(1000)
        self.credentials_file = 'your_credentials.json'
        self.api_name_drive = 'drive'
        self.api_version_drive = 'v3'
        self.editor_emails = editor_emails
        self.directory_link = directory_link
        self.filename = filename
        self.user_email = user_email
        self.existing_sheet_names = []
        self.sheet_ids = []
        self.color_text = {
            'green':'\033[0;32m',
            'blue':'\033[0;34m',
            'cyan':'\033[0;36m',
            'yellow':'\033[1;33m',
            'reset':'\033[0m',
        }

    def parse_dir_url(self) -> str:
        data = re.findall(r"(?:v=|\/)([0-9A-Za-z_-]{33}).*", self.directory_link)
        if data:
            self.directory_id = data[0]
            print(data[0])
        return ""

    def get_current_timestamp(self):
        current_time = datetime.now()
        current_time_srt = current_time.strftime("%Y-%m-%d_%H:%M:%S")
        print(f"Generating Timestamp.....{current_time_srt}")
        return current_time_srt

    def conn_gapps_api(self):
        self.drive_service = build(self.api_name_drive, self.api_version_drive, credentials=self.credentials)
        print(f"Connecting to Gdrive API with credentials Service object: {self.drive_service}.....")
        print(f"Connecting to GSheets API with credentials Service object: {self.client}.....")

    def get_excels_from_dir(self):
        self.excel_files = self.drive_service.files().list(q=f"'{self.directory_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'", fields="files(id, name)").execute()
        self.excel_files_count = len(self.excel_files['files'])
        print(self.excel_files_count)
        print(self.excel_files)

    def get_gsheet_names(self):
        print(f"Getting Gsheets for Directory ID: {self.directory_id}.....")
        existing_gsheets = self.drive_service.files().list(q=f"'{self.directory_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'", fields="files(id, name)").execute()
        for s in existing_gsheets['files']:
            if "consolidated" not in s['name'].lower():
                self.existing_sheet_names.append(s['name'])
        print(self.existing_sheet_names)


    def conv_xlsx_to_gsheets(self):
        for file in self.excel_files['files']:
            file_id = file['id']
            file_name = file['name']

            raw_file_name = file_name.replace('.xlsx', '')
            if raw_file_name not in self.existing_sheet_names:
                # Convert XLSX to Google Sheets
                attempts = 10
                brand_name = raw_file_name.split("-")[0].strip()
                for i in range(attempts):
                    try:
                        print(f"Attempt no.{i + 1}\nConverting '{brand_name}' xlsx to Google Sheet...")
                        conversion_request = self.drive_service.files().copy(fileId=file_id, body={"mimeType": "application/vnd.google-apps.spreadsheet"}).execute()
                        break
                    except Exception as e:
                        if i < attempts - 1:
                                print("Error, retrying function.....")
                                self.countdown(10)
                        else:
                            print(f"Used all {attempts} Attempts")
                            raise # Exception("API Rate Limit Restoration Failed.....")

                print("Converted from xlsx to Google Sheets.....")

                # Get the ID of the newly created Google Sheets file
                self.google_sheets_file_id = conversion_request['id']
                print(f"Google sheets generated file ID: {self.google_sheets_file_id}")

                for editor_email in self.editor_emails:
                    self.drive_service.permissions().create(
                    fileId=self.google_sheets_file_id,
                            body={
                            "type": "user",
                            "role": "writer",
                            "emailAddress": editor_email
                            }
                    ).execute()

                google_sheets_file = self.drive_service.files().get(fileId=self.google_sheets_file_id,
                                                        fields="webViewLink").execute()

                google_sheets_url = google_sheets_file['webViewLink']
                self.sheet_ids.append(self.google_sheets_file_id)

                print(f"XLSX file '{file_name}' has been converted to Google Sheets. URL: {google_sheets_url}\n")

            else:
                print(file_name + '\t already converted')

    def conso_data_from_df(self):
        # initializing the google sheet list of files

        # Start consolidation once all the files are converted and uploaded succesfully
        gsheets_list = self.drive_service.files().list(q=f"'{self.directory_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'", fields="files(id, name)").execute()
        while not self.excel_files_count <= len(gsheets_list['files']):
            gsheets_list = self.drive_service.files().list(q=f"'{self.directory_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'", fields="files(id, name)").execute()
            count = self.excel_files_count - len(gsheets_list['files'])
            if count < 0:
                count = 0
            print(f"Converted files not yet uploaded. Waiting for {count} file/s to be uploaded.....")
            time.sleep(5)

        print("All files have been converted. Consolidating data now.....")
        print(gsheets_list['files'])

        #Loop through each sheet ID and read data
        self.df_list = []
        total_count = 0
        #for gsheet in gsheets_list:
        for gsheet in gsheets_list['files']:

            self.file_id = gsheet['id']
            file_name = gsheet['name']
            """ This needs to catch the APIError for gspread..."""

            if "consolidated" in file_name.lower():
                continue
            else:
                print(file_name)
                attempts = 20
                for i in range(attempts):
                    try:
                        print(f"Attempt no.{i + 1}")
                        sheet = self.client.open_by_key(self.file_id)
                        worksheet = sheet.get_worksheet(0)  # Assuming data is in the first worksheet

                        # Read data from individual gsheets
                        data = worksheet.get_all_values()

                        data_df = pd.DataFrame(data)

                        data_df.columns = data_df.iloc[0]
                        data_df = data_df[1:]  # Remove the first row from the data

                        total_count = total_count + len(data_df.index)

                        self.df_list.append(data_df)

                    except gspread.exceptions.APIError as e:
                        if i < attempts - 1:
                            print("API Error, retrying function.....")
                            self.countdown(10)
                            continue
                        else:
                            print(f"Used all {attempts} Attempts")
                            raise Exception("API Rate Limit Restoration Failed.....")
                    break


        warnings.filterwarnings("ignore")
        print('Processed Row Count = ' + str(total_count))
        print("Data from Multiple GSheets Processed Successfully.....")



    def transfer_from_df_to_Gsheet(self):
        self.combined_fname_timestamp = f"{self.filename}_{self.get_current_timestamp()}"
        combined_data = pd.concat(self.df_list).fillna('')

        # Create a new Google Sheet for combined data
        self.combined_sheet = self.client.create(self.combined_fname_timestamp)

        # Convert the combined_data DataFrame back to a list of lists
        combined_values = combined_data.values.tolist()
        cols = combined_data.columns.values.tolist()

        print('Combined row count ' + str(len(combined_values)))

        # Update the combined Google Sheet with the combined data
        combined_worksheet = self.combined_sheet.get_worksheet(0)

        combined_worksheet.update('A1', [cols])
        print("Updated column headers")

        # Share the sheet with the editor
        for email in self.editor_emails:
            self.combined_sheet.share(email, perm_type='user', role='writer')
        # Get the URL of the combined Google Sheet
        self.combined_sheet_url = self.combined_sheet.url

        self.chunk_data_upload(combined_values, combined_worksheet)
        # print(f"The Created Sheet metadata is: {self.combined_sheet.fetch_sheet_metadata()}")


    def combine_function(self, range_cell, worksheet, values):
        attempts = 20
        for i in range(attempts):
            try:
                print("Proceeding on updating values")
                worksheet.update(range_cell, values)

            except gspread.exceptions.APIError as e:
                if i < attempts - 1:
                        print(f"API Error: {self.color_text['yellow']}{e}{self.color_text['reset']}, retrying function.....")
                        self.countdown(10)
                        continue
                else:
                    print(f"Used all {attempts} Attempts")
                    raise Exception("API Rate Limit Restoration Failed.....")
            break


    def chunk_data_upload(self, value, worksheet):
        chunk_size = len(value) // 2
        second_chunk = 'A{}'.format(chunk_size + 1)
        data_chunk1 = value[:chunk_size]
        data_chunk2 = value[chunk_size:]
        print("Chunked data into two")
        # Update the first chunk
        self.combine_function('A2', worksheet, data_chunk1)
        print("Uploaded first chunk")
        print(f"The Created Sheet filename is: {self.combined_sheet.title}")
        self.upload_to_drive()
        # Update the second chunk
        self.combine_function(second_chunk, worksheet, data_chunk2)
        print("Uploaded second chunk")


    def add_and_rename_sheet(self):
        worksheet_name = 'Unique Data'
        self.combined_sheet.add_worksheet(title=worksheet_name,rows="1000",cols="5",index=1)
        print(f"Sheet Added with title: {worksheet_name}.....")
        attempts = 20
        for i in range(attempts):
            try:
                # worksheet_name = 'Unique Data'
                # self.combined_sheet.add_worksheet(title=worksheet_name,rows="1000",cols="5",index=1)
                # print(f"Sheet Added with title: {worksheet_name}.....")
                self.combined_sheet.sheet1.update_title('Orders')
                print("Renamed sheet1 to 'Orders'.")
                break
            except gspread.exceptions.APIError as e:
                # if "already exists" in str(e):
                #     print(f"Sheet with title '{worksheet_name}' already exists. Please enter another name.")
                #     break  # Exit the loop if the sheet already exists
                if i < attempts - 1:
                    print(f"Failed to rename sheet1. Error: {e}")
                    self.countdown(10)
                else:
                    print(f"Used all {attempts} Attempts")
                    raise



    def fill_second_sheet(self):
        self.df_list_second = []
        headers = ['Marketplace Order Id', 'Brand', 'Inventory SKU Qty', 'Marketplace', 'Order Date']
        order_worksheet = self.combined_sheet.get_worksheet(0)
        attempts = 20
        for i in range(attempts):
            try:
              print("Proceeding to fill second sheet with unique data")
              specified_values = order_worksheet.get_all_values()
              print("Got all values")

            except gspread.exceptions.APIError as e:
                if i < attempts - 1:
                        print("API Error, retrying function.....")
                        self.countdown(10)
                        continue
                else:
                    print(f"Used all {attempts} Attempts")
                    raise Exception("API Rate Limit Restoration Failed.....")
            break

        zip_data = zip(*(e for e in zip(*specified_values) if e[0] in headers))
        df_second_to_process = pd.DataFrame(zip_data, columns=headers)
        df_second_no_dupli = df_second_to_process.drop_duplicates(subset='Marketplace Order Id', keep="first")
        df_second = df_second_no_dupli[1:]

        self.df_list_second.append(df_second)
        print("Appended from Dataframe to List.....")
        combined_data_second = pd.concat(self.df_list_second).fillna('')

        combined_values = combined_data_second.values.tolist()
        chunk_size_two = len(combined_values) // 2
        second_chunk = 'A{}'.format(chunk_size_two + 1)
        print('Combined row count ' + str(len(combined_values)))
        print(f"If divided by two chunks: {chunk_size_two}")
        print(second_chunk)
        unique_chunk1 = combined_values[:chunk_size_two]
        unique_chunk2 = combined_values[chunk_size_two:]
        print("Chunked unique data into two")
        cols = ['Marketplace Order Id', 'Brand', 'Product Qty', 'Marketplace', 'Order Date']

        combined_worksheet_two = self.combined_sheet.get_worksheet(1)
        combined_worksheet_two.clear()
        print("Clear all cells first")
        combined_worksheet_two.update('A1', [cols])

        # Update the first chunk
        self.combine_function('A2', combined_worksheet_two, unique_chunk1)
        print("Uploaded first unique chunk")
        # Update the second chunk
        self.combine_function(second_chunk, combined_worksheet_two, unique_chunk2)
        print("Uploaded second unique chunk")


    def set_sheet_properties(self):
        sheet_to_fit = self.combined_sheet.get_worksheet(0)
        sheet_to_fit.format("A1:AW1",{"textFormat":{"bold":True}})
        sheet_to_fit_two =  self.combined_sheet.get_worksheet(1)
        sheet_to_fit_two.format("A1:Z1",{"textFormat":{"bold":True}})
        print("Set Sheet Properties.....")


    def upload_to_drive(self):
        try:
            uploaded_file = self.drive_service.files().update(fileId=self.combined_sheet.id, addParents=self.directory_id, removeParents='root').execute()
            print(f"Uploading Sheet Object: {uploaded_file}")
            print("Uploaded Sheet Successfully.....")
            print(f"{self.color_text['green']}Combined data has been successfully written to the new Google Sheet:{self.color_text['reset']} {self.color_text['blue']}{self.combined_sheet.url}{self.color_text['reset']}")

        except Exception as e:
            logging.exception(e)


    def countdown(self, secs):
        for i in range(secs, 0, -1):
            print(f"{self.color_text['cyan']}Retrying Request {str(i)} sec(s){self.color_text['reset']}")
            time.sleep(1)


    def thread_data_process(self, func_idx):
        list_of_func_to_thread = [self.transfer_from_df_to_Gsheet, self.set_sheet_properties]
        thread = threading.Thread(target=list_of_func_to_thread[func_idx])
        thread.start()
        for c in itertools.cycle(['...']):
            print(f"{self.color_text['blue']}\rLoading Data Consolidation{c}{self.color_text['reset']}")
            time.sleep(0.1)
            if not thread.is_alive():
                break
        print('Transfer Process Done!     ')


    def main(self):
        self.parse_dir_url()
        self.conn_gapps_api()
        self.get_excels_from_dir()
        self.get_gsheet_names()
        self.conv_xlsx_to_gsheets()
        self.conso_data_from_df()
        self.transfer_from_df_to_Gsheet()
        self.add_and_rename_sheet()
        self.fill_second_sheet()
        self.thread_data_process(1)
        self.upload_to_drive()


if __name__ == "__main__":
    # test
    """Set your Email, Directory link, Editors and filename here....."""
    user_email = 'robinignaciosky70@gmail.com' # please put your email here.

    #test gdrive directory
    directory_link = "https://drive.google.com/drive/folders/1obpzqT7VeD5vCUZJjqs8eNECkCibFh_6"

    editor_emails = []

    filename_combined_sheet = 'Consolidated_file'      #this will be generated with an '_' and timestamp...

    """Executions"""
    google_app_actions = GoogleAppsAuto(user_email, directory_link, editor_emails, filename_combined_sheet)
    google_app_actions.main()

