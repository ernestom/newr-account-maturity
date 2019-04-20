import csv
import os
import time

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

SHEET_DEFAULT_COLUMNS=26

def sheet_value(x):
    """ create the proper JSON to append data to a sheet depending on the value type """

    if type(x) == str:
        return {
            "userEnteredValue": {
                "stringValue": x
            }
        }
    elif type(x) == bool:
        return {
            "userEnteredValue": {
                "boolValue": x
            }
        }
    elif type(x) == int:
        return {
            "userEnteredValue": {
                "numberValue": x
            }, 
            "userEnteredFormat": {
                "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "#,##0"
                }
            }
        }
    # ducktape hack... Only floats above 40K in my data are DATES
    elif type(x) == float and x > 40000: 
        return {
            "userEnteredValue": {
                "numberValue": x
            }, 
            "userEnteredFormat": {
                "numberFormat": {
                    "type": "DATE"
                }
            }
        }
    else:
        return {
            "userEnteredValue": {
                "numberValue": x
            }, 
            "userEnteredFormat": {
                "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "#,##0.00"
                }
            }
        }

class StorageGoogleDrive():

    OBJECT_TYPES = {
        'folder': 'application/vnd.google-apps.folder',
        'spreadsheet': 'application/vnd.google-apps.spreadsheet'
    }

    def __init__(self, folder_id, json_keyfile_name=None, writers=[], readers=[]):
        self.files = {}
        self.folder_id = folder_id

        self.__readers = readers
        self.__writers = writers

        if not json_keyfile_name:
            os.getenv('GOOGLE_SERVICE_ACCOUNT_KEYFILE_NAME', '')
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_keyfile_name,
            [
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )
        self.__drive = discovery.build('drive', 'v3', credentials=credentials)
        self.__sheets = discovery.build('sheets', 'v4', credentials=credentials)

        output_folder = time.strftime('MATURITY_RUN-%Y-%m-%d %H:%M', time.localtime())
        self.output_folder_id, _ = self.__create_object('folder', output_folder, folder_id)

    def __set_permissions(self, object_id=None):
        """ set readers / writers permission on object id """

        if object_id:
            if self.__writers:
                body = {
                    'role': 'writer',
                    'type': 'user',
                    'emailAddress': self.__writers,
                }
                self.__drive.permissions().create(fileId=object_id, body=body).execute() # pylint: disable=no-member

            if self.__readers:
                body = {
                    'role': 'reader',
                    'type': 'user',
                    'emailAddress': self.__readers,
                }
                self.__drive.permissions().create(fileId=object_id, body=body).execute() # pylint: disable=no-member
        
        return object_id

    def __get_object_id(self, mime_type, object_name, parent_id):
        """ return the id from the the object name """

        response = self.__drive.files().list( # pylint: disable=no-member
            q=f"'{parent_id}' in parents and name = '{object_name}' and mimeType = '{mime_type}'",
            spaces='drive',
            fields='files(id)'
        ).execute()
        files = response.get('files', [])

        if len(files) == 1:
            object_id = files[0].get('id', None)
        else:
            object_id = None

        return object_id

    def __create_object(self, object_type, object_name, parent_id):
        """ create a new object """

        if not object_type in StorageGoogleDrive.OBJECT_TYPES:
            raise Exception('error: unsuported object type')

        mime_type = StorageGoogleDrive.OBJECT_TYPES[object_type]
        object_id = self.__get_object_id(mime_type, object_name, parent_id)
        if not object_id:
            body = {
                'name': object_name,
                'mimeType': mime_type,
                'parents': [parent_id]
            }
            response = self.__drive.files().create(body=body).execute() # pylint: disable=no-member
            object_id = response['id']
            just_created = True
        else:
            just_created = False

        return object_id, just_created

    def __sheet_adjust_size(self, spreadsheet_id, sheet_id=0, total_columns=SHEET_DEFAULT_COLUMNS):
        """ set the total number of columns on a sheet """

        if total_columns > SHEET_DEFAULT_COLUMNS:
            requests = [{
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "length": total_columns - SHEET_DEFAULT_COLUMNS
                }
            }]
        elif total_columns < SHEET_DEFAULT_COLUMNS:
            requests = [{
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": SHEET_DEFAULT_COLUMNS - total_columns
                    }
                }
            }]
        else:
            requests = None
        
        if requests:
            body = {"requests": requests}
            spreadsheets = self.__sheets.spreadsheets() # pylint: disable=no-member
            spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def __sheet_append(self, spreadsheet_id, sheet_id=0, sheet_data=[]):
        """ append new rows to a sheet """

        total_rows = len(sheet_data)
        if total_rows == 0:
            raise Exception('error: empty sheet data')

        rows = [{"values": [sheet_value(cell) for cell in row]} for row in sheet_data]
        requests = [{
            "appendCells": {
                "sheetId": sheet_id,
                 "rows": rows,
                    "fields": "*",
            }
        }]

        body = {"requests": requests}
        spreadsheets = self.__sheets.spreadsheets() # pylint: disable=no-member
        spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def get_file_handle(self, name):
        if not name in self.files:
            spreadsheet_id, just_created = self.__create_object('spreadsheet', name, self.output_folder_id)
            self.files.update({name: spreadsheet_id})
        else:
            just_created = False
        return self.files[name], just_created

    def get_accounts(self, spreadsheet_id):
        spreadsheets = self.__sheets.spreadsheets() # pylint: disable=no-member
        request = spreadsheets.values().get(spreadsheetId=spreadsheet_id, range='AccountsList')
        response = request.execute()
        values = response.get('values', [])
        accounts = []
        if len(values) > 1:
            headers, rows = values[0], values[1:]
            for row in rows:
                accounts.append({headers[k]:v for k,v in enumerate(row)})
        return accounts
        
    def dump_metrics(self, name, data=[], metadata={}):
        if type(data) == list and len(data) > 0:
            if len(metadata) > 0:
                for row in data:
                    row.update(metadata)
            spreadsheet_id, just_created = self.get_file_handle(name)

            if just_created:
                fieldnames = list(data[0].keys())
                sheet_data = [fieldnames]
                self.__sheet_adjust_size(spreadsheet_id, 0, len(fieldnames))
            else:
                sheet_data = []
            
            for row in data:
                sheet_data.append(list(row.values()))
            self.__sheet_append(spreadsheet_id, 0, sheet_data)