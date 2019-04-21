import csv
import os
import time

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

SHEET_DEFAULT_COLUMNS=26

def sheet_value(x):
    """ create the proper snippet to append data to a sheet depending on the value type """

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
    elif type(x) == float and x > 40000:  # hack... only floats above 40K are dates
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

    get_output_folder_name = lambda self: time.strftime('MATURITY_RUN-%Y-%m-%d %H:%M', time.localtime())

    def __init__(self, folder_id, json_keyfile_name, writers=[], readers=[]):
        self.__cache = {}

        self.__readers = readers
        self.__writers = writers

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_keyfile_name,
            [
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )
        drive = discovery.build('drive', 'v3', credentials=credentials)
        sheets = discovery.build('sheets', 'v4', credentials=credentials)

        self.__files = drive.files() # pylint: disable=no-member
        self.__permissions = drive.permissions() # pylint: disable=no-member
        self.__spreadsheets = sheets.spreadsheets() # pylint: disable=no-member

        self.output_folder_id, _ = self.__create_object('folder', self.get_output_folder_name(), folder_id)

    def __set_permissions(self, object_id):
        """ set readers / writers permission on object id """

        if object_id:
            if self.__writers:
                body = {
                    'role': 'writer',
                    'type': 'user',
                    'emailAddress': self.__writers,
                }
                self.__permissions.create(fileId=object_id, body=body).execute()

            if self.__readers:
                body = {
                    'role': 'reader',
                    'type': 'user',
                    'emailAddress': self.__readers,
                }
                self.__permissions.create(fileId=object_id, body=body).execute()
        
        return object_id

    def __get_object_id(self, object_type, object_name, parent_id):
        """ search for an object name / type under a parent id and return the id """

        assert object_type in StorageGoogleDrive.OBJECT_TYPES,\
        'error: unsuported object type'
        mime_type = StorageGoogleDrive.OBJECT_TYPES[object_type]

        query = f"'{parent_id}' in parents and name = '{object_name}' and mimeType = '{mime_type}'"
        response = self.__files.list(q=query, spaces='drive', fields='files(id)').execute()
        files = response.get('files', [])
        
        if len(files) == 1:
            object_id = files[0].get('id', None)
        else:
            object_id = None

        return object_id

    def __create_object(self, object_type, object_name, parent_id):
        """ create a new object """

        assert object_type in StorageGoogleDrive.OBJECT_TYPES,\
        'error: unsuported object type'
        mime_type = StorageGoogleDrive.OBJECT_TYPES[object_type]

        object_id = self.__get_object_id(object_type, object_name, parent_id)
        if not object_id:
            body = {'name': object_name, 'mimeType': mime_type, 'parents': [parent_id]}
            response = self.__files.create(body=body).execute()
            object_id = response.get('id', None)
            just_created = True
        else:
            just_created = False

        return object_id, just_created

    def __get_sheet_id(self, spreadsheet_id, sheet_name):
        """ search for a sheet name in a spreadsheet id and return the id """

        response = self.__spreadsheets.get(spreadsheetId=spreadsheet_id).execute()
        sheets = response.get('sheets', [])
        sheets = [k for i,k in enumerate(sheets) if k.get('name', None) == sheet_name]
        if len(sheets) == 1:
            sheet_id = sheets[0].get('properties', {}).get('sheetId', None)
        else:
            sheet_id = None

        return sheet_id

    def __create_sheet(self, spreadsheet_id, sheet_name):
        """ create a new sheet """

        sheet_id = self.__get_sheet_id(spreadsheet_id, sheet_name)
        if not sheet_id:
            requests = [{
               "addSheet": {
                    "properties": {
                        "title": sheet_name
                    }
                }
            }]

            body = {"requests": requests}
            response = self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            sheet_id = \
                response.get('replies', [{}])[0].get('addSheet', {}).get('properties', {}).get('sheetId', None)
            self.__reduce_sheet_rows(spreadsheet_id, sheet_id)
            just_created = True
        else:
            just_created = False

        return sheet_id, just_created

    def __adjust_sheet_columns(self, spreadsheet_id, sheet_id, total_columns=SHEET_DEFAULT_COLUMNS):
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
            self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def __reduce_sheet_rows(self, spreadsheet_id, sheet_id):
        """ set the total number of rows to 1 """

        requests = [{
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": 1001
                }
            }
        }]
        
        body = {"requests": requests}
        self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def __append_dataset(self, spreadsheet_id, sheet_id, sheet_data=[]):
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
        self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def get_handle(self, spreadsheet_name, sheet_name):
        if not spreadsheet_name in self.__cache:
            spreadsheet_id, just_created = self.__create_object('spreadsheet', spreadsheet_name, self.output_folder_id)
            self.__cache.update({spreadsheet_name: spreadsheet_id})
        else:
            spreadsheet_id = self.__cache[spreadsheet_name]

        if not (spreadsheet_name, sheet_name) in self.__cache:
            sheet_id, just_created = self.__create_sheet(spreadsheet_id, sheet_name)
            self.__cache.update({(spreadsheet_name,sheet_name): (spreadsheet_id,sheet_id)})
        else:
            just_created = False
        
        return self.__cache[(spreadsheet_name,sheet_name)], just_created

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
        
    def dump_metrics(self, name, data, metadata={}):
        if type(data) == list and len(data) > 0:
            if len(metadata) > 0:
                for row in data:
                    row.update(metadata)

            spreadsheet_name, sheet_name = name.split('/')
            (spreadsheet_id, sheet_id), just_created = self.get_handle(spreadsheet_name, sheet_name)

            if just_created:
                fieldnames = list(data[0].keys())
                sheet_data = [fieldnames]
                self.__adjust_sheet_columns(spreadsheet_id, sheet_id, len(fieldnames))
            else:
                sheet_data = []
            
            for row in data:
                sheet_data.append(list(row.values()))

            self.__append_dataset(spreadsheet_id, sheet_id, sheet_data)