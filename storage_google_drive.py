import csv
import os
import time

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from google_sheets_helpers import *

SHEET_DEFAULT_COLUMNS=26

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
                body = {'role': 'writer', 'type': 'user', 'emailAddress': self.__writers}
                self.__permissions.create(fileId=object_id, body=body).execute()

            if self.__readers:
                body = {'role': 'reader', 'type': 'user', 'emailAddress': self.__readers}
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
            body = {"requests": [add_sheet_request(sheet_name)]}
            response = self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            sheet_id = \
                response.get('replies', [{}])[0].get('addSheet', {}).get('properties', {}).get('sheetId', None)
            self.__fit_sheet_rows(spreadsheet_id, sheet_id)
            just_created = True
        else:
            just_created = False

        return sheet_id, just_created

    def __fit_sheet_columns(self, spreadsheet_id, sheet_id, total_columns=SHEET_DEFAULT_COLUMNS):
        """ set the total number of columns on the sheet """

        if total_columns > SHEET_DEFAULT_COLUMNS:
            requests = [
                append_dimension_request(sheet_id, "COLUMNS", total_columns - SHEET_DEFAULT_COLUMNS)
            ]

        elif total_columns < SHEET_DEFAULT_COLUMNS:
            requests = [
                delete_dimension_request(sheet_id, "COLUMNS", 0, SHEET_DEFAULT_COLUMNS - total_columns)
            ]
        else:
            requests = None
        
        if requests:
            body = {"requests": requests}
            self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def __fit_sheet_rows(self, spreadsheet_id, sheet_id):
        """ set the total number of rows to 1 """

        requests = [
            delete_dimension_request(sheet_id, "ROWS", 1, 1001)
        ]

        body = {"requests": requests}
        self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def __append_dataset(self, spreadsheet_id, sheet_id, sheet_data=[]):
        """ append new rows to a sheet from a list (rows) of a list (columns) of values """

        if len(sheet_data) > 0:
            rows = [{"values": [cell_snippet(cell) for cell in row]} for row in sheet_data]

            requests = [
                append_cells_request(sheet_id, rows)
            ]

            body = {"requests": requests}
            self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def get_handle(self, spreadsheet_name, sheet_name):
        """ return a (spreadsheet,sheet) handle and a flag if just created """

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

    def get_accounts(self, spreadsheet_id, _range='Sheet1'):
        """ return a list of accounts dictionaries """

        request = self.__spreadsheets.values().get(spreadsheetId=spreadsheet_id, range=_range)
        response = request.execute()

        values = response.get('values', [])
        accounts = []
        if len(values) > 1:
            headers, rows = values[0], values[1:]
            for row in rows:
                accounts.append({headers[k]:v for k,v in enumerate(row)})
    
        return accounts
        
    def dump_metrics(self, name, data, metadata={}):
        """ append the data to the spreadsheet/sheet decorating each row with optional metadata """

        assert name.count('/') == 1,\
        'error: invalid name. it must follow spreadsheet/sheet nomenclature'

        if type(data) == list and len(data) > 0:
            if len(metadata) > 0:
                for row in data:
                    row.update(metadata)

            spreadsheet_name, sheet_name = name.split('/')
            (spreadsheet_id, sheet_id), just_created = self.get_handle(spreadsheet_name, sheet_name)

            if just_created:
                headers = list(data[0].keys())
                sheet_data = [headers]
                self.__fit_sheet_columns(spreadsheet_id, sheet_id, len(headers))
            else:
                sheet_data = []
            
            for row in data:
                sheet_data.append(list(row.values()))

            self.__append_dataset(spreadsheet_id, sheet_id, sheet_data)


    def format_spreadsheets(self, summary_pivot=None, apm_pivot=None):
        """ format every spreadsheet create to enhance end-user experience """

        requests_queue = {}
        for k,v in iter(self.__cache.items()):
            if type(k) == tuple:
                (_,sheet_name), (spreadsheet_id,sheet_id) = k, v

                if not spreadsheet_id in requests_queue:
                    requests_queue[spreadsheet_id] = []

                if summary_pivot and sheet_name == 'SUMMARY':
                    request = get_summary_pivot_request(spreadsheet_id, sheet_id, summary_pivot)
                    if request:
                        requests_queue[spreadsheet_id].extend(request)

                if apm_pivot and sheet_name == 'APM':
                    request = get_apm_pivot_request(spreadsheet_id, sheet_id, apm_pivot)
                    if request:
                        requests_queue[spreadsheet_id].extend(request)

                requests_queue[spreadsheet_id].extend([
                    basic_filter_request(sheet_id),
                    format_header_request(sheet_id),
                    freeze_header_request(sheet_id),
                    auto_resize_dimension_request(sheet_id)
                ])

            else:
                if not v in requests_queue:
                    requests_queue[v] = []
                requests_queue[v].append(delete_sheet_request(0))

        for spreadsheet_id,requests in iter(requests_queue.items()):
            body = {"requests": requests}
            self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
