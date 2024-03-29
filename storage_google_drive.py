import json
import os
import time

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from google_sheets_helpers import *


def abort(message):
    """ abort the command """

    print(message)
    exit()

class StorageGoogleDrive():

    OBJECT_TYPES = {
        'folder': 'application/vnd.google-apps.folder',
        'spreadsheet': 'application/vnd.google-apps.spreadsheet'
    }

    def __init__(self, account_file_id, output_folder_id, secret_file, timestamp=None, prefix='RUN', writers=[], readers=[]):
        """ init """

        self.__cache = {}
        self.__account_file_id = account_file_id
        self.__output_folder_id = output_folder_id
        self.__run_folder = \
            time.strftime(
                f'{prefix}_%Y-%m-%d_%H-%M',
                time.localtime() if not timestamp else timestamp
            )
        self.__run_folder_id = None
        self.__readers = readers
        self.__writers = writers

        if not os.path.exists(secret_file):
            abort(f'error: cannot find secret file {secret_file}')

        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                secret_file,
                [
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/spreadsheets'
                ]
            )
            drive = discovery.build('drive', 'v3', credentials=credentials)
            sheets = discovery.build('sheets', 'v4', credentials=credentials)
        except:
            abort('error: cannot open a connection to Google API')

        self.__files = drive.files() # pylint: disable=no-member
        self.__permissions = drive.permissions() # pylint: disable=no-member
        self.__spreadsheets = sheets.spreadsheets() # pylint: disable=no-member

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

        if not object_type in StorageGoogleDrive.OBJECT_TYPES:
            abort('error: unsuported Google Drive object type')

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
        """ set the total number of rows to 1 by removing rows 2..1000 """

        requests = [
            delete_dimension_request(sheet_id, "ROWS", 1, SHEET_DEFAULT_ROWS)
        ]

        body = {"requests": requests}
        self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def __get_dataset(self, spreadsheet_id, _range):
        """ return a list of accounts dictionaries """

        request = self.__spreadsheets.values().get(spreadsheetId=spreadsheet_id, range=_range)
        return request.execute().get('values', [])

    def __append_dataset(self, spreadsheet_id, sheet_id, sheet_data=[]):
        """ append new rows to a sheet from a list (rows) of a list (columns) of values """

        if sheet_data:
            rows = [{"values": [cell_snippet(cell) for cell in row]} for row in sheet_data]

            requests = [
                append_cells_request(sheet_id, rows)
            ]

            body = {"requests": requests}
            self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    def __get_handle(self, spreadsheet_name, sheet_name):
        """ return a (spreadsheet,sheet) handle and a flag if just created """

        if not spreadsheet_name in self.__cache:
            spreadsheet_id, just_created = self.__create_object(
                'spreadsheet', spreadsheet_name, self.__run_folder_id)
            self.__cache.update({spreadsheet_name: spreadsheet_id})
        else:
            spreadsheet_id = self.__cache[spreadsheet_name]

        if not (spreadsheet_name, sheet_name) in self.__cache:
            sheet_id, just_created = self.__create_sheet(spreadsheet_id, sheet_name)
            self.__cache.update({(spreadsheet_name,sheet_name): (spreadsheet_id,sheet_id)})
        else:
            just_created = False

        return self.__cache[(spreadsheet_name,sheet_name)], just_created

    def get_accounts(self, sheet_range='Sheet1'):
        """ return a list of accounts dictionaries """

        values = self.__get_dataset(self.__account_file_id, sheet_range)
        accounts = []
        if len(values) > 1:
            headers, rows = values[0], values[1:]
            for row in rows:
                accounts.append({headers[k]:v for k,v in enumerate(row)})

        return accounts

    def dump_data(self, spreadsheet_name, sheet_name, data=[]):
        """ appends the data to the output spreadsheet/sheet """

        # creates the output folder on the first dump
        if not self.__run_folder_id:
            self.__run_folder_id, _ = self.__create_object(
                'folder',
                self.__run_folder,
                self.__output_folder_id
            )

        if type(data) == list and len(data):
            (spreadsheet_id, sheet_id), just_created = \
                self.__get_handle(spreadsheet_name, sheet_name)

            if just_created:
                headers = list(data[0].keys())
                sheet_data = [headers]
                self.__fit_sheet_columns(spreadsheet_id, sheet_id, len(headers))
            else:
                sheet_data = []

            sheet_data.extend([list(row.values()) for row in data])
            self.__append_dataset(spreadsheet_id, sheet_id, sheet_data)

    def format_data(self, pivots={}):
        """ format all spreadsheets / sheets in the cache """

        requests_queue = {}
        for k,v in iter(self.__cache.items()):
            if type(k) == tuple:
                (_,sheet_name), (spreadsheet_id,sheet_id) = k, v

                if not spreadsheet_id in requests_queue:
                    requests_queue[spreadsheet_id] = []

                # create a pivot table
                pivot_sheet_name = sheet_name + 'Pivot'
                pivot_sheet_id, _ = self.__create_sheet(spreadsheet_id, pivot_sheet_name)
                headers = self.__get_dataset(spreadsheet_id, sheet_name + '!1:1')[0]
                pivot = pivots.get(sheet_name, {})
                pivot_table = pivot_table_snippet(sheet_id, pivot, headers)

                # add all formatting requests to the queue
                requests_queue[spreadsheet_id].extend([
                    pivot_request(pivot_sheet_id, pivot_table),
                    basic_filter_request(sheet_id),
                    format_header_request(sheet_id),
                    freeze_rows_request(sheet_id),
                    freeze_columns_request(sheet_id, 3),
                    auto_resize_dimension_request(sheet_id)
                ])

            else:
                # remove Sheet1
                spreadsheet_id = v
                if not v in requests_queue:
                    requests_queue[spreadsheet_id] = []
                requests_queue[spreadsheet_id].append(delete_sheet_request(SHEET1_SHEET_ID))

        # post the batch request queue
        for spreadsheet_id,requests in iter(requests_queue.items()):
            body = {"requests": requests}
            self.__spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()