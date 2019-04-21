#!env/bin/python

def cell_snippet(x):
    ''' create the proper snippet to append data to a sheet depending on the value type '''

    if type(x) == str:
        return {
            'userEnteredValue': {
                'stringValue': x
            }
        }
    elif type(x) == bool:
        return {
            'userEnteredValue': {
                'boolValue': x
            }
        }
    elif type(x) == int:
        return {
            'userEnteredValue': {
                'numberValue': x
            }, 
            'userEnteredFormat': {
                'numberFormat': {
                    'type': 'NUMBER',
                    'pattern': '#,##0'
                }
            }
        }
    elif type(x) == float and x > 40000:  # hack... only floats above 40K are dates
        return {
            'userEnteredValue': {
                'numberValue': x
            }, 
            'userEnteredFormat': {
                'numberFormat': {
                    'type': 'DATE'
                }
            }
        }
    else:
        return {
            'userEnteredValue': {
                'numberValue': x
            }, 
            'userEnteredFormat': {
                'numberFormat': {
                    'type': 'NUMBER',
                    'pattern': '#,##0.00'
                }
            }
        }


def add_sheet_request(title):
    return {
        "addSheet": {
            "properties": {
                "title": title
            }
        }
    }


def delete_sheet_request(sheet_id):
    return {
        "deleteSheet": {
            "sheetId": sheet_id
        }
    }


def append_dimension_request(sheet_id, dimension, length):
    return {
        "appendDimension": {
            "sheetId": sheet_id,
            "dimension": dimension,
            "length": length
        }
    }


def delete_dimension_request(sheet_id, dimension, start_index=0, end_index=1):
    return {
        "deleteDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": dimension,
                "startIndex": start_index,
                "endIndex": end_index
            }
        }
    }


def append_cells_request(sheet_id, rows):
    return {
        "appendCells": {
            "sheetId": sheet_id,
                "rows": rows,
                "fields": "*",
        }
    }


def get_summary_pivot_request(spreadsheet_id, sheet_id, summary_pivot):
    return ''


def get_apm_pivot_request(spreadsheet_id, sheet_id, summary_pivot):
    return ''


def basic_filter_request(sheet_id):
    return {
        'setBasicFilter': {
            'filter': {
                'range': {
                    'sheetId': sheet_id
                }
            }
        }
    }


def format_header_request(sheet_id):
    return {
        'repeatCell': {
            'range': {
            'sheetId': sheet_id,
            'startRowIndex': 0,
            'endRowIndex': 1
            },
            'cell': {
                'userEnteredFormat': {
                    'horizontalAlignment' : 'CENTER',
                    'textFormat': {
                        'bold': True
                    }
                }
            },
            'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
        }
    }


def freeze_header_request(sheet_id):
    return {
        'updateSheetProperties': {
            'properties': {
                'sheetId': sheet_id,
                'gridProperties': {
                    'frozenRowCount': 1
                }
            },
            'fields': 'gridProperties.frozenRowCount'
        }
    }


def auto_resize_dimension_request(sheet_id, dimension='COLUMNS'):
    return {
        'autoResizeDimensions': {
            'dimensions': {
                'sheetId': sheet_id,
                'dimension': dimension,
                'startIndex': 0
            }
        }
    }


def __get_sheet_format_requests(sheet_id):
    return [
        {
            'setBasicFilter': {
                'filter': {
                    'range': {
                        'sheetId': sheet_id
                    }
                }
            }
        },
        {
            'autoResizeDimensions': {
                'dimensions': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0
                }
            }
        },
        {
            'repeatCell': {
                'range': {
                'sheetId': sheet_id,
                'startRowIndex': 0,
                'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment' : 'CENTER',
                        'textFormat': {
                            'bold': True
                        }
                    }
                },
                'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
            }
        },
        {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': 1
                    }
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        }
    ]
