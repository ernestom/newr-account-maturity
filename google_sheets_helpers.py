#!env/bin/python

SHEET1_SHEET_ID=0
SHEET_DEFAULT_COLUMNS=26
SHEET_DEFAULT_ROWS=1000

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
        'addSheet': {
            'properties': {
                'title': title
            }
        }
    }


def delete_sheet_request(sheet_id):
    return {
        'deleteSheet': {
            'sheetId': sheet_id
        }
    }


def append_dimension_request(sheet_id, dimension, length):
    return {
        'appendDimension': {
            'sheetId': sheet_id,
            'dimension': dimension,
            'length': length
        }
    }


def delete_dimension_request(sheet_id, dimension, start_index=0, end_index=1):
    return {
        'deleteDimension': {
            'range': {
                'sheetId': sheet_id,
                'dimension': dimension,
                'startIndex': start_index,
                'endIndex': end_index
            }
        }
    }


def append_cells_request(sheet_id, rows):
    return {
        'appendCells': {
            'sheetId': sheet_id,
            'rows': rows,
            'fields': '*',
        }
    }


def pivot_request(sheet_id, pivot_sheet_id, rows=[], columns=[], values=[]):
    return {
        'updateCells': {
            'rows': [
                {
                    'values': [
                        {
                            'pivotTable': {
                                'source': {
                                    'sheetId': sheet_id
                                },
                                'rows': rows,
                                'columns': columns,
                                'values': values,
                                'valueLayout': 'HORIZONTAL'
                            }
                        }
                    ]
                }
            ],
            'start': {
                'sheetId': pivot_sheet_id
            },
            'fields': 'pivotTable'
        }
    }


def summary_pivot_request(sheet_id, pivot_sheet_id):
    rows = [
        {
            'sourceColumnOffset': 0,
            'showTotals': True,
            'sortOrder': 'ASCENDING'
        },
        {
            'sourceColumnOffset': 2,
            'showTotals': True,
            'sortOrder': 'ASCENDING'
        }   
    ]
    values = [
        {
            'sourceColumnOffset': 32,
            'summarizeFunction': 'SUM',
            'name': 'APM Total'
        },
        {
            'sourceColumnOffset': 30,
            'summarizeFunction': 'SUM',
            'name': 'APM w/ Labels'
        },
        {
            'sourceColumnOffset': 28,
            'summarizeFunction': 'SUM',
            'name': 'APM w/ Conditions'
        }  
    ]

    return pivot_request(sheet_id, pivot_sheet_id, rows=rows, columns=[], values=values)


def apm_pivot_request(sheet_id, pivot_sheet_id):
    rows = [
        {
            'sourceColumnOffset': 6,
            'showTotals': True,
            'sortOrder': 'ASCENDING'
        },
        {
            'sourceColumnOffset': 2,
            'showTotals': True,
            'sortOrder': 'ASCENDING'
        }   
    ]
    values = [
        {
            'sourceColumnOffset': 4,
            'summarizeFunction': 'COUNT',
            'name': 'APM Total'
        },
        {
            'sourceColumnOffset': 7,
            'summarizeFunction': 'SUM',
            'name': 'Conditions Total'
        },
        {
            'sourceColumnOffset': 8,
            'summarizeFunction': 'SUM',
            'name': 'Labels Total'
        }  
    ]

    return pivot_request(sheet_id, pivot_sheet_id, rows=rows, columns=[], values=values)


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


def freeze_rows_request(sheet_id, frozen_row_count=1):
    return {
        'updateSheetProperties': {
            'properties': {
                'sheetId': sheet_id,
                'gridProperties': {
                    'frozenRowCount': frozen_row_count
                }
            },
            'fields': 'gridProperties.frozenRowCount'
        }
    }


def freeze_columns_request(sheet_id, frozen_column_count=1):
    return {
        'updateSheetProperties': {
            'properties': {
                'sheetId': sheet_id,
                'gridProperties': {
                    'frozenColumnCount': frozen_column_count
                }
            },
            'fields': 'gridProperties.frozenColumnCount'
        }
    }


def auto_resize_dimension_request(sheet_id, dimension='COLUMNS'):
    return {
        'autoResizeDimensions': {
            'dimensions': {
                'sheetId': sheet_id,
                'dimension': dimension
            }
        }
    }