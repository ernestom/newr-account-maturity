import time
import json
import os

from global_constants import *

from newrelic_account_metrics import NewRelicAccountMetrics

from storage_newrelic_insights import StorageNewRelicInsights
from storage_google_drive import StorageGoogleDrive
from storage_local import StorageLocal

CONFIG_FILE = 'config.json'


def to_datetime(timestamp):
    """ converts a timestamp to a Sheets / Excel datetime """

    # 1970-01-01 00:00:00 in Google Sheets / Microsoft Excel
    EPOCH_START = 25569
    SECONDS_IN_A_DAY = 86400
    return timestamp / SECONDS_IN_A_DAY + EPOCH_START


def abort(message):
    """ abort the command """

    print(message)
    exit()


def get_config():
    """ read the config settings """

    if not os.path.exists(CONFIG_FILE):
        abort('error: config.json not found')
    
    config = json.load(open(CONFIG_FILE, 'r'))
    output_folder = config.get('output_folder', '')
    account_file = config.get('account_file', '')
    output_folder_id = config.get('output_folder_id', '')
    account_file_id = config.get('account_file_id', '')
    account_sheet = config.get('account_sheet', 'Sheet1')
    secret_file = config.get('secret_file', '')
    insert_api_key = config.get('insert_api_key', '')
    insert_account_id = config.get('insert_account_id', '')
    pivots = config.get('pivots', {})
    input_local = bool(account_file)
    input_google = bool(account_file_id)
    output_local = bool(output_folder)
    output_google = bool(output_folder_id)
    output_insights = bool(insert_api_key)

    if input_local and input_google:
        abort('error: one and only one input list can be set (local or google)')

    if not secret_file and output_folder_id:
        abort('error: found a google output folder without a google secret file')

    if not secret_file and account_file_id:
        abort('error: found a google input list without a google secret file')

    if bool(insert_api_key) ^ bool(insert_account_id):
        abort('error: both a new relic insights key and account id must be set')

    del config
    return locals()


def inject_metadata(data, metadata):
    """ inject the metadata at the beginning of the dictionary """

    for index,row in enumerate(data):
        _row = {}
        _row.update(metadata)
        _row.update(row)
        data[index] = _row


def export_metrics(config):
    timestamp = int(time.time())

    # setup the required input and output instances
    if config['input_local'] or config['output_local']:
        local_storage = StorageLocal(
            config['account_file'],
            config['output_folder'],
            time.localtime(timestamp),
            'MATURITY'
        )

    if config['input_google'] or config['output_google']:
        google_storage = StorageGoogleDrive(
            config['account_file_id'], 
            config['output_folder_id'], 
            config['secret_file'],
            time.localtime(timestamp),
            'MATURITY'
        )

    if config['output_insights']:
        insights_storage = StorageNewRelicInsights(
            config['account_file'],
            config['insert_account_id'], 
            config['insert_api_key'],
            timestamp
        )

    # get the accounts list
    if config['input_local']:
        accounts = local_storage.get_accounts()
    elif config['input_google']:
        accounts = google_storage.get_accounts(config['account_sheet'])
    else:
        accounts = []

    # traverse, extract, store maturity metrics from all accounts
    for index, account in enumerate(accounts):

        # get account fields
        master_name = account['master_name']
        account_id = account['account_id']
        account_name = account['account_name']
        rest_api_key = account['rest_api_key']

        # can do a better progression log...
        print('{}/{}: {} - {}'.format(
            index+1,
            len(accounts),
            account_id,
            account_name
        ))

        # get metrics from current account
        account_maturity = NewRelicAccountMetrics(rest_api_key)
        account_summary, apm_apps, browser_apps, mobile_apps = account_maturity.metrics()
        
        # inject the required metadata in all lists
        for item in [account_summary, apm_apps, browser_apps, mobile_apps]:
            inject_metadata(
                item, {
                    'master_name': master_name,
                    'account_id': account_id,
                    'account_name': account_name,
                    'datetime': to_datetime(timestamp)
                }
            )

        # dumps the data to available storages
        storages = [
            local_storage if config['output_local'] else None,
            google_storage if config['output_google'] else None,
            insights_storage if config['output_insights'] else None
        ]
        for storage in storages:
            if storage:
                storage.dump_data(SUMMARY_NAME, account_summary)
                storage.dump_data(master_name, APM_NAME, apm_apps)
                storage.dump_data(master_name, BROWSER_NAME, browser_apps)
                storage.dump_data(master_name, MOBILE_NAME, mobile_apps)

    if config['output_google']:
        google_storage.format_data(config['pivots'])

def main():
    try:
        config = get_config()
        export_metrics(config)
    except Exception as error:
        print(error.args)

if __name__ == '__main__':
    main()