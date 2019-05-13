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
    local_output_folder_path = config.get('local_output_folder_path', '')
    local_account_list_path = config.get('local_account_list_path', '')
    google_output_folder_id = config.get('google_output_folder_id', '')
    google_account_list_id = config.get('google_account_list_id', '')
    google_account_list_sheet = config.get('google_account_list_sheet', 'Sheet1')
    google_secret_file_path = config.get('google_secret_file_path', '')
    new_relic_insights_api_key = config.get('new_relic_insights_api_key', '')
    new_relic_insights_api_account_id = config.get('new_relic_insights_api_account_id', '')
    pivots = config.get('pivots', {})
    del config

    if not (bool(local_account_list_path) ^ bool(google_account_list_id)):
        abort('error: one and only one input list can be set (local or google)')

    if not google_secret_file_path and google_output_folder_id:
        abort('error: found a google output folder without a google secret file')

    if not google_secret_file_path and google_account_list_id:
        abort('error: found a google input list without a google secret file')

    if bool(new_relic_insights_api_key) ^ bool(new_relic_insights_api_account_id):
        abort('error: both a new relic insights key and account id must be set')

    input_local = bool(local_account_list_path)
    input_google = bool(google_account_list_id)
    output_local = bool(local_output_folder_path)
    output_google = bool(google_output_folder_id)
    output_insights = bool(new_relic_insights_api_key)

    return locals()


def inject_metadata(data, metadata):
    """ inject the metadata at the beginning of the dictionary """

    for index,row in enumerate(data):
        _row = {}
        _row.update(metadata)
        _row.update(row)
        data[index] = _row


def dump_data(config):
    timestamp = int(time.time())

    # setup the required input and output instances
    if config['input_local'] or config['output_local']:
        local_storage = StorageLocal(
            config['local_account_list_path'],
            config['local_output_folder_path'],
            'MATURITY',
            time.localtime(timestamp)
        )

    if config['input_google'] or config['output_google']:
        google_storage = StorageGoogleDrive(
            config['google_account_list_id'], 
            config['google_output_folder_id'], 
            config['google_secret_file_path'],
            'MATURITY',
            time.localtime(timestamp)
        )

    if config['output_insights']:
        insights_storage = StorageNewRelicInsights(
            config['local_account_list_path'],
            config['new_relic_insights_api_account_id'], 
            config['new_relic_insights_api_key'],
            timestamp
        )

    # get the accounts list
    if config['input_local']:
        accounts = local_storage.get_accounts()
    elif config['input_google']:
        accounts = google_storage.get_accounts()
    else:
        accounts = []

    # traverse, extract, store maturity metrics from all accounts
    for index, account in enumerate(accounts):

        # get account fields
        account_master = account['master_name']
        account_id = account['account_id']
        account_name = account['account_name']
        rest_api_key = account['rest_api_key']

        # can do a better progression log...
        print('{}/{}: {} - {}'.format(
            index + 1,
            len(accounts),
            account_id,
            account_name
        ))

        # get metrics from current account
        account_maturity = NewRelicAccountMetrics(rest_api_key)
        account_summary, apm_apps, browser_apps, mobile_apps = account_maturity.metrics()
        
        # collect and inject the required metadata
        metadata = {
            'master_name': account_master,
            'account_id': account_id,
            'account_name': account_name,
            'datetime': to_datetime(timestamp)
        }
        inject_metadata(account_summary, metadata)
        inject_metadata(apm_apps, metadata)
        inject_metadata(browser_apps, metadata)
        inject_metadata(mobile_apps, metadata)

        # dump metrics to local storage
        if config['output_local']:
            local_storage.dump_data(SUMMARY_NAME, account_summary)
            local_storage.dump_data(account_master + '_' + APM_NAME, apm_apps)
            local_storage.dump_data(account_master + '_' + BROWSER_NAME, browser_apps)
            local_storage.dump_data(account_master + '_' + MOBILE_NAME, mobile_apps)

        # dump metrics to google storage
        if config['output_google']:
            google_storage.dump_data(SUMMARY_NAME + '/' + SUMMARY_NAME, account_summary)
            google_storage.dump_data(account_master + '/' + APM_NAME, apm_apps)
            google_storage.dump_data(account_master + '/' + BROWSER_NAME, browser_apps)
            google_storage.dump_data(account_master + '/' + MOBILE_NAME, mobile_apps)
        
        # dump metrics to insights storage 
        if config['output_insights']:
            insights_storage.dump_data(SUMMARY_NAME, account_summary)
            insights_storage.dump_data(APM_NAME, apm_apps)
            insights_storage.dump_data(BROWSER_NAME, browser_apps)
            insights_storage.dump_data(MOBILE_NAME, mobile_apps)

    if config['output_google']:
        google_storage.format_spreadsheets(config['pivots'])

def main():
    try:
        config = get_config()
        dump_data(config)
    except Exception as error:
        print(error.args)

if __name__ == '__main__':
    main()