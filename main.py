import time
import json
import os

from newrelic_account_maturity import NewRelicAccountMaturity

from storage_newrelic_insights import StorageNewRelicInsights
from storage_google_drive import StorageGoogleDrive
from storage_local import StorageLocal


CONFIG_FILE = 'config.json'
GOOGLE_SHEETS_EPOCH = 25569 # 1970-01-01 00:00:00 in Google Sheets
SECONDS_IN_A_DAY = 86400


def get_config():
    """ read the config settings """

    assert os.path.exists(CONFIG_FILE),\
    'error: config.json not found'
    
    config = json.load(open(CONFIG_FILE, 'r'))
    local_output_folder_path = config.get('local_output_folder_path', '')
    local_account_list_path = config.get('local_account_list_path', '')
    google_output_folder_id = config.get('google_output_folder_id', '')
    google_account_list_id = config.get('google_account_list_id', '')
    google_secret_file_path = config.get('google_secret_file_path', '')
    new_relic_insights_api_key = config.get('new_relic_insights_api_key', '')
    new_relic_insights_api_account_id = config.get('new_relic_insights_api_account_id', '')
    del config

    assert bool(local_account_list_path) ^ bool(google_account_list_id),\
    'error: one and only one input list can be set (local or google)'

    assert google_secret_file_path or not google_output_folder_id,\
    'error: found a google output folder without a google secret file'

    assert google_secret_file_path or not google_account_list_id,\
    'error: found a google input list without a google secret file'

    assert not (bool(new_relic_insights_api_key) ^ bool(new_relic_insights_api_account_id)),\
    'error: both a new relic insights key and account id must be set'

    input_source = 'local' if bool(local_account_list_path) else 'google'
    output_local = bool(local_output_folder_path)
    output_google = bool(google_output_folder_id)
    output_insights = bool(new_relic_insights_api_key)

    return locals()


def dump_metrics(config):
    timestamp = int(time.time())
    updated_at = timestamp / SECONDS_IN_A_DAY + GOOGLE_SHEETS_EPOCH

    # setup the required input and output instances
    if config['input_source'] == 'local' or config['output_local']:
        local_storage = StorageLocal(
            config['local_output_folder_path']
        )
    
    if config['input_source'] == 'google' or config['output_google']:
        google_storage = StorageGoogleDrive(
            config['google_output_folder_id'], 
            config['google_secret_file_path']
        )

    if config['output_insights']:
        insights_storage = StorageNewRelicInsights(
            config['new_relic_insights_api_account_id'], 
            config['new_relic_insights_api_key']
        )

    # get the accounts list
    if config['input_source'] == 'local':
        accounts = local_storage.get_accounts(config['local_account_list_path'])
    elif config['input_source'] == 'google':
        accounts = google_storage.get_accounts(config['google_account_list_id'])
    else:
        accounts = []

    for index, account in enumerate(accounts):
        # can do a better log output :-)
        print('{}/{}: {} - {}'.format(
            index + 1,
            len(accounts),
            account['account_id'],
            account['account_name']
        ))

        # timestamp and updated_at are the same date under different formats (Insights, Google)
        metadata = {'updated_at': updated_at, 'timestamp': timestamp}

        # only collect non-sensitive metadata from account dictionary
        for key in sorted(account):
            if not 'key' in key:
                metadata[key] = account[key]

        # get metrics from current account
        account_maturity = NewRelicAccountMaturity(account['rest_api_key'])
        account_summary, apm_apps, browser_apps, mobile_apps = account_maturity.metrics()
        account_master = account['master_name']

        # dump metrics to local storage
        if config['output_local']:
            local_storage.dump_metrics('SUMMARY', [account_summary], metadata)
            local_storage.dump_metrics(account_master + '_APM', apm_apps, metadata)
            local_storage.dump_metrics(account_master + '_BROWSER', browser_apps, metadata)
            local_storage.dump_metrics(account_master + '_MOBILE', mobile_apps, metadata)

        # dump metrics to google storage
        if config['output_google']:
            google_storage.dump_metrics('SUMMARY/SUMMARY', [account_summary], metadata)
            google_storage.dump_metrics(account_master + '/APM', apm_apps, metadata)
            google_storage.dump_metrics(account_master + '/BROWSER', browser_apps, metadata)
            google_storage.dump_metrics(account_master + '/MOBILE', mobile_apps, metadata)
        
        # dump metrics to insights storage 
        # must be called at the very last as it adds an eventType attribute to every metric row
        if config['output_insights']:
            insights_storage.dump_metrics('Summary', [account_summary], metadata)
            insights_storage.dump_metrics('ApmDetails', apm_apps, metadata)
            insights_storage.dump_metrics('BrowserDetails', browser_apps, metadata)
            insights_storage.dump_metrics('MobileDetails', mobile_apps, metadata)

    if config['output_google']:
        google_storage.format_spreadsheets()

def main():
    #try:
        config = get_config()
        dump_metrics(config)
    #except Exception as error:
    #    print(error.args)

if __name__ == '__main__':
    main()