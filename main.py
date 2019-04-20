import time

from newrelic_account_maturity import NewRelicAccountMaturity

from storage_newrelic_insights import StorageNewRelicInsights
from storage_google_drive import StorageGoogleDrive
from storage_local import StorageLocal

def main():
    updated_at = int(time.time())

    local_storage = StorageLocal('/Users/pmonteiro/newr-account-maturity')
    insights_storage = StorageNewRelicInsights(2315900)
    google_storage = StorageGoogleDrive(
        '1JCh2seqmxBrVfLMUEi1FjjA85M16i2rN', 'client_secrets.json', writers=['pmonteiro@newrelic.com']
    )

    accounts = local_storage.get_accounts('maturity-input')

    for index, account in enumerate(accounts):
        # can do a better log output :-)
        print('{}/{}: {} - {}'.format(
            index + 1,
            len(accounts),
            account['account_id'],
            account['account_name']
        ))

        # only collect non-sensitive metadata from account dictionary
        metadata = {'updated_at': updated_at}
        for key in sorted(account):
            if not 'key' in key:
                metadata[key] = account[key]

        # get metrics from current account
        account_maturity = NewRelicAccountMaturity(account['rest_api_key'])
        account_summary, apm_apps, browser_apps, mobile_apps = account_maturity.metrics()

        # dump Summary metrics
        local_storage.dump_metrics('SUMMARY', [account_summary], metadata)
        insights_storage.dump_metrics('Summary', [account_summary], metadata)
        google_storage.dump_metrics('SUMMARY', [account_summary], metadata)

        # consolidate output based on master name
        account_master = account['master_name']

        # dump APM metrics
        local_storage.dump_metrics(account_master + '_APM', apm_apps, metadata)
        insights_storage.dump_metrics('ApmDetails', apm_apps, metadata)
        #google_storage.dump_metrics(account_master + '_APM', apm_apps, metadata)

        # dump Browser metrics
        local_storage.dump_metrics(account_master + '_BROWSER', browser_apps, metadata)
        insights_storage.dump_metrics('BrowserDetails', apm_apps, metadata)
        #google_storage.dump_metrics(account_master + '_BROWSER', browser_apps, metadata)

        # dump Mobile metrics
        local_storage.dump_metrics(account_master + '_MOBILE', mobile_apps, metadata)
        insights_storage.dump_metrics('MobileDetails', apm_apps, metadata)
        #google_storage.dump_metrics(account_master + '_MOBILE', mobile_apps, metadata)

if __name__ == '__main__':
    main()
