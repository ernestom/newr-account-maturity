import csv
import json
import os
import sys
import yaml

from insights_cli_argparser import get_cmdline_args
from newrelic_query_api import NewRelicQueryAPI
from storage_local import StorageLocal
from storage_google_drive import StorageGoogleDrive
from storage_newrelic_insights import StorageNewRelicInsights


def abort(message):
    """ abort the command """

    print(message)
    exit()


def log(message):
    """ lazy man log """

    print(message)


def open_file(filename=None, mode='r', *args, **kwargs):
    """ open method facade for regular files, stdin and stdout """

    if filename == '-':
        if 'r' in mode:
            stream = sys.stdin
        else:
            stream = sys.stdout
        if 'b' in mode:
            handler = stream.buffer
        else:
            handler = stream
    else:
        try:
            handler = open(filename, mode, *args, **kwargs)
        except:
            if filename == '-':
                if 'r' in mode:
                    filename = 'stdin'
                else:
                    filename = 'stdout'
            abort(f'error: cannot open {filename}')

    return handler


def get_vault(vault_file):
    """ returns a dict of keys from a CSV file """ 

    _, ext = os.path.splitext(vault_file)
    if ext.lower() not in ['.csv']:
        abort('error: CSV query file expected')

    with open_file(vault_file) as f:
        try:
            csv_reader = csv.DictReader(f, delimiter=',')
            return {
                row['secret']: {
                    'account_id': row['account_id'], 
                    'query_api_key': row['query_api_key']
                } for row in csv_reader
            }
        except:
            abort('error: cannot parse CSV vault file')


def get_queries(query_file):
    """ returns a list of queries from an YAML file """

    _, ext = os.path.splitext(query_file)
    if ext.lower() not in ['.yml', '.yaml']:
        abort('error: YAML query file expected')

    with open_file(query_file) as f:
        try:
            queries = yaml.load(f, Loader=yaml.FullLoader)
        except:
            abort('error: cannot parse YAML query file')

    return queries


def validate_input(subject, data, keys):
    """ validates the subjected input """

    if not type(data) == list:
        abort(f'error: {subject} list expected')

    len_data = len(data)
    if len_data:
        item = data[0]
        if not type(item) == dict:
            abort(f'error: {subject} dictionary expected')

        for key in keys:
            if not key in item:
                abort(f'error: {key} in account dictionary expected')

    return len_data


def do_query(**args):
    """ query command """

    query = args['query']
    account_id = args['account_id']
    query_api_key = args['query_api_key']
    output_file = args['output_file']
    output_format = args['output_format']

    if not account_id:
        account_id = os.getenv('NEW_RELIC_ACCOUNT_ID', '')
    if not account_id:
        abort('account id not provided and env NEW_RELIC_ACCOUNT_ID not set')

    if not query_api_key:
        query_api_key = os.getenv('NEW_RELIC_QUERY_API_KEY', '')
    if not query_api_key:
        abort('query api key not provided and env NEW_RELIC_QUERY_API_KEY not set')

    if not output_file:
        output_file = '-'

    if query == '-':
        with open_file(query) as f:
            nrql = f.read()
    else:
        nrql = query

    api = NewRelicQueryAPI(account_id, query_api_key)
    events = api.events(nrql, include={'account_id': account_id})

    if not len(events):
        abort('error: empty events list returned')

    try:
        with open_file(output_file, 'w') as f:
            if output_format == 'json':
                json.dump(events, f, sort_keys=True, indent=4)
            else:
                csv_writer = csv.DictWriter(f, fieldnames=events[0].keys())
                csv_writer.writeheader()
                for event in events:
                    csv_writer.writerow(event)
    except:
        abort(f'error: cannot write to {output_file}')


def export_accounts_events(storage, vault_file, query_file, accounts):

    vault = get_vault(vault_file) if vault_file else {}

    queries = get_queries(query_file)
    len_queries = validate_input('queries', queries, ['name', 'nrql'])

    metadata_keys = ['master_name', 'account_id', 'account_name', 'query_api_key']
    len_accounts = validate_input('accounts', accounts, metadata_keys)
    metadata_keys.remove('query_api_key')

    for idx_account, account in enumerate(accounts):

        master_name = account['master_name']
        account_name = account['account_name']

        metadata = {k:v for k,v in account.items() if k in metadata_keys}

        for idx_query, query in enumerate(queries):

            name = query['name']
            nrql = query['nrql']
            secret = query.get('secret', None)

            if secret:
                if not secret in vault:
                    abort(f'error: cannot find {secret} in vault')
                account_id = vault[secret]['account_id']
                query_api_key = vault[secret]['query_api_key']
            else:
                account_id = account['account_id']
                query_api_key = account['query_api_key']

            log('account {}/{}: {} - {}, query {}/{}: {}'.format(
                idx_account+1, len_accounts, account_id, account_name,
                idx_query+1, len_queries, name)
            )
 
            api = NewRelicQueryAPI(account_id, query_api_key)
            events = api.events(query['nrql'], include=metadata)
            storage.dump_data(master_name, query['name'], events)


def do_batch_local(**args):
    """ batch-local command """

    vault_file = args['vault_file']
    query_file = args['query_file']
    account_file = args['account_file']
    output_folder = args['output_folder']
    
    storage = StorageLocal(account_file, output_folder)
    accounts = storage.get_accounts()

    export_accounts_events(storage, vault_file, query_file, accounts)


def do_batch_google(**args):
    """ batch-local command """

    vault_file = args['vault_file']
    query_file = args['query_file']
    account_file_id = args['account_file_id']
    output_folder_id = args['output_folder_id']
    secret_file = args['secret_file']

    storage = StorageGoogleDrive(account_file_id, output_folder_id, secret_file)
    accounts = storage.get_accounts()
     
    export_accounts_events(storage, vault_file, query_file, accounts)

    # add some nice formatting to all Google Sheets
    storage.format_data()


def do_batch_insights(**args):
    """ batch-insights command """

    vault_file = args['vault_file']
    query_file = args['query_file']
    account_file = args['account_file']
    insert_account_id = args['insert_account_id']
    insert_api_key = args['insert_api_key']

    storage = StorageNewRelicInsights(account_file, insert_account_id, insert_api_key)
    accounts = storage.get_accounts()

    export_accounts_events(storage, vault_file, query_file, accounts)

if __name__ == "__main__":
    args, error = get_cmdline_args()
    locals()[args.command](**vars(args)) if not error else error()