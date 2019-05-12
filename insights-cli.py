import csv
import json
import os
import sys
import yaml

from insights_cli_argparser import get_cmdline_args
from newrelic_query_api import NewRelicQueryAPI
from storage_local import StorageLocal


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


def validate_accounts(accounts):
    """ validates the accounts list """

    if not type(accounts) == list:
        abort('error: accounts list  expected')

    len_accounts = len(accounts)
    if not len_accounts:
        abort('error: empty account list received')
    
    account = accounts[0]
    if not type(account) == dict:
        abort('error: account dictionary expected')

    keys = ['master_name', 'account_id', 'account_name', 'query_api_key']
    for key in keys:
        if not key in account:
            abort(f'error: {key} in account dictionary expected')

    return len_accounts

   
def get_queries(query_file):
    """ returns a list of queries from an YAML file """

    _, ext = os.path.splitext(query_file)
    if ext.lower() not in ['.yml', '.yaml']:
        abort('error: YAML query file expected')

    with open_file(query_file) as f:
        try:
            queries = yaml.load(f)
        except:
            abort('error: cannot parse YAML query file')

    return queries


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


def do_batch_local(**args):
    """ batch-local command """

    query_file = args['query_file']
    account_file = args['account_file']
    output_folder = args['output_folder']

    queries = get_queries(query_file)

    storage = StorageLocal(account_file, output_folder)
    accounts = storage.get_accounts()
    len_accounts = validate_accounts(accounts)
    metadata_keys = ['master_name', 'account_id', 'account_name']

    for index, account in enumerate(accounts):

        master_name = account['master_name']
        account_id = account['account_id']
        account_name = account['account_name']
        query_api_key = account['query_api_key']

        log('{}/{}: {} - {}'.format(index+1, len_accounts, account_id, account_name))

        metadata = {k:v for k,v in account.items() if k in metadata_keys}
        api = NewRelicQueryAPI(account_id, query_api_key)

        for query in queries:
            output_file = master_name + '_' + query['name']
            events = api.events(query['nrql'], include=metadata)   
            storage.dump_data(output_file, events)


def do_batch_google(**args):
    pass


def do_batch_insights(**args):
    pass


if __name__ == "__main__":
    args, error = get_cmdline_args()
    locals()[args.command](**vars(args)) if not error else error()