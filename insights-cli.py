import csv
import json
import os
import sys

from insights_cli_view import get_cmdline_args
from newrelic_query_api import NewRelicQueryAPI


def abort(message):
    """ abort the command """

    print(message)
    exit()

def _open(filename: str = None, mode: str = 'r', *args, **kwargs):
    """ file open method that can handle stdin and stdout """

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
        handler = open(filename, mode, *args, **kwargs)

    return handler


def do_query(**args):

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

    if query == '-':
        with _open(query) as f:
            nrql = f.read()
    else:
        nrql = query

    api = NewRelicQueryAPI(account_id, query_api_key)
    events = api.events(nrql, include={'account_id': account_id})

    if not output_file:
        output_file = '-'

    with _open(output_file, 'w') as f:
        if output_format == 'json':
            json.dump(events, f, sort_keys=True, indent=4)
        else:
            if len(events):
                csv_writer = csv.DictWriter(f, fieldnames=events[0].keys())
                csv_writer.writeheader()
                for event in events:
                    csv_writer.writerow(event)


def do_batch_local():
    pass


def do_batch_google():
    pass


def do_batch_insights():
    pass


def main():
    args, error = get_cmdline_args()
    if error:
        error()
    else:
        globals()[args.command](**vars(args))

if __name__ == "__main__":
    main()