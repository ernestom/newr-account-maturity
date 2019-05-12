
import json
import os

from insights_cli_view import get_cmdline_args
from newrelic_query_api import NewRelicQueryAPI


def do_query():
    pass


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

    elif args['command'] == 'query':
        do_query()

    elif args['command'] == 'batch-local':
        do_batch_local()

    elif args['command'] == 'batch-google':
        do_batch_google()

    elif args['command'] == 'batch-insights':
        do_batch_insights()

    print(args)

if __name__ == "__main__":
    main()