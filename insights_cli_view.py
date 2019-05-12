import argparse

def get_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.set_defaults(command=None)
    subparsers = parser.add_subparsers()
    
    prepare_query_parser(subparsers)
    prepare_batch_local_parser(subparsers)
    prepare_batch_google_parser(subparsers)
    prepare_batch_insights_parser(subparsers)
    
    args = parser.parse_args()
    error = parser.print_help if args.command == None else None

    return args, error


def prepare_query_parser(subparsers):
    query_parser = subparsers.add_parser('query')
    query_parser.set_defaults(command='do_query')
    query_parser.add_argument('-k', '--query-api-key',
        help='New Relic Insights query API key'
    )
    query_parser.add_argument('-a', '--account-id',
        help='New Relic account ID',
        type=int
    )
    query_parser.add_argument('-o', '--output-file',
        help='output file name'
    )
    query_parser.add_argument('-f', '--output-format',
        help='output type',
        choices=['json', 'csv'],
        default='json'
    )
    query_parser.add_argument('-q', '--query',
        help='New Relic Insights query language https://docs.newrelic.com/docs/insights/nrql-new-relic-query-language/nrql-reference/nrql-syntax-components-functions',
        required=True
    )

def prepare_batch_local_parser(subparsers):
    batch_local_parser = subparsers.add_parser('batch-local')
    batch_local_parser.set_defaults(command='do_batch_local')
    batch_local_parser.add_argument('-v', '--vault-file',
        help='New Relic vault file formatted as CSV [key_name,key_value]',
        type=argparse.FileType('r')
    )
    batch_local_parser.add_argument('-q', '--query-file',
        help='Local JSON input queries [{"name": "name", "query": "query"},...]',
        type=argparse.FileType('r'),
        required=True
    )
    batch_local_parser.add_argument('-a', '--account-file',
        help='Local CSV input accounts [master_name,account_name,query_key]',
        type=argparse.FileType('r'),
        required=True
    )
    batch_local_parser.add_argument('-o', '--output-folder',
        help='Local output folder name',
        required=True
    )
    batch_local_parser.add_argument('-f', '--output-format',
        help='output format',
        choices=['json', 'csv'],
        default='csv'
    )


def prepare_batch_google_parser(subparsers):
    batch_google_parser = subparsers.add_parser('batch-google')
    batch_google_parser.set_defaults(command='do_batch_google')
    batch_google_parser.add_argument('-v', '--vault-file',
        help='New Relic vault file formatted as CSV [key_name,key_value]',
        type=argparse.FileType('r')
    )
    batch_google_parser.add_argument('-q', '--query-file',
        help='Input queries list formatted as JSON [{name, query}, ...]',
        type=argparse.FileType('r'),
        required=True
    )
    batch_google_parser.add_argument('-a', '--account-file-id',
        help='Google Sheet input accounts [master_name,account_name,query_key]',
        required=True
    )
    batch_google_parser.add_argument('-o', '--output-folder-id',
        help='Google output folder id',
        required=True
    )
    batch_google_parser.add_argument('-s', '--secret-file',
        help='Google secret file location',
        type=argparse.FileType('r'),
        required=True
    )


def prepare_batch_insights_parser(subparsers):
    batch_insights_parser = subparsers.add_parser('batch-insights')
    batch_insights_parser.set_defaults(command='do_batch_insights')
    batch_insights_parser.add_argument('-v', '--vault-file',
        help='New Relic vault file formatted as CSV [key_name,key_value]',
        type=argparse.FileType('r')
    )
    batch_insights_parser.add_argument('-q', '--query-file',
        help='Input queries list formatted as JSON',
        type=argparse.FileType('r'),
        required=True
    )
    batch_insights_parser.add_argument('-a', '--account-file',
        help='Input accounts formatted as CSV [master_name,account_name,query_key]',
        type=argparse.FileType('r'),
        required=True
    )
    batch_insights_parser.add_argument('-i', '--insert-api-key',
        help='New Relic Insights insert API key',
        required=True
    )