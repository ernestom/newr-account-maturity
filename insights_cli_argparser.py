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
        help='Local CSV vault file [secret,account_id,query_api_key]',
        # type=argparse.FileType('r')
    )
    batch_local_parser.add_argument('-q', '--query-file',
        help='Local YAML input queries [{"name": "name", "nrql": "query"},...]',
        # type=argparse.FileType('r'),
        required=True
    )
    batch_local_parser.add_argument('-a', '--account-file',
        help='Local CSV input accounts [master_name,account_id,account_name,query_api_key]',
        # type=argparse.FileType('r'),
        required=True
    )
    batch_local_parser.add_argument('-o', '--output-folder',
        help='Local output folder name',
        required=True
    )


def prepare_batch_google_parser(subparsers):
    batch_google_parser = subparsers.add_parser('batch-google')
    batch_google_parser.set_defaults(command='do_batch_google')
    batch_google_parser.add_argument('-v', '--vault-file',
        help='Local CSV vault file [secret,account_id,query_api_key]',
        # type=argparse.FileType('r')
    )
    batch_google_parser.add_argument('-q', '--query-file',
        help='Local YAML input queries [{"name": "name", "nrql": "query"},...]',
        # type=argparse.FileType('r'),
        required=True
    )
    batch_google_parser.add_argument('-a', '--account-file-id',
        help='Google Sheets input accounts [master_name,account_id,account_name,query_api_key]',
        required=True
    )
    batch_google_parser.add_argument('-o', '--output-folder-id',
        help='Google Drive output folder id',
        required=True
    )
    batch_google_parser.add_argument('-s', '--secret-file',
        help='Google secret file location',
        # type=argparse.FileType('r'),
        required=True
    )


def prepare_batch_insights_parser(subparsers):
    batch_insights_parser = subparsers.add_parser('batch-insights')
    batch_insights_parser.set_defaults(command='do_batch_insights')
    batch_insights_parser.add_argument('-v', '--vault-file',
        help='Local CSV vault file [secret,account_id,query_api_key]',
        # type=argparse.FileType('r')
    )
    batch_insights_parser.add_argument('-q', '--query-file',
        help='Local YAML input queries [{"name": "name", "nrql": "query"},...]',
        # type=argparse.FileType('r'),
        required=True
    )
    batch_insights_parser.add_argument('-a', '--account-file',
        help='Local CSV input accounts [master_name,account_id,account_name,query_api_key]',
        # type=argparse.FileType('r'),
        required=True
    )
    batch_insights_parser.add_argument('-i', '--account-id',
        help='New Relic Insights insert API key',
        required=True
    )
    batch_insights_parser.add_argument('-k', '--insert-api-key',
        help='New Relic Insights insert API key',
        required=True
    )