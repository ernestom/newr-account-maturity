import json
import os
import requests
import sys
import urllib.parse as urlparse
from datetime import datetime, date, timedelta

MAX_PAGES = 200 # max number of pages to fetch on a paginating endpoint
MAX_RETRIES = 5 # max number of requests before giving up


def empty_next_url(response):
    """ always return None to force no pagination """
    return None


def paginating_next_url(response):
    """ looks for a next link in the response to get next url """

    url = response.links.get('next', {}).get('url', None)
    if url:
        parsed = urlparse.urlparse(url)
        page = int(urlparse.parse_qs(parsed.query)['page'][0])
        if page > MAX_PAGES:
            url = None
    return url


def deployments_next_url(response):
    """ looks for a next link in the response to get next url """

    url = response.links.get('next', {}).get('url', None)
    deployments = response.json().get('deployments', [])
    if not deployments:
        url = None
    
    if url:
        a_month_ago = (datetime.today() - timedelta(days=30)).timestamp()
        timestamp = deployments[-1].get('timestamp', None)
        if not timestamp:
            url = None

    if url:
        timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z').timestamp()
        if timestamp < a_month_ago:
            url = None

    if url:
        parsed = urlparse.urlparse(url)
        page = int(urlparse.parse_qs(parsed.query)['page'][0])
        if page > MAX_PAGES:
            url = None
    return url


class NewRelicRestAPI():
    """ Facade to New Relic REST API LIST endpoints """

    """ New Relic REST API LIST endpoints """
    ENDPOINTS = {
    'applications': {
        'url': 'https://api.newrelic.com/v2/applications.json',
        'next_url': paginating_next_url,
        'result_set_name': 'applications'
    },
    'application_hosts': {
        'url': 'https://api.newrelic.com/v2/applications/{}/hosts.json',
        'next_url': paginating_next_url,
        'result_set_name': 'application_hosts'
    },
    'application_instances': {
        'url': 'https://api.newrelic.com/v2/applications/{}/instances.json',
        'next_url': paginating_next_url,
        'result_set_name': 'application_instances'
    },
    'application_deployments': {
        'url': 'https://api.newrelic.com/v2/applications/{}/deployments.json',
        # 'next_url': deployments_next_url,
        'result_set_name': 'deployments'
    },
    'mobile_applications': {
        'url': 'https://api.newrelic.com/v2/mobile_applications.json',
        'result_set_name': 'applications'
    },
    'browser_applications': {
        'url': 'https://api.newrelic.com/v2/browser_applications.json',
        'result_set_name': 'browser_applications'
    },
    'key_transactions': {
        'url': 'https://api.newrelic.com/v2/key_transactions.json',
        'result_set_name': 'key_transactions'
    },
    'users': {
        'url': 'https://api.newrelic.com/v2/users.json',
        'next_url': paginating_next_url,
        'result_set_name': 'users'
    },
    'plugins': {
        'url': 'https://api.newrelic.com/v2/plugins.json',
        'next_url': paginating_next_url,
        'result_set_name': 'plugins'
    },
    'labels': {
        'url': 'https://api.newrelic.com/v2/labels.json',
        'next_url': paginating_next_url,
        'result_set_name': 'labels'
    },
    'alerts_events': {
        'url': 'https://api.newrelic.com/v2/alerts_events.json',
        'next_url': paginating_next_url,
        'result_set_name': 'recent_events'
    },
    'alerts_policies': {
        'url': 'https://api.newrelic.com/v2/alerts_policies.json',
        'next_url': paginating_next_url,
        'result_set_name': 'policies'
    },
    'alerts_channels': {
        'url': 'https://api.newrelic.com/v2/alerts_channels.json',
        'next_url': paginating_next_url,
        'result_set_name': 'channels'
    },
    'alerts_conditions': {
        'url': 'https://api.newrelic.com/v2/alerts_conditions.json',
        'next_url': paginating_next_url,
        'result_set_name': 'conditions'
    },
    'alerts_plugins_conditions': {
        'url': 'https://api.newrelic.com/v2/alerts_plugins_conditions.json',
        'next_url': paginating_next_url,
        'result_set_name': 'conditions'
    },
    'external_service_conditions': {
        'url': 'https://api.newrelic.com/v2/alerts_external_service_conditions.json',
        'next_url': paginating_next_url,
        'result_set_name': 'conditions'
    },
    'alerts_synthetics_conditions': {
        'url': 'https://api.newrelic.com/v2/alerts_synthetics_conditions.json',
        'next_url': paginating_next_url,
        'result_set_name': 'conditions'
    },
    'alerts_nrql_conditions': {
        'url': 'https://api.newrelic.com/v2/alerts_nrql_conditions.json',
        'next_url': paginating_next_url,
        'result_set_name': 'conditions'
    },
    'alerts_entity_conditions': {
        'url': 'https://api.newrelic.com/v2/alerts_entity_conditions/{}.json',
        'result_set_name': 'conditions'
    }
    }

    def __init__(self, rest_api_key=''):
        if not rest_api_key:
            rest_api_key = os.getenv('NEW_RELIC_REST_API_KEY', '')
        if not rest_api_key:
            raise Exception('error: missing New Relic REST API KEY')
        self.__headers = {'X-API-Key': rest_api_key}

    def get(self, endpoint, params={}, next_url=None, max_retries=MAX_RETRIES):
        """ returns a list from the provided endpoint """

        ENDPOINT = NewRelicRestAPI.ENDPOINTS.get(endpoint, None)
        if ENDPOINT == None:
            return [], False

        url = ENDPOINT['url']
        if '{}' in url:
            entity_id = params.get('entity_id', None)
            if entity_id is None:
                ok = False
            else:
                url = url.format(entity_id)

        if next_url == None:
            next_url = ENDPOINT.get('next_url', empty_next_url)

        result_set_name = ENDPOINT['result_set_name']

        result, ok = [], True
        while ok and url:
            succeeded = False
            count_retries = 0
            while not succeeded and count_retries < max_retries:
                try:
                    count_retries += 1
                    response = requests.get(
                        url,
                        headers=self.__headers,
                        params=params
                    )
                    succeeded = (response.status_code == requests.codes.ok)
                except:
                    succeeded = False
            if succeeded:
                response_json = response.json()[result_set_name]
                result += response_json
                url = next_url(response)
            else:
                result, ok = [], False

        return result, ok


def main():
    try:
        if len(sys.argv) == 1:
            raise Exception('usage: newrelic_rest_api.py endpoint_name [id]')

        endpoint = sys.argv[1]
        if endpoint in ['application_hosts', 'application_instances', 'application_deployments']:
            if len(sys.argv) != 3:
                raise Exception('error: this endpoint requires an entity id')
            else:
                params = {'entity_id': sys.argv[2]}
        elif endpoint == 'alerts_entity_conditions':
            if len(sys.argv) != 3:
                raise Exception('error: this endpoint requires a policy id')
            else:
                params = {'policy_id': sys.argv[2]}
        else:
            if len(sys.argv) != 2:
                raise Exception('error: this endpoint does not take any id')
            else:
                params = {}
   
        api = NewRelicRestAPI()
        result, ok = api.get(endpoint, params=params)

        if not ok:
            raise Exception('error: could not fetch data')
        
        print(json.dumps(result, sort_keys=True, indent=4))

    except Exception as exception:
        print(exception.args[0])


if __name__ == "__main__":
    main()