import json
import os
import requests

MAX_RETRIES = 5 # max number of requests before giving up

class StorageNewRelicInsights():

    INSIGHTS_MAX_EVENTS = 1000

    def __init__(self, new_relic_account_id, insert_api_key):
        self.__headers = {
            'Content-Type': 'application/json',
            'X-Insert-Key': insert_api_key
        }
        self.__url = f'https://insights-collector.newrelic.com/v1/accounts/{new_relic_account_id}/events'

    def get_accounts(self, name):
        raise Exception('error: get_accounts not implemented on StorageNewRelicInsights class.')
        
    def dump_metrics(self, event_type, data=[], metadata={}, max_retries=MAX_RETRIES):
        if type(data) == list and len(data) > 0:
            for row in data:
                row.update({'eventType': event_type})
                row.update(metadata)
            for i in range(0, len(data), StorageNewRelicInsights.INSIGHTS_MAX_EVENTS):
                data_chunk = data[i:i+StorageNewRelicInsights.INSIGHTS_MAX_EVENTS]
                succeeded = False
                count_retries = 0
                while not succeeded and count_retries < max_retries:
                    try:
                        count_retries += 1
                        response = requests.post(
                            self.__url,
                            data=json.dumps(data_chunk),
                            headers=self.__headers
                        )
                        succeeded = (response.status_code == requests.codes.ok)
                    except:
                        succeeded = False