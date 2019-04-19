import json
import os
import requests

MAX_RETRIES = 5 # max number of requests before giving up

class StorageNewRelicInsights():

    INSIGHTS_MAX_EVENTS = 1000

    def __init__(self, account_id, insert_api_key=''):
        if len(insert_api_key) != 32:
            insert_api_key = os.getenv('NEW_RELIC_INSIGHTS_INSERT_KEY', '')
        self.files = {}
        self.headers = {
            'Content-Type': 'application/json',
            'X-Insert-Key': insert_api_key
        }
        self.url = f'https://insights-collector.newrelic.com/v1/accounts/{account_id}/events'

    def get_accounts(self, name):
        return []
        
    def dump_metrics(self, name, data=[], metadata={}, max_retries=MAX_RETRIES):
        if type(data) == list:
            if len(metadata) > 0:
                for row in data:
                    row.update({'eventType': name})
                    row.update(metadata)
            for i in range(0, len(data), StorageNewRelicInsights.INSIGHTS_MAX_EVENTS):
                data_chunk = data[i:i+StorageNewRelicInsights.INSIGHTS_MAX_EVENTS]
                succeeded = False
                count_retries = 0
                while not succeeded and count_retries < max_retries:
                    try:
                        count_retries += 1
                        response = requests.post(
                            self.url,
                            data=json.dumps(data_chunk),
                            headers=self.headers
                        )
                        succeeded = (response.status_code == requests.codes.ok)
                    except:
                        succeeded = False
                

    def flush_metrics(self, location):
        pass
