import csv
import json
import requests


class StorageNewRelicInsights():

    INSIGHTS_MAX_EVENTS = 1000
    MAX_RETRIES = 5 # max number of requests before giving up

    def __init__(self, account_file, account_id, insert_api_key, timestamp=None):
        """ init """

        self.__account_file = account_file
        self.__headers = {
            'Content-Type': 'application/json',
            'X-Insert-Key': insert_api_key
        }
        self.__url = f'https://insights-collector.newrelic.com/v1/accounts/{account_id}/events'
        self.__timestamp = timestamp

    def __get_events(self, event_type, data=[]):
        """ inject the metadata at the beginning of the dictionary """

        events = []
        for row in data:
            _row = {'eventType': event_type}
            if self.__timestamp:
                _row['timestamp'] = self.__timestamp
            _row.update(row)
            events.append(_row)
        return events

    def get_accounts(self):
        """ returns a list of accounts dictionaries """

        with open(self.__account_file) as f:
            csv_reader = csv.DictReader(f, delimiter=',')
            return list(dict(row) for row in csv_reader) 
        
    def dump_data(self, master, event_type, data=[], max_retries=MAX_RETRIES):
        """ appends the data to the event """
        
        if type(data) == list and data:
            events = self.__get_events(event_type, data)

            for i in range(0, len(events), StorageNewRelicInsights.INSIGHTS_MAX_EVENTS):
                data_chunk = events[i:i+StorageNewRelicInsights.INSIGHTS_MAX_EVENTS]
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