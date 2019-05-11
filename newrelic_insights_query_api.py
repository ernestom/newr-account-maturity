import json
import os
import requests


def get_empty(results, header, include={}):
    """ returns an empty event list """
    return []


def get_results_header(contents):
    """ extracts results names from the contents attribute """

    names = []
    for content in contents:
        alias = content.get('alias', '')
        if alias:
            content = content['contents']
        function = content['function']
        attribute = content.get('attribute', '')
        name = alias if alias else function + '_' + attribute if attribute else function
        if function == 'percentile':
            for threshold in content['thresholds']:
                names.append(name + '_' + str(threshold)) 
        elif function == 'histogram':
            start = content['start']
            size = content['bucketSize']
            for i in range(0, content['bucketCount']):
                end = start + size
                bucket = '%05.2f' % start + '_' + '%05.2f' % end
                names.append(name + '_' + bucket)
                start = end
        else:
            names.append(name)
    
    return names


def get_facets_header(facet, header):

    data = {}
    if type(facet) is str:
        data.update({header[0]: facet})
    elif type(facet) is list:
        data.update({k:v for k,v in list(zip(header, facet))})

    return data



def get_results(results, header, include={}, offset=0):

    row = {k:v for k,v in include.items()}
    index = 0
    for result in results:
        if 'percentiles' in result:
            for percentile in list(result['percentiles'].values()):
                row.update({header[offset+index]: percentile})
                index += 1
        elif 'histogram' in result:
            for histogram in result['histogram']:
                row.update({header[offset+index]: histogram})
                index += 1
        else:          
            row.update({header[offset+index]: list(result.values())[0]})
            index += 1

    return row


def get_facets(results, header, include={}, offset=0):

    data = []
    for result in results:
        # row = {k:v for k,v in include.items()}
        row = get_facets_header(result['name'], header)
        row.update(get_results(result['results'], header, include, offset=len(row)))
        data.append(row)

    return data


def get_timeseries(results, header, include={}, offset=0):

    data = []
    for result in results:
        # row = {k:v for k,v in include.items()}
        row = get_results(result['results'], header, include, offset)
        row.update({
            'inspectedCount': result['inspectedCount'],
            'timewindow': result['endTimeSeconds'] - result['beginTimeSeconds'],
            'timestamp': result['endTimeSeconds']
        })
        data.append(row)

    return data


def get_single(results, header, include={}, offset=0):
    """ SELECT aggr1, aggr2, ... FROM ... """

    return [get_results(results, header, include=include)]


def get_events(results, header, include={}, offset=0):
    """ SELECT attr1, attr2, ... FROM ... """
    
    data = []
    events = results[0]['events']
    for event in events:
        row = {k:v for k,v in include.items()}
        row.update({k:v for k,v in event.items()})
        data.append(row)

    return data


def get_facets_timeseries(results, header, include={}, offset=0):
    """ SELECT aggr1(), aggr2(), ... FROM ... FACET attr1, attr2, ... TIMESERIES """
    
    data = []
    for result in results:
        # row = {k:v for k,v in include.items()}
        row = get_facets_header(result['name'], header)
        _include = include
        _include.update(row)
        data.extend(
            get_timeseries(
                result['timeSeries'], 
                header, 
                _include,
                offset=offset
            )
        )

    return data


def get_calc_with_compare_facet(results, header, include={}, offset=0):

    data = []
    facets_current = results['current']['facets']
    facets_previous = results['previous']['facets']
    header_previous = [v if i < offset else v + '_compare' for i,v in enumerate(header)]
    current = get_facets(facets_current, header, include, offset)
    previous = get_facets(facets_previous, header_previous, {}, offset)
    for curr,prev in list(zip(current, previous)):
        curr.update(prev)
        data.append(curr)

    return data


def get_calc_with_compare_timeseries(results, header, include={}, offset=0):
    return []
    
    
def get_compare_with(results, header, include={}, offset=0):
    """ SELECT aggr1(), aggr2(), ... FROM ... COMPARE WITH ... """
    
    current = results['current']['results']
    previous = results['previous']['results']
    header_previous = [v if i < offset else v + '_compare' for i,v in enumerate(header)]
    row = get_results(current, header, include, offset)
    row.update(get_results(previous, header_previous, {}, offset))

    return [row]

class NewRelicInsightsQueryAPI():

    MAX_RETRIES = 5

    def __init__(self, new_relic_account_id, query_api_key, max_retries=MAX_RETRIES):
        """ init """
        
        if not new_relic_account_id:
            new_relic_account_id = os.getenv('NEW_RELIC_ACCOUNT_ID', '')

        if not new_relic_account_id:
            raise Exception('error: missing New Relic account id')

        if not query_api_key:
            query_api_key = os.getenv('NEW_RELIC_INSIGHTS_QUERY_API_KEY', '')

        if not query_api_key:
            raise Exception('error: missing New Relic Insights query API key')

        self.__headers = {
            'Accept': 'application/json',
            'X-Query-Key': query_api_key
        }
        self.__url = f'https://insights-api.newrelic.com/v1/accounts/{new_relic_account_id}/query'
        self.__max_retries = max_retries

    def query(self, nrql):
        """ request a JSON result from the Insights Query API """

        succeeded = False
        count_retries = 0
        while not succeeded and count_retries < self.__max_retries:
            try:
                count_retries += 1
                response = requests.get(
                    self.__url, headers=self.__headers, params={'nrql': nrql}
                )
                succeeded = (response.status_code == requests.codes.ok)
            except:
                pass
        return response.json() if succeeded else []

    def events(self, nrql, include={}):
        """ execute the nrql and convert to an events list """

        _include = {}
        _include.update(include)

        response = self.query(nrql)
        if not response:
            return []

        del response['performanceStats']
        print(json.dumps(response, sort_keys=True, indent=4))

        metadata = response.get('metadata', {})
        contents = metadata.get('contents', {})

        # determine the NRQL structure
        has_compare_with = 'compareWith' in metadata
        has_facet = 'facet' in metadata or 'facet' in contents
        has_timeseries = 'timeSeries' in metadata or 'timeSeries' in contents
        is_simple = not has_compare_with and not has_facet and not has_timeseries
        has_events = is_simple and len(contents) and 'order' in contents[0]
        has_single = is_simple and len(contents) and not 'order' in contents[0]

        # normalize the contents list
        if has_timeseries and (has_compare_with or has_facet):
            contents = contents['timeSeries']['contents']
        elif has_timeseries and not (has_compare_with or has_facet):
            contents = metadata['timeSeries']['contents']
        elif has_compare_with and has_facet:
            contents = contents['contents']['contents']
        elif has_compare_with or has_facet:
            contents = contents['contents']
        else:
            contents = metadata['contents']

        # get facets attribute names
        if has_facet:
            if has_compare_with:
                facet = metadata['contents']['facet']
            else:
                facet = metadata['facet']
        else:
            facet = None

        # build the header and get the offset
        header = []
        if type(facet) is list:
            header.extend(facet)
            offset = len(header)
        elif type(facet) is str:
            header.append(facet)
            offset = 1
        else:
            offset = 0
        header.extend(get_results_header(contents))

        # select the proper parsing function and parameters
        if has_single:
            fetch_data = get_single
            results = response['results']
            _include.update({
                'timewindow': int((metadata['endTimeMillis'] - metadata['beginTimeMillis']) / 1000),
                'timestamp': int(metadata['endTimeMillis'] / 1000)
            })

        elif has_events:
            fetch_data = get_events
            results = response['results']

        elif has_compare_with:
            if has_facet:
                fetch_data = get_calc_with_compare_facet
                _include.update({
                    'timewindow': int((metadata['endTimeMillis'] - metadata['beginTimeMillis']) / 1000),
                    'timestamp': int(metadata['endTimeMillis'] / 1000),
                    'timestamp_compare': int((metadata['beginTimeMillis'] - metadata['compareWith']) / 1000)
                })
            elif has_timeseries:
                fetch_data = get_calc_with_compare_timeseries
            else:
                fetch_data = get_compare_with
                _include.update({
                    'timewindow': int((metadata['endTimeMillis'] - metadata['beginTimeMillis']) / 1000),
                    'timestamp': int(metadata['endTimeMillis'] / 1000),
                    'timestamp_compare': int((metadata['beginTimeMillis'] - metadata['compareWith']) / 1000)
                })
            results = {'current': response['current'], 'previous': response['previous']}

        elif has_facet:
            if has_timeseries:
                fetch_data = get_facets_timeseries
            else:
                fetch_data = get_facets
                _include.update({
                    'timewindow': int((metadata['endTimeMillis'] - metadata['beginTimeMillis']) / 1000),
                    'timestamp': int(metadata['endTimeMillis'] / 1000)
                })
            results = response['facets']

        elif has_timeseries:
            fetch_data = get_timeseries
            results = response['timeSeries']

        else:
            fetch_data = get_empty
            results = []

        # parse the result JSON an return a list of event
        return fetch_data(results, header, _include, offset)

if __name__ == "__main__":
    new_relic_account_id = 2328917
    new_relic_insights_query_api_key = ''
    api = NewRelicInsightsQueryAPI(new_relic_account_id, new_relic_insights_query_api_key)
    nrql = "select count(*), average(duration) from Transaction compare with 1 day ago facet appName"
    #events = api.query(nrql)
    events = api.events(nrql, include={'eventType': 'Test'})
    print(json.dumps(events, sort_keys=True, indent=4))