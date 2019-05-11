import json
import os
import requests


def o(obj, attr):
    """ ensures Insights results are sound otherwise abort nicely """

    if not attr in obj:
        raise Exception(f'error: Cannot parse query {attr}')
    return obj[attr]


def get_results_names(contents):
    """ extracts results names from the contents attribute """

    names = []
    for content in contents:
        if 'alias' in content:
            names.append(content['alias'])
        else:
            function = content.get('function', '')
            attribute = content.get('attribute', '')
            name = function + '_' + attribute if attribute else function
            names.append(name)  
    return names


def get_facets_header(facet, header):

    data = {}
    if type(facet) is str:
        offset = 1
        data.update({header[0]: facet})
    elif type(facet) is list:
        offset = len(facet)
        data.update({k:v for k,v in list(zip(header, facet))})
    return data


def get_none(results, header, include={}):
    """ returns an empty event list """
    return []


def get_results(results, header, include={}, offset=0):

    row = {k:v for k,v in include.items()}
    for index,result in enumerate(results):
        row.update({header[offset+index]: list(result.values())[0]})
    return row


def get_facets(results, header, include={}):

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
            'beginTimeSeconds': result['beginTimeSeconds'],
            'endTimeSeconds': result['endTimeSeconds'],
            'inspectedCount': result['inspectedCount'],
            'timestamp': result['endTimeSeconds']
        })
        data.append(row)
    return data


def get_single(results, header, include={}):
    """ SELECT aggr1, aggr2, ... FROM ... """

    return [get_results(results, header, include=include)]


def get_events(results, header, include={}):
    """ SELECT attr1, attr2, ... FROM ... """
    
    data = []
    events = o(results[0], 'events')
    for event in events:
        row = {k:v for k,v in include.items()}
        row.update({k:v for k,v in event.items()})
        data.append(row)
    return data


def get_facets_timeseries(results, header, include={}):
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
                offset=len(row)
            )
        )
    return data


def get_calc_with_compare_facet(results, header, include={}):
    return []


def get_calc_with_compare_timeseries(results, header, include={}):
    return []
    
    
def get_calc_with_compare(results, header, include={}):
    return []


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
        """ send a request to the Insights Query API """

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

    def get_events(self, nrql, include={}):
        """ execute the nrql and convert to an events array """

        response = self.query(nrql)
        if not response:
            return []

        metadata = o(response, 'metadata')
        contents = metadata.get('contents', {})
        results = response.get(
            'results', response.get(
                'facets', response.get(
                    'timeSeries', []
                )
            )
        )

        # determine NRQL structure
        has_compare_with = 'compareWith' in metadata
        has_facet = 'facet' in metadata or 'facet' in contents
        has_timeseries = 'timeSeries' in metadata or 'timeSeries' in contents
        is_simple = not has_compare_with and not has_facet and not has_timeseries
        has_events = is_simple and len(contents) and 'order' in contents[0]
        has_single = is_simple and len(contents) and not 'order' in contents[0]

        # normalize the contents list
        if has_timeseries and (has_compare_with or has_facet):
            contents = o(o(contents, 'timeSeries'), 'contents')
        elif has_timeseries and not (has_compare_with or has_facet):
            contents = o(o(metadata, 'timeSeries'), 'contents')
        elif has_facet:
            contents = o(contents, 'contents')
        else:
            contents = o(metadata, 'contents')

        # build the header
        facet = None
        if has_facet:
            if has_compare_with:
                facet = contents['facet']
            else:
                facet = metadata['facet']

        header = []
        if type(facet) is list:
            header.extend(facet)
        elif type(facet) is str:
            header.append(facet)
        header.extend(get_results_names(contents))

        # select the proper parsing function
        fetch_data = get_none
        if has_single:
            fetch_data = get_single
        if has_events:
            fetch_data = get_events
        elif has_compare_with and has_facet:
            fetch_data = get_calc_with_compare_facet
        elif has_compare_with and has_timeseries:
            fetch_data = get_calc_with_compare_timeseries
        elif has_compare_with:
            fetch_data = get_calc_with_compare
        elif has_facet and has_timeseries:
            fetch_data = get_facets_timeseries
        elif has_facet:
            fetch_data = get_facets
        elif has_timeseries:
            fetch_data = get_timeseries

        return fetch_data(results, header, include)

if __name__ == "__main__":
    new_relic_account_id = 2328917
    new_relic_insights_query_api_key = ''
    api = NewRelicInsightsQueryAPI(new_relic_account_id, new_relic_insights_query_api_key)
    nrql = "select count(*), average(duration) as 'avg' from Transaction timeseries 5 hour since 1 months ago facet appName"
    #events = api.query(nrql)
    events = api.get_events(nrql, include={'eventType': 'Test'})
    print(json.dumps(events, sort_keys=True, indent=4))
    print('done')