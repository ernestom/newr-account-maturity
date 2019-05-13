import json
import os
import re
import requests

SP = '_'


def abort(message):
    """ abort the command """

    print(message)
    exit()


def to_datetime(timestamp):
    """ converts a timestamp to a Sheets / Excel datetime """

    # 1970-01-01 00:00:00 in Google Sheets / Microsoft Excel
    EPOCH_START = 25569
    SECONDS_IN_A_DAY = 86400
    return timestamp / SECONDS_IN_A_DAY / 1000 + EPOCH_START


def get_results_header(contents):
    """ extracts results names from the contents attribute """

    names = []
    for content in contents:
        alias = content.get('alias', '')
        if alias:
            content = content['contents']
        function = content['function']
        attribute = content.get('attribute', '')
        name = alias if alias else function + SP + attribute if attribute else function
        
        if function == 'funnel':
            for step in content['steps']:
                names.append(name + SP + str(step.replace(' ', SP)))

        elif function == 'percentile':
            for threshold in content['thresholds']:
                names.append(name + SP + str(threshold)) 

        elif function == 'rate':
            if not alias:
                of = content['of']
                name = name + SP + of['function'] + SP + of['attribute']
            names.append(name)

        elif function == 'histogram':
            start = content['start']
            size = content['bucketSize']
            for i in range(0, content['bucketCount']):
                end = start + size
                bucket = '%05.2f' % start + SP + '%05.2f' % end
                names.append(name + SP + bucket)
                start = end
        
        elif function == 'apdex':
            # this other matters as get_result_values use the same
            names.append(name + '_count')
            names.append(name + '_s')
            names.append(name + '_t')
            names.append(name + '_f')
            names.append(name + '_score')


        else:
            names.append(name)
    
    return names


def get_facets_values(facet, header):
    """ extracts facets values from the contents attribute """

    data = {}
    if type(facet) is str:
        data.update({header[0]: facet})
    elif type(facet) is list:
        data.update({k:v for k,v in list(zip(header, facet))})

    return data


def get_results_values(results, header, include={}, offset=0):
    """ extracts results values from the contents attribute """

    row = {k:v for k,v in include.items()}
    index = 0
    for result in results:
        if 'percentiles' in result: # percentiles
            for percentile in list(result['percentiles'].values()):
                row.update({header[offset+index]: percentile})
                index += 1

        elif 'histogram' in result: # histogram
            for histogram in result['histogram']:
                row.update({header[offset+index]: histogram})
                index += 1

        elif 'steps' in result: # funnel
            for step in result['steps']:
                row.update({header[offset+index]: step})
                index += 1

        elif 'score' in result: # apdex
                # this other matters as get_results_header use the same
                row.update({header[offset+index]: result['count']})
                index += 1
                row.update({header[offset+index]: result['s']})
                index += 1
                row.update({header[offset+index]: result['t']})
                index += 1
                row.update({header[offset+index]: result['f']})
                index += 1
                row.update({header[offset+index]: result['score']})
                index += 1

        else: # default aggregation
            row.update({header[offset+index]: list(result.values())[0]})
            index += 1

    return row


def get_single(results, header, include={}, offset=0):
    """ SELECT aggr1, aggr2, ... FROM ... """

    return [get_results_values(results, header, include, offset)]


def get_events(results, header, include={}, offset=0):
    """ SELECT attr1, attr2, ... FROM ... """
    
    data = []
    events = results[0]['events']
    for event in events:
        row = {k:v for k,v in include.items()}
        row.update({k:v for k,v in event.items()})
        if 'timestamp' in row:
            row.update({
                'datetime': to_datetime(row['timestamp'])
            })
        data.append(row)

    return data


def get_facets(results, header, include={}, offset=0):
    """ SELECT aggr1, aggr2, ... FROM ... FACET attr1, attr2, ... """

    data = []
    for result in results:
        row = get_facets_values(result['name'], header)
        row.update(get_results_values(result['results'], header, include, offset=len(row)))
        data.append(row)

    return data


def get_timeseries(results, header, include={}, offset=0, prefix=''):
    """ SELECT aggr1, aggr2, ... FROM ... TIMESERIES """

    data = []
    for result in results:
        row = get_results_values(result['results'], header, include, offset)
        row.update({
            'inspectedCount' + prefix: result['inspectedCount'],
            'timewindow' + prefix: result['endTimeSeconds'] - result['beginTimeSeconds'],
            'timestamp' + prefix: result['endTimeSeconds'],
            'datetime' + prefix: to_datetime(int(result['endTimeSeconds']) * 1000)
        })
        data.append(row)

    return data


def get_compare(results, header, include={}, offset=0):
    """ SELECT aggr1(), aggr2(), ... FROM ... COMPARE WITH ... """
    
    current = results['current']['results']
    previous = results['previous']['results']
    header_previous = [v if i < offset else v + '_compare' for i,v in enumerate(header)]
    row = get_results_values(current, header, include, offset)
    row.update(get_results_values(previous, header_previous, {}, offset))

    return [row]


def get_facets_timeseries(results, header, include={}, offset=0):
    """ SELECT aggr1(), aggr2(), ... FROM ... FACET attr1, attr2, ... TIMESERIES """
    
    data = []
    for result in results:
        row = get_facets_values(result['name'], header)
        _include = include
        _include.update(row)
        data.extend(get_timeseries(result['timeSeries'], header, _include, offset))

    return data


def get_compare_facets(results, header, include={}, offset=0):
    """ SELECT aggr1(), aggr2(), ... FROM ... COMPARE WITH ... FACET attr1, attr2, ... """

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


def get_compare_timeseries(results, header, include={}, offset=0):
    """ SELECT aggr1(), aggr2(), ... FROM ... COMPARE WITH ... TIMESERIES """

    data = []
    timeseries_current = results['current']['timeSeries']
    timeseries_previous = results['previous']['timeSeries']
    header_previous = [v if i < offset else v + '_compare' for i,v in enumerate(header)]
    current = get_timeseries(timeseries_current, header, include, offset)
    previous = get_timeseries(timeseries_previous, header_previous, {}, offset, '_compare')
    for curr,prev in list(zip(current, previous)):
        curr.update(prev)
        data.append(curr)

    return data
    

def parse_nrql(nrql, params):
    """ replace variables in nrql """

    # find all occurences of {string} and replace
    pattern = re.compile(r'\{[a-zA-Z][\w]*}')
    for var in pattern.findall(nrql):
        if not var[1:-1] in params:
            abort(f'error: cannot find {var} in parameters dictionary')
        nrql.replace(var, params[var[1:-1]])
        
    return nrql

class NewRelicQueryAPI():
    """ interface to New Relic Query API that always returns a list of events 
    
        standard aggregate functions fully supported:
            - min
            - max
            - sum
            - average
            - latest
            - stddev
            - percentage
            - filter
            - count
            - rate

        complex aggregate functions fully supported:
            - percentiles: denormalized, one percentile per attribute
            - histogram: denormalized, one bucket per attribute
            - apdex: all 5 attributes (s, f, t, count, score)
            - funnel: denormalized, one step per attribute

        other functions:
            - keyset: stores a list of keys in one single attribute
            - uniques: stores a list of uniques in one single attribute

        all NRQL syntax supported:
            - events lists
            - single values
            - comparisons
                - duplicates all metrics with prefix _compared
                - stores timestamp_compared attribute
            - facets
            - timeseries
            - combinations of all above

        other tidbits:
            - create attribute names from function and attribute metadata
                - or from alias (if present) and attribute
            - timestamp is overwritten with the 'UNTIL TO' one
            - timewindow stores how many seconds in the analysis
                - analysis range is [timestamp - timewindows : timestamp]
    """

    MAX_RETRIES = 5

    def __init__(self, account_id=0, query_api_key='', max_retries=MAX_RETRIES):
        """ init """
        
        if not account_id:
            account_id = os.getenv('NEW_RELIC_ACCOUNT_ID', '')
        if not account_id:
            abort('account id not provided and env NEW_RELIC_ACCOUNT_ID not set')

        if not query_api_key:
            query_api_key = os.getenv('NEW_RELIC_QUERY_API_KEY', '')
        if not query_api_key:
            abort('query api key not provided and env NEW_RELIC_QUERY_API_KEY not set')

        self.__headers = {
            'Accept': 'application/json',
            'X-Query-Key': query_api_key
        }
        self.__url = f'https://insights-api.newrelic.com/v1/accounts/{account_id}/query'
        self.__max_retries = max_retries

    def query(self, nrql, params={}):
        """ request a JSON result from the Insights Query API """

        parsed_nrql = parse_nrql(nrql, params)
        succeeded = False
        count_retries = 0
        while not succeeded and count_retries < self.__max_retries:
            try:
                count_retries += 1
                response = requests.get(
                    self.__url, headers=self.__headers, params={'nrql': parsed_nrql}
                )
                succeeded = (response.status_code == requests.codes.ok) 
            except:
                pass

        return response.json() if succeeded else []

    def events(self, nrql, include={}, params={}):
        """ execute the nrql and convert to an events list """
        
        # get the NRQL results
        response = self.query(nrql, params=params)
        if not response:
            return []

        metadata = response.get('metadata', {})
        contents = metadata.get('contents', {})

        # determine the NRQL structure
        has_compare = 'compareWith' in metadata
        has_facets = 'facet' in metadata or 'facet' in contents
        has_timeseries = 'timeSeries' in metadata or 'timeSeries' in contents
        is_simple = not has_compare and not has_facets and not has_timeseries
        has_events = is_simple and len(contents) and 'order' in contents[0]
        has_single = is_simple and len(contents) and not 'order' in contents[0]

        # normalize the contents list
        if has_timeseries and (has_compare or has_facets):
            contents = contents['timeSeries']['contents']
        elif has_timeseries and not (has_compare or has_facets):
            contents = metadata['timeSeries']['contents']
        elif has_compare and has_facets:
            contents = contents['contents']['contents']
        elif has_compare or has_facets:
            contents = contents['contents']
        else:
            contents = metadata['contents']

        # get facets attribute names
        if has_facets:
            if has_compare:
                facet = metadata['contents']['facet']
            else:
                facet = metadata['facet']
        else:
            facet = None

        # build the header and determine the offset
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

        # kick of the metadata to be included in results
        _include = {}
        _include.update(include)

        # select the proper parsing function and parameters
        if has_single:
            fetch_data = get_single
            results = response['results']
            _include.update({
                'timewindow': int(metadata['endTimeMillis']) - int(metadata['beginTimeMillis']),
                'timestamp': int(metadata['endTimeMillis']),
                'datetime': to_datetime(metadata['endTimeMillis'])
            })

        elif has_events:
            fetch_data = get_events
            results = response['results']

        elif has_compare:
            if has_facets:
                fetch_data = get_compare_facets

            elif has_timeseries:
                fetch_data = get_compare_timeseries

            else:
                fetch_data = get_compare

            results = {'current': response['current'], 'previous': response['previous']}

            if not has_timeseries:
                _include.update({
                    'timewindow': int(metadata['endTimeMillis']) - int(metadata['beginTimeMillis']),
                    'timestamp': int(metadata['endTimeMillis']),
                    'datetime': to_datetime(metadata['endTimeMillis']),
                    'timestamp_compare': int(metadata['beginTimeMillis']) - int(metadata['compareWith']),
                    'datetime_compare': to_datetime(int(metadata['beginTimeMillis']) - int(metadata['compareWith']))
                })

        elif has_facets:
            if has_timeseries:
                fetch_data = get_facets_timeseries

            else:
                fetch_data = get_facets

                _include.update({
                    'timewindow': int(metadata['endTimeMillis'] - metadata['beginTimeMillis']),
                    'timestamp': int(metadata['endTimeMillis']),
                    'datetime': to_datetime(metadata['endTimeMillis'])
                })

            results = response['facets']

        elif has_timeseries:
            fetch_data = get_timeseries
            results = response['timeSeries']

        else:
            results = []

        # parse the result JSON and return a list of event
        return fetch_data(results, header, _include, offset) if results else []

if __name__ == "__main__":
    nrqls = [
    # CASE 1 - event list
    """
    select
        appName, appId
    from
        Transaction
    """,

    # CASE 2 - aggregated values
    """
    select 
        percentage(count(*), where duration < 0.05),
        funnel(traceId, where duration < 2, where duration < 1),
        apdex(duration, 0.02),
        uniqueCount(appId),
        count(*),
        min(duration),
        max(duration),
        latest(duration),
        sum(duration),
        average(duration),
        stddev(duration),
        percentile(duration, 50, 75, 90),
        rate(uniqueCount(appId), 1 minute),
        percentage(count(*), where duration < 0.05) as 'MyPercentage',
        funnel(traceId, where duration < 2 as 'step1', where duration < 1 as 'step2'),
        apdex(duration, 0.02) as 'MyApdex',
        uniqueCount(appId) as 'MyUniqueCount',
        count(*) as 'MyCount',
        min(duration) as 'MyMin',
        max(duration) as 'MyMax',
        latest(duration) as 'MyLatest',
        sum(duration) as 'MySum',
        average(duration) as 'MyAverage',
        stddev(duration) as 'MyStdDev',
        percentile(duration, 50, 75, 90) as 'MyPercentile',
        rate(uniqueCount(appId), 1 hour) as 'MyRate'
    from
        Transaction
    """,

    # CASE 3 - aggregated values over time
    """
    select 
        percentage(count(*), where duration < 0.05),
        funnel(traceId, where duration < 2, where duration < 1),
        apdex(duration, 0.02),
        uniqueCount(appId),
        count(*),
        min(duration),
        max(duration),
        latest(duration),
        sum(duration),
        average(duration),
        stddev(duration),
        percentile(duration, 50, 75, 90),
        rate(uniqueCount(appId), 1 minute),
        percentage(count(*), where duration < 0.05) as 'MyPercentage',
        funnel(traceId, where duration < 2 as 'step1', where duration < 1 as 'step2'),
        apdex(duration, 0.02) as 'MyApdex',
        uniqueCount(appId) as 'MyUniqueCount',
        count(*) as 'MyCount',
        min(duration) as 'MyMin',
        max(duration) as 'MyMax',
        latest(duration) as 'MyLatest',
        sum(duration) as 'MySum',
        average(duration) as 'MyAverage',
        stddev(duration) as 'MyStdDev',
        percentile(duration, 50, 75, 90) as 'MyPercentile',
        rate(uniqueCount(appId), 1 hour) as 'MyRate'
    from
        Transaction
    timeseries
    """,

    # CASE 4 - segmented aggregated values
    """
    select
        percentage(count(*), where duration < 0.05),
        funnel(traceId, where duration < 2, where duration < 1),
        apdex(duration, 0.02),
        uniqueCount(appId),
        count(*),
        min(duration),
        max(duration),
        latest(duration),
        sum(duration),
        average(duration),
        stddev(duration),
        percentile(duration, 50, 75, 90),
        rate(uniqueCount(appId), 1 minute),
        percentage(count(*), where duration < 0.05) as 'MyPercentage',
        funnel(traceId, where duration < 2 as 'step1', where duration < 1 as 'step2'),
        apdex(duration, 0.02) as 'MyApdex',
        uniqueCount(appId) as 'MyUniqueCount',
        count(*) as 'MyCount',
        min(duration) as 'MyMin',
        max(duration) as 'MyMax',
        latest(duration) as 'MyLatest',
        sum(duration) as 'MySum',
        average(duration) as 'MyAverage',
        stddev(duration) as 'MyStdDev',
        percentile(duration, 50, 75, 90) as 'MyPercentile',
        rate(uniqueCount(appId), 1 hour) as 'MyRate'
    from
        Transaction
    facet
        appName, appId
    """,

    # CASE 5 - segmented aggregated values over time
    """
    select
        percentage(count(*), where duration < 0.05),
        apdex(duration, 0.02),
        uniqueCount(appId),
        count(*),
        min(duration),
        max(duration),
        latest(duration),
        sum(duration),
        average(duration),
        stddev(duration),
        percentile(duration, 50, 75, 90),
        rate(uniqueCount(appId), 1 minute),
        percentage(count(*), where duration < 0.05) as 'MyPercentage',
        apdex(duration, 0.02) as 'MyApdex',
        uniqueCount(appId) as 'MyUniqueCount',
        count(*) as 'MyCount',
        min(duration) as 'MyMin',
        max(duration) as 'MyMax',
        latest(duration) as 'MyLatest',
        sum(duration) as 'MySum',
        average(duration) as 'MyAverage',
        stddev(duration) as 'MyStdDev',
        percentile(duration, 50, 75, 90) as 'MyPercentile',
        rate(uniqueCount(appId), 1 hour) as 'MyRate'
    from
        Transaction
    facet
        appName, appId
    timeseries
    """,

    # CASE 6 - compared with aggregated values
    """
    select 
        percentage(count(*), where duration < 0.05),
        funnel(traceId, where duration < 2, where duration < 1),
        apdex(duration, 0.02),
        uniqueCount(appId),
        count(*),
        min(duration),
        max(duration),
        latest(duration),
        sum(duration),
        average(duration),
        stddev(duration),
        percentile(duration, 50, 75, 90),
        rate(uniqueCount(appId), 1 minute),
        percentage(count(*), where duration < 0.05) as 'MyPercentage',
        funnel(traceId, where duration < 2 as 'step1', where duration < 1 as 'step2'),
        apdex(duration, 0.02) as 'MyApdex',
        uniqueCount(appId) as 'MyUniqueCount',
        count(*) as 'MyCount',
        min(duration) as 'MyMin',
        max(duration) as 'MyMax',
        latest(duration) as 'MyLatest',
        sum(duration) as 'MySum',
        average(duration) as 'MyAverage',
        stddev(duration) as 'MyStdDev',
        percentile(duration, 50, 75, 90) as 'MyPercentile',
        rate(uniqueCount(appId), 1 hour) as 'MyRate'
    from
        Transaction
    compare with
        1 week ago
    """,

    # CASE 7 - compared with segmented aggregated values
    """
    select
        percentage(count(*), where duration < 0.05),
        apdex(duration, 0.02),
        uniqueCount(appId),
        count(*),
        min(duration),
        max(duration),
        latest(duration),
        sum(duration),
        average(duration),
        stddev(duration),
        percentile(duration, 50, 75, 90),
        rate(uniqueCount(appId), 1 minute),
        percentage(count(*), where duration < 0.05) as 'MyPercentage',
        apdex(duration, 0.02) as 'MyApdex',
        uniqueCount(appId) as 'MyUniqueCount',
        count(*) as 'MyCount',
        min(duration) as 'MyMin',
        max(duration) as 'MyMax',
        latest(duration) as 'MyLatest',
        sum(duration) as 'MySum',
        average(duration) as 'MyAverage',
        stddev(duration) as 'MyStdDev',
        percentile(duration, 50, 75, 90) as 'MyPercentile',
        rate(uniqueCount(appId), 1 hour) as 'MyRate'
    from
        Transaction
    compare with
        1 week ago
    facet
        appName, appId
    """,

    # CASE 8 - compare with values over time
    """
    select
        percentage(count(*), where duration < 0.05),
        funnel(traceId, where duration < 2, where duration < 1),
        apdex(duration, 0.02),
        uniqueCount(appId),
        count(*),
        min(duration),
        max(duration),
        latest(duration),
        sum(duration),
        average(duration),
        stddev(duration),
        percentile(duration, 50, 75, 90),
        rate(uniqueCount(appId), 1 minute),
        percentage(count(*), where duration < 0.05) as 'MyPercentage',
        funnel(traceId, where duration < 2 as 'step1', where duration < 1 as 'step2'),
        apdex(duration, 0.02) as 'MyApdex',
        uniqueCount(appId) as 'MyUniqueCount',
        count(*) as 'MyCount',
        min(duration) as 'MyMin',
        max(duration) as 'MyMax',
        latest(duration) as 'MyLatest',
        sum(duration) as 'MySum',
        average(duration) as 'MyAverage',
        stddev(duration) as 'MyStdDev',
        percentile(duration, 50, 75, 90) as 'MyPercentile',
        rate(uniqueCount(appId), 1 hour) as 'MyRate'
    from
        Transaction
    compare with
        1 week ago
    timeseries
    """
    ]

    # simple test case
    api = NewRelicQueryAPI()
    for nrql in nrqls:
        #events = api.query(nrql)
        events = api.events(nrql, include={'eventType': 'MyCustomEvent'})
        print(json.dumps(events, sort_keys=True, indent=4))