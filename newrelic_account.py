#!env/bin/python
import json
import sys

from newrelic_rest_api import NewRelicRestAPI

class NewRelicAccount():
    "New Relic Account with a caching layer on top of the REST API"

    def __init__(self, rest_api_key='', return_type='list'):
        self.__rest_api = NewRelicRestAPI(rest_api_key)
        self.__cache = []
        self.__return_type = return_type

    def __get_cache(self, set_name):
        L = list(filter(lambda set: set['set_name'] == set_name, self.__cache))
        if len(L) == 1:
            return L[0]['data'], True
        else:
            return [], False

    def __return(self, result_set):
        if self.__return_type == 'dict':
            return [{'id':item['id'], 'data':item} for item in result_set]
        elif self.__return_type == 'list':
            return result_set
        else:
            return []

    def users(self, next_url=None):
        result, ok = self.__get_cache('users')
        if not ok:
            result, ok = self.__rest_api.get('users', next_url=next_url)
            if ok:
                self.__cache.append({
                    'set_name': 'users',
                    'data': result,
                })
        return self.__return(result), ok

    def labels(self, next_url=None):
        result, ok = self.__get_cache('labels')
        if not ok:
            result, ok = self.__rest_api.get('labels', next_url=next_url)
            if ok:
                self.__cache.append({
                    'set_name': 'labels',
                    'data': result,
                })
        return self.__return(result), ok

    def apm_applications(self, next_url=None):
        result, ok = self.__get_cache('applications')
        if not ok:
            result, ok = self.__rest_api.get('applications', next_url=next_url)
            if ok:
                self.__cache.append({
                    'set_name': 'applications',
                    'data': result,
                })
        return self.__return(result), ok

    def mobile_applications(self):
        result, ok = self.__get_cache('mobile_applications')
        if not ok:
            result, ok = self.__rest_api.get('mobile_applications')
            if ok:
                self.__cache.append({
                    'set_name': 'mobile_applications',
                    'data': result,
                })
        return self.__return(result), ok

    def browser_applications(self):
        result, ok = self.__get_cache('browser_applications')
        if not ok:
            result, ok = self.__rest_api.get('browser_applications')
            if ok:
                self.__cache.append({
                    'set_name': 'browser_applications',
                    'data': result,
                })
        return self.__return(result), ok

    def application_deployments(self, next_url=None):
        result, ok = self.__get_cache('application_deployments')
        if not ok:
            apm_applications, _ = self.apm_applications()
            for apm_application in apm_applications:
                entity_id = apm_application.get('id', 0)
                application_deployments, ok = self.__rest_api.get(
                    'application_deployments',
                    params={'entity_id': entity_id},
                    next_url=next_url
                )
                if ok:
                    result.append({
                        'id': entity_id,
                        'data': application_deployments
                    })
                else:
                    result = []
                    break
            if ok:
                self.__cache.append({
                    'set_name': 'application_deployments',
                    'data': result,
                })
        return self.__return(result), ok

    def alerts_policies(self, next_url=None):
        result, ok = self.__get_cache('alerts_policies')
        if not ok:
            result, ok = self.__rest_api.get('alerts_policies', next_url=next_url)
            if ok:
                self.__cache.append({
                    'set_name': 'alerts_policies',
                    'data': result,
                })
        return self.__return(result), ok

    def alerts_conditions(self, next_url=None):
        result, ok = self.__get_cache('alerts_conditions')
        if not ok:
            alerts_policies, _ = self.alerts_policies()
            for alerts_policy in alerts_policies:
                policy_id = alerts_policy.get('id', 0)
                alerts_conditions, ok = self.__rest_api.get(
                    'alerts_conditions',
                    params={'policy_id': policy_id},
                    next_url=next_url
                )
                if ok:
                    result.append({
                        'policy_id': policy_id,
                        'conditions': alerts_conditions
                    })
                else:
                    result = []
                    break
            if ok:
                self.__cache.append({
                    'set_name': 'alerts_conditions',
                    'data': result,
                })
        return self.__return(result), ok


def main():
    set_names = [
        'users', 'labels', 'apm_applications', 'application_deployments',
        'browser_applications', 'mobile_applications', 'alerts_policies', 'alerts_conditions'
    ]
    try:
        if len(sys.argv) == 1:
            raise Exception('usage: newrelic_account.py set_name')

        set_name = sys.argv[1]
        if not set_name in set_names:
            raise Exception('error: invalid set name')

        account = NewRelicAccount()
        method = getattr(account, set_name)
        result, ok = method()

        if not ok:
            raise Exception('error: could not fetch data')
        
        print(json.dumps(result, sort_keys=True, indent=4))

    except Exception as exception:
        print(exception.args[0])


if __name__ == "__main__":
    main()