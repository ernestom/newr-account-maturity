import json
import time

from newrelic_account import NewRelicAccount

DEFAULT_APDEX = 0.5

class NewRelicAccountMetrics():
    "calculates maturity metrics using the New Relic REST API"

    __WEEK_TIME = 60*60*24*7
    __MONTH_TIME = __WEEK_TIME * 4.5
    __METRIC_NAMES = [
        'users_total',
        'apm_total',
        'browser_total',
        'mobile_total',
        'alerts_policies_total',

        'apm_default_apdex',
        'apm_dotnet', #10
        'apm_go',
        'apm_java',
        'apm_nodejs',
        'apm_php',
        'apm_sdk',
        'apm_python',
        'apm_ruby',
        'apm_reporting',
        'apm_non_reporting',
        'apm_with_conditions', #20
        'apm_without_conditions',
        'apm_with_deployments',
        'apm_without_deployments',
        'apm_with_labels',
        'apm_without_labels',

        'browser_with_conditions',
        'browser_without_conditions',

        'mobile_reporting',
        'mobile_non_reporting',
        'mobile_with_conditions', #30
        'mobile_without_conditions',

        'alerts_policies_a_month_old',
        'alerts_policies_a_week_old',
        'alerts_policies_per_condition',
        'alerts_policies_per_policy',
        'alerts_policies_per_target',

        'apm_conditions',
        'apm_deployments',
        'apm_labels',
        'apm_hosts', #40
        'apm_instances',
        'apm_concurrent_instances',

        'get_metadata_duration'
    ]

    def __init__(self, rest_api_key=''):
        self.__account = NewRelicAccount(rest_api_key)
        self.reset_metrics()

    def reset_metrics(self):
        self.__metrics = {}
        for metric_name in NewRelicAccountMaturity.__METRIC_NAMES:
            self.__metrics[metric_name] = 0

    def cache_apps_with_deployments(self):
        self.apps_with_deployments = {}
        app_deployments, ok = self.__account.application_deployments()
        if ok:
            for deployment in app_deployments:
                entity = deployment['id']
                total_deployments = len(deployment['deployments'])
                if total_deployments:
                    self.apps_with_deployments[entity] = total_deployments

    def cache_apps_with_labels(self):
        self.apps_with_labels = {}
        labels, ok = self.__account.labels()
        if ok:
            for label in labels:
                entities = label['links']['applications']
                if entities:
                    for entity in entities:
                        self.apps_with_labels[entity] = \
                            self.apps_with_labels.get(entity, 0) + 1

    def cache_entities_with_conditions(self):
        self.entities_with_conditions = {}
        alerts_conditions, ok = self.__account.alerts_conditions()
        if ok:
            for policy in alerts_conditions:
                conditions = policy['conditions']
                if conditions:
                    for condition in conditions:
                        # apm_app_metric, apm_kt_metric, browser_metric, mobile_metric
                        condition_type = condition['type']
                        for entity in condition['entities']:
                            entity = int(entity)
                            self.entities_with_conditions[(condition_type,entity)] = \
                                self.entities_with_conditions.get((condition_type,entity), 0) + 1

    def get_users_metrics(self):
        users, _ = self.__account.users()
        self.__metrics['users_total'] = len(users)

    def get_apm_metrics(self):
        result_apps = []
        apm_apps, _ = self.__account.apm_applications()
        for apm_app in apm_apps:
            id = apm_app['id']

            result_apps.append({
                'app_id': id,
                'app_name': apm_app['name'],
                'language': apm_app['language'],
                'conditions': self.entities_with_conditions.get(('apm_app_metric',id), 0),
                'deployments': self.apps_with_deployments.get(id, 0),
                'labels': self.apps_with_labels.get(id, 0),
                'default_apdex': int(apm_app['settings']['app_apdex_threshold'] == DEFAULT_APDEX),
                'reporting': int(apm_app['reporting'])
            })

            self.__metrics['apm_total'] += 1

            if not id in self.apps_with_deployments:
                self.__metrics['apm_without_deployments'] += 1
            else:
                self.__metrics['apm_with_deployments'] += 1
                self.__metrics['apm_deployments'] += self.apps_with_deployments[id]

            if not id in self.apps_with_labels:
                self.__metrics['apm_without_labels'] += 1
            else:
                self.__metrics['apm_with_labels'] += 1
                self.__metrics['apm_labels'] += self.apps_with_labels[id]

            if not ('apm_app_metric',id) in self.entities_with_conditions:
                self.__metrics['apm_without_conditions'] += 1
            else:
                self.__metrics['apm_with_conditions'] += 1
                self.__metrics['apm_conditions'] += self.entities_with_conditions[('apm_app_metric',id)]

            metric_name = 'apm_' + apm_app['language']
            self.__metrics[metric_name] += 1

            if apm_app['reporting']:
                self.__metrics['apm_reporting'] += 1

                summary = apm_app['application_summary']
                self.__metrics['apm_hosts'] += summary['host_count']
                self.__metrics['apm_instances'] += summary.get('instance_count', 0)
                self.__metrics['apm_concurrent_instances'] += summary.get('concurrent_instance_count', 0)
                if summary['apdex_target'] == DEFAULT_APDEX:
                    self.__metrics['apm_default_apdex'] += 1
            else:
                self.__metrics['apm_non_reporting'] += 1

        return result_apps

    def get_mobile_metrics(self):
        result_apps = []
        mobile_apps, _ = self.__account.mobile_applications()
        self.__metrics['mobile_total'] = len(mobile_apps)
        for mobile_app in mobile_apps:
            id = mobile_app['id']
            # summary = mobile_app.get('summary',{})
            crash_summary = mobile_app.get('crash_summary',{})

            result_apps.append({
                'app_id': id,
                'app_name': mobile_app['name'],
                'conditions': self.entities_with_conditions.get(('mobile_metric',id), 0),
                'supports_crash_data': int(crash_summary.get('supports_crash_data', False))
            })

            if mobile_app['reporting']:
                self.__metrics['mobile_reporting'] += 1
            else:
                self.__metrics['mobile_non_reporting'] += 1
    
            if not ('mobile_metric',id) in self.entities_with_conditions:
                self.__metrics['mobile_without_conditions'] += 1
            else:
                self.__metrics['mobile_with_conditions'] += 1

        return result_apps

    def get_browser_metrics(self):
        result_apps = []
        browser_apps, _ = self.__account.browser_applications()
        self.__metrics['browser_total'] = len(browser_apps)
        for browser_app in browser_apps:
            id = browser_app['id']

            if not ('browser_metric',id) in self.entities_with_conditions:
                self.__metrics['browser_without_conditions'] += 1
            else:
                self.__metrics['browser_with_conditions'] += 1
        return result_apps # to be implemented

    def get_alerts_policies_metrics(self, current_time):
        alerts_policies, _ = self.__account.alerts_policies()
        for alerts_policy in alerts_policies:
            self.__metrics['alerts_policies_total'] += 1

            incident_preference = alerts_policy['incident_preference']
            if incident_preference == 'PER_POLICY':
                self.__metrics['alerts_policies_per_policy'] += 1
            elif incident_preference == 'PER_CONDITION':
                self.__metrics['alerts_policies_per_condition'] += 1
            elif incident_preference == 'PER_CONDITION_AND_TARGET':
                self.__metrics['alerts_policies_per_target'] += 1

            update_delta = current_time - alerts_policy['updated_at'] / 1000
            if update_delta < NewRelicAccountMaturity.__WEEK_TIME:
                self.__metrics['alerts_policies_a_week_old'] += 1
            elif update_delta < NewRelicAccountMaturity.__MONTH_TIME:
                self.__metrics['alerts_policies_a_month_old'] += 1

    def metrics(self):
        start_time = time.time()
        self.reset_metrics()
        self.cache_apps_with_deployments()
        self.cache_apps_with_labels()
        self.cache_entities_with_conditions()
        apm_apps = self.get_apm_metrics()
        browser_apps = self.get_browser_metrics()
        mobile_apps = self.get_mobile_metrics()
        self.get_users_metrics()
        self.get_alerts_policies_metrics(start_time)
        elapsed_time = round(time.time() - start_time, 2)
        self.__metrics['get_metadata_duration'] = elapsed_time
        return [self.__metrics], apm_apps, browser_apps, mobile_apps


def main():
    try:
        account_maturity = NewRelicAccountMetrics()
        summary, apm_apps, browser_apps, mobile_apps = account_maturity.metrics()
        print(json.dumps(summary, sort_keys=True, indent=4))
        print(json.dumps(apm_apps, sort_keys=True, indent=4))
        print(json.dumps(browser_apps, sort_keys=True, indent=4))
        print(json.dumps(mobile_apps, sort_keys=True, indent=4))
    
    except Exception as exception:
        print(exception.args[0])


if __name__ == '__main__':
    main()