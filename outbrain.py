import yaml
import json
import pytz
import datetime as dt
import requests
import time
import urllib.parse
import os


class OutbrainAPI(object):
    if os.name == 'posix':
        PATH = './outbrain_ads_api_connector/outbrain_credentials.yml'
    else:
        PATH = r'C:\Users\outbrain_credentials.yml'

    print(PATH)
    outbrain_config = yaml.load(open(PATH, 'r'), Loader=yaml.FullLoader)
    _insert_time = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def __init__(self):
        # using class attributes
        diff = dt.date.today() - \
            dt.datetime.strptime(
                OutbrainAPI.outbrain_config['token_date'], '%Y-%m-%d').date()
        print('Age(in # of days) of Access Token :{}'.format(diff.days))
        if diff.days > 29:
            print("Getting New Token")
            print(OutbrainAPI.outbrain_config)
            # raise ValueError('A very specific bad thing happened.')
            OutbrainAPI.outbrain_config['api_token'] = self.get_token(
                OutbrainAPI.outbrain_config['user'],
                OutbrainAPI.outbrain_config['password']
            )
            OutbrainAPI.outbrain_config['token_date'] = dt.date.today().strftime('%Y-%m-%d')
            with open(OutbrainAPI.PATH, 'w') as outfile:
                yaml.dump(OutbrainAPI.outbrain_config,
                          outfile, default_flow_style=False)

        # using instance attributes
        self.user = OutbrainAPI.outbrain_config['user']
        self.password = OutbrainAPI.outbrain_config['password']
        self.base_url = OutbrainAPI.outbrain_config['base_url']
        self.token_date = OutbrainAPI.outbrain_config['token_date']
        self.api_token = OutbrainAPI.outbrain_config['api_token']
        # print((self.user, self.password, self.base_url,
        # self.token_date, self.api_token))

        # Outbrain's reporting is in Eastern time
        self.locale = pytz.timezone("US/Eastern")
        if not self.base_url.endswith('/'):
            self.base_url += '/'

    def _request(self, path, payload={}, data={}, method='GET'):
        """

        """
        if method not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise ValueError('Illegal HTTP method {}'.format(method))

        url = self.base_url + path

        request_func = getattr(requests, method.lower())

        headers = {'OB-TOKEN-V1': self.api_token,
                   'Content-Type': 'application/json'}

        r = request_func(url, headers=headers, params=payload, data=data)
        print(r.url)

        if 200 <= r.status_code < 300:
            return r.json()
        return None

    def get_token(self, user, password):
        """ Requesting Access Token Explicitly
        Authentication requests to obtain token (/login) limited to 2 requests per hour per user.
        TO DO: Check if a valid token exists if it does not then request a new token
        """
        print("Getting New Token")
        token_url = OutbrainAPI.outbrain_config['base_url'] + 'login'
        basic_auth = requests.auth.HTTPBasicAuth(user, password)
        r = requests.get(token_url, auth=basic_auth)
        results = json.loads(r.text)
        print(results)
        if results.get('message') is not None:
            raise ValueError(
                'Number of Login requests for User exceeded rate limit[limited to 2 requests per hour per user]')
        return results['OB-TOKEN-V1']

    def get_marketers_dictionary(self):
        """
        """
        path = 'marketers'
        result = self._request(path)
        return result.get('marketers', [])

    def get_marketers_ids_name(self):
        """
        """
        m_dict = self.get_marketers_dictionary()
        return {dd['id']: dd['name'] for dd in m_dict if dd['id'] not in ['org_id_to_exclude']}

    def get_campaigns_marketers_dictionary(self, l_mk_ids):
        """
        Reference: https://amplifyv01.docs.apiary.io/#reference/campaigns/campaigns-collection-via-marketer/list-all-campaigns-associated-with-a-marketer
        """
        payload = {
            'limit': 50,
            'offset': 0,
            'includeArchived': 'true',
            'fetch': 'all',
            'extraFields': 'CustomAudience,Locations,InterestsTargeting,BidBySections,BlockedSites,PlatformTargeting,CampaignOptimization,Scheduling',
            'includeConversionDetails': 'true',
            'conversionsByClickDate': 'true'
        }

        stage1 = list()
        for mk_id in l_mk_ids:
            path = 'marketers/{0}/campaigns'.format(mk_id)
            result_dd = self._request(path, payload=payload)
            if len(result_dd.get('campaigns', [])) != 0:
                stage1.append(result_dd)

        stage2 = list()
        for dd in stage1:
            for cmpgn_dd in dd['campaigns']:
                stage2.append(cmpgn_dd)

        result = list()
        for dd in stage2:
            flat_dd = self.flatten_json(dd)
            row = dict()
            row['_insert_time'] = self._insert_time
            row['id'] = flat_dd.get('id')
            row['name'] = flat_dd.get('name')
            row['enabled'] = flat_dd.get('enabled')
            row['creationTime'] = flat_dd.get('creationTime')
            row['currency'] = flat_dd.get('currency')
            row['marketerId'] = flat_dd.get('marketerId')
            row['marketerName'] = self.get_marketers_ids_name().get(
                flat_dd.get('marketerId'))
            row['budget_id'] = flat_dd.get('budget_id')
            row['budget_shared'] = flat_dd.get('budget_shared')
            row['budget_amount'] = flat_dd.get('budget_amount')
            row['budget_type'] = flat_dd.get('budget_type')
            row['budget_pacing'] = flat_dd.get('budget_pacing')
            row['campaignOptimizationType'] = flat_dd.get(
                'campaignOptimization_optimizationType')
            result.append(row)

        return result

    def get_campaign_name_ids(self, l_mk_ids):
        """
        """
        data = self.get_campaigns_marketers_dictionary(l_mk_ids)
        return (
            [dd['id'] for dd in data],
            {dd['id']: dd['name'] for dd in data},
            {dd['id']: dd['marketerId'] for dd in data}
        )

    def get_promoted_links_campaings_dictionary(self, l_cmpgns):
        """
        """
        stage1 = list()
        for _bool in [True, False]:
            payload = {
                'limit': 200,
                'offset': 0,
                'enabled': _bool,
                'statuses': 'APPROVED,PENDING,REJECTED',
                'sort': '-creationTime'}
            for cmpgn in l_cmpgns:
                path = 'campaigns/{0}/promotedLinks'.format(cmpgn)
                response = self._request(path, payload=payload)
                stage1.append(response)

        stage2 = list()
        for _dd in stage1:
            if _dd.get('promotedLinks', []):
                l_pl_dd = _dd['promotedLinks']
                for pl_dd in l_pl_dd:
                    stage2.append(pl_dd)

        result = list()
        for dd in stage2:
            flat_dd = self.flatten_json(dd)
            row = dict()
            row['_insert_time'] = self._insert_time
            row['id'] = flat_dd.get('id', None)
            row['campaignId'] = flat_dd.get('campaignId', None)
            row['text'] = flat_dd.get('text', None)
            row['creationTime'] = flat_dd.get('creationTime', None)
            row['url'] = flat_dd.get('url', None)
            row['siteName'] = flat_dd.get('siteName', None)
            row['sectionName'] = flat_dd.get('sectionName', None)
            row['status'] = flat_dd.get('status', None)
            row['enabled'] = flat_dd.get('enabled', None)
            row['cachedImageUrl'] = flat_dd.get('cachedImageUrl', None)
            row['archived'] = flat_dd.get('archived', None)
            row['documentLanguage'] = flat_dd.get('documentLanguage', None)
            row['onAirStatus_onAir'] = flat_dd.get('onAirStatus_onAir', None)
            row['onAirStatus_reason'] = flat_dd.get('onAirStatus_reason', None)
            row['baseUrl'] = flat_dd.get('baseUrl', None)
            row['documentId'] = flat_dd.get('documentId', None)
            row['approvalStatus_status'] = flat_dd.get(
                'approvalStatus_status', None)
            row['approvalStatus_isEditable'] = flat_dd.get(
                'approvalStatus_isEditable', None)
            result.append(row)
        return result

    def get_campaigns_periodic_performance(self, from_date, to_date, l_mk_ids):
        """
        """
        payload = {
            'from': from_date,
            'to': to_date,
            'limit': 500,
            'offset': 0,
            'includeArchivedCampaigns': 'true',
            'breakdown': 'daily',
            'includeConversionDetails': 'true',
            'conversionsByClickDate': 'true'
        }
        l_cr_dd = list()
        for mk_id in l_mk_ids:
            path = 'reports/marketers/{0}/campaigns/periodic'.format(mk_id)
            response = self._request(path, payload=payload)
            if response.get('campaignResults', []):
                l_cr_dd.append(response)

        result = []
        for dd in l_cr_dd:
            l_cmpgn_results = dd['campaignResults']
            # cmpgn_results = dict_keys(['campaignId', 'results', 'totalResults'])
            for cmpgn_results in l_cmpgn_results:
                r_list = cmpgn_results['results']
                for metrics_dd in r_list:
                    m_dd = metrics_dd['metrics']
                    if m_dd.get('impressions', None):
                        if m_dd.get('impressions', None) != 0:
                            row = dict()
                            row['_insert_time'] = self._insert_time
                            row['date'] = metrics_dd.get(
                                'metadata', None)['id']
                            row['campaignId'] = cmpgn_results['campaignId']
                            row['impressions'] = m_dd['impressions']
                            row['clicks'] = m_dd['clicks']
                            row['totalConversions'] = m_dd['totalConversions']
                            row['conversions'] = m_dd['conversions']
                            row['viewConversions'] = m_dd['viewConversions']
                            row['spend'] = m_dd['spend']
                            row['ecpc'] = m_dd['ecpc']
                            row['ctr'] = m_dd['ctr']
                            row['conversionRate'] = m_dd['conversionRate']
                            row['viewConversionRate'] = m_dd['viewConversionRate']
                            row['cpa'] = m_dd['cpa']
                            row['totalCpa'] = m_dd['totalCpa']
                            row['totalValue'] = m_dd['totalValue']
                            row['totalSumValue'] = m_dd['totalSumValue']
                            row['sumValue'] = m_dd['sumValue']
                            row['viewSumValue'] = m_dd['viewSumValue']
                            row['totalAverageValue'] = m_dd['totalAverageValue']
                            row['averageValue'] = m_dd['averageValue']
                            row['viewAverageValue'] = m_dd['viewAverageValue']
                            result.append(row)
        return result

    def get_promoted_link_periodic_performance(self, from_date, to_date, mk_cmpgn_ids):
        """
        ref: https://amplifyv01.docs.apiary.io/#reference/promotedlinks/promotedlinks-collection/list-promotedlinks-for-campaign
        path: reports/marketers/id/campaigns/campaignId/periodicContent
        """
        payload = {
            'from': from_date,
            'to': to_date,
            'limit': 500,
            'offset': 0,
            'includeArchivedCampaigns': 'true',
            'breakdown': 'daily',
            'includeConversionDetails': 'true',
            'conversionsByClickDate': 'true'
        }

        l_pl_r_dd = []
        for _ids in mk_cmpgn_ids:
            time.sleep(2)
            mk_id, cmpgn_id = _ids
            path = 'reports/marketers/{0}/campaigns/{1}/periodicContent'.format(mk_id, cmpgn_id)
            response = self._request(path, payload=payload)
            if response.get('promotedLinkResults', []):
                l_pl_r_dd.append(response)

        result = []
        for dd in l_pl_r_dd:
            # dd : dict_keys(['promotedLinkResults', 'totalPromotedLinks'])
            l_pl_results = dd['promotedLinkResults']
            for pl_results_dd in l_pl_results:
                # results_dd : dict_keys(['metadata', 'metrics'])
                pl_r_list = pl_results_dd['results']
                for results_dd in pl_r_list:
                    m_dd = results_dd['metrics']
                    if m_dd.get('impressions', None):
                        if m_dd.get('impressions', None) != 0:
                            row = dict()
                            row['_insert_time'] = self._insert_time
                            row['date'] = results_dd.get('metadata', None)['id']
                            row['promotedLinkId'] = pl_results_dd.get('promotedLinkId', None)
                            row['impressions'] = m_dd.get('impressions', None)
                            row['clicks'] = m_dd.get('clicks', None)
                            row['totalConversions'] = m_dd.get('totalConversions', None)
                            row['conversions'] = m_dd.get('conversions', None)
                            row['viewConversions'] = m_dd.get('viewConversions', None)
                            row['spend'] = m_dd.get('spend', None)
                            row['ecpc'] = m_dd.get('ecpc', None)
                            row['ctr'] = m_dd.get('ctr', None)
                            row['conversionRate'] = m_dd.get('conversionRate', None)
                            row['viewConversionRate'] = m_dd.get('viewConversionRate', None)
                            row['cpa'] = m_dd.get('cpa', None)
                            row['totalCpa'] = m_dd.get('totalCpa', None)
                            row['totalValue'] = m_dd.get('totalValue', None)
                            row['totalSumValue'] = m_dd.get('totalSumValue', None)
                            row['sumValue'] = m_dd.get('sumValue', None)
                            row['viewSumValue'] = m_dd.get('viewSumValue', None)
                            row['totalAverageValue'] = m_dd.get('totalAverageValue', None)
                            row['averageValue'] = m_dd.get('averageValue', None)
                            row['viewAverageValue'] = m_dd.get('viewAverageValue', None)
                            result.append(row)

        return result

    def get_campaigns_region_performance(self, from_date, to_date, l_mk_ids):
        """
        NOTE: Does not have Perdioc Segment
        #reference/performance-reporting/geo-by-campaign/retrieve-performance-statistics-for-all-marketer-campaigns-by-geo
        Ref: https://amplifyv01.docs.apiary.io/
        RUN: Need to Run Daily
        """

        result = []
        list_dates = self.get_marketers_performance(from_date, to_date, l_mk_ids)

        for _date in list_dates:
            payload = {
                'from': _date,
                'to': _date,
                'limit': 500,
                'offset': 0,
                'includeArchivedCampaigns': 'true',
                'breakdown': 'region',
                'includeConversionDetails': 'true',
                'conversionsByClickDate': 'true'
            }

            l_c_p_region = list()
            for mk_id in l_mk_ids:
                path = 'reports/marketers/{0}/campaigns/geo'.format(mk_id)
                response = self._request(path, payload=payload)
                if response.get('campaignResults', []):
                    l_c_p_region.append(response)
                time.sleep(5)

            for dd in l_c_p_region:
                # structure of dd : dict_keys(['campaignResults', 'totalCampaigns'])
                l_cmpgn_results = dd['campaignResults']
                for cmpgn_result_dd in l_cmpgn_results:
                    # structure of dd : dict_keys(['campaignId', 'results', 'totalResults'])
                    r_list = cmpgn_result_dd['results']
                    for metrics_dd in r_list:
                        meta_dd = metrics_dd['metadata']
                        m_dd = metrics_dd['metrics']
                        if m_dd.get('impressions', None):
                            if m_dd.get('impressions', None) != 0:
                                row = dict()
                                row['_insert_time'] = self._insert_time
                                row['date'] = _date
                                row['campaignId'] = cmpgn_result_dd.get('campaignId', None)
                                row['id'] = meta_dd.get('id', None)
                                row['name'] = meta_dd.get('name', None)
                                row['code'] = meta_dd.get('code', None)
                                row['countryId'] = meta_dd.get('countryId', None)
                                row['countryName'] = meta_dd.get('countryName', None)
                                row['countryCode'] = meta_dd.get('countryCode', None)
                                row['impressions'] = m_dd.get('impressions', None)
                                row['clicks'] = m_dd.get('clicks', None)
                                row['totalConversions'] = m_dd.get('totalConversions', None)
                                row['conversions'] = m_dd.get('conversions', None)
                                row['viewConversions'] = m_dd.get('viewConversions', None)
                                row['spend'] = m_dd.get('spend', None)
                                row['ecpc'] = m_dd.get('ecpc', None)
                                row['ctr'] = m_dd.get('ctr', None)
                                row['conversionRate'] = m_dd.get('conversionRate', None)
                                row['viewConversionRate'] = m_dd.get('viewConversionRate', None)
                                row['cpa'] = m_dd.get('cpa', None)
                                row['totalCpa'] = m_dd.get('totalCpa', None)
                                row['totalValue'] = m_dd.get('totalValue', None)
                                row['totalSumValue'] = m_dd.get('totalSumValue', None)
                                row['sumValue'] = m_dd.get('sumValue', None)
                                row['viewSumValue'] = m_dd.get('viewSumValue', None)
                                row['totalAverageValue'] = m_dd.get('totalAverageValue', None)
                                row['averageValue'] = m_dd.get('averageValue', None)
                                row['viewAverageValue'] = m_dd.get('viewAverageValue', None)
                                result.append(row)
        return result

    @staticmethod
    def flatten_json(y):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)
        return out

    @staticmethod
    def generate_date(from_date: str, to_date: str):
        from_date = dt.datetime.strptime(from_date, '%Y-%m-%d')
        to_date = dt.datetime.strptime(to_date, '%Y-%m-%d')

        delta = to_date - from_date
        dates = []
        for i in range(delta.days + 1):
            day = from_date + dt.timedelta(days=i)
            dates.append(day.strftime("%Y-%m-%d"))

        return dates

    @staticmethod
    def create_dates(lb_window=29, days_skip=0):
        today = dt.datetime.utcnow()
        to_date = today - dt.timedelta(1 + days_skip)
        from_date = to_date - dt.timedelta(lb_window)
        return (from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))

    def get_marketers_performance(self, from_date, to_date, l_mk_ids):
        """
        REF: https://amplifyv01.docs.apiary.io/#reference/performance-reporting/retrieve-periodic-performance-statistics-for-a-marketer
        PATH : reports/marketers/id/periodic
        """
        payload = {
            'from': from_date,
            'to': to_date,
            'limit': 500,
            'offset': 0,
            'includeArchivedCampaigns': 'true',
            'breakdown': 'daily',
            'includeConversionDetails': 'true',
            'conversionsByClickDate': 'true',
            'sort': '-impressions',
            'filter': 'impressions+ge+1'
        }

        headers = {'OB-TOKEN-V1': self.api_token,
                   'Content-Type': 'application/json'}

        l_mr_dd = list()
        for mk_id in l_mk_ids:
            path = 'reports/marketers/{0}/periodic'.format(mk_id)
            url = self.base_url + path
            response = requests.get(url, params=payload)
            r_url = urllib.parse.unquote(response.url)
            response = requests.get(r_url, headers=headers)
            if response.json().get('results', []):
                l_mr_dd.append(response.json().get('results'))

        dates_metrics = []
        for d_list in l_mr_dd:
            for dd in d_list:
                if dd.get('metrics', None):
                    m_dd = dd.get('metrics', None)
                    if m_dd.get('impressions') > 0:
                        dates_metrics.append(dd['metadata']['id'])

        dates = [dt.datetime.strptime(ts, "%Y-%m-%d") for ts in dates_metrics]
        dates.sort()
        sorteddates = [dt.datetime.strftime(ts, "%Y-%m-%d") for ts in dates]
        print(sorteddates)
        return sorteddates
