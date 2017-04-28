import json
import unittest

from mock import MagicMock, patch, Mock

from event_indexing.scrapers.power_outages.lu_power_outages import LUPowerOutages
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.power_outages import get_data_path


class LUPowerOutagesTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('lu_power_outages.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = LUPowerOutages(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'LUPowerOutages')

    def test_tz_name(self):
        self.assertEqual(self.scraper.tz_name, 'US/Eastern')

    def test_method(self):
        self.assertEqual(self.scraper.method, 'GET')

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        data = request_args.get('data')

        self.assertEqual(size, 4)

        self.assertEqual(url, 'http://www.outageentry.com/CustomerFacingAppJQM/ajaxShellOut.php')
        self.assertFalse(verify)
        self.assertIsNotNone(headers)
        self.assertIsNone(data)

    def test_get_headers(self):
        self.scraper.get_user_agent = MagicMock(return_value='test')
        headers = self.scraper.get_headers()

        size = len(headers.keys())

        user_agent = headers.get('User-Agent')
        content_type = headers.get('Content-Type')
        host = headers.get('Host')
        ramire_purpose = headers.get('X-Ramire-Purpose')

        self.assertEqual(size, 2)

        self.assertEqual(user_agent, 'test')
        self.assertEqual(content_type, 'application/json')
        self.assertIsNone(host)
        self.assertIsNone(ramire_purpose)

    def test_get_max_delay(self):
        max_delay = self.scraper.get_max_delay()

        self.assertEqual(max_delay, 3600)

    @patch('time.time', return_value=1471568199)
    def test_scrape(self, mock_time):
        incidents = self.data['response']
        result = next(self.scraper.scrape(incidents))

        incident_id = result['id']
        incident = result['incident']
        created_at = result['created_at']
        start_date = result['start_date']
        longitude = result['longitude']
        latitude = result['latitude']
        consumers_affected = result['consumers_affected']

        self.assertEqual(created_at, 1471567800)
        self.assertEqual(incident_id, '5501cc261b79ef4535eb70bf504d6e7f')
        self.assertEqual(incident, 'Power Outage')
        self.assertEqual(start_date, '04/24 02:31:01 pm')
        self.assertEqual(longitude, '-84.0379808698967')
        self.assertEqual(latitude, '36.3322327488042')
        self.assertEqual(consumers_affected, '14')

    @patch('time.time', return_value=1471568199)
    def test_scrape_delay(self, mock_time):
        incidents = self.data['response']
        size = sum(1 for i in self.scraper.scrape(incidents))

        self.assertEqual(size, 1)

    def test_parse_alert(self):
        alert = self.scraper.parse(self.data['incident'])

        alert_keys = alert.keys()

        detected_at = alert['detected_at']
        incident = alert['incident']
        category = alert['category']
        prediction = alert['prediction']

        self.assertEqual(len(alert_keys), 6)

        self.assertEqual(detected_at, 1471567800)
        self.assertEqual(incident, 'Power Outage')
        self.assertEqual(category, 'power_outage')
        self.assertEqual(prediction, 1.0)

    def test_parse_alert_source(self):
        alert = self.scraper.parse(self.data['incident'])
        source = alert['source']

        source_keys = source.keys()
        provider_keys = source['provider'].keys()

        created_at = source['created_at']
        id = source['id']
        text = source['text']
        source_type = source['type']

        provider_api_host = source['provider']['api_host']
        provider_api_route = source['provider']['api_route']
        provider_api_name = source['provider']['name']
        provider_api_url = source['provider']['url']
        provider_api_id = source['provider']['id']

        self.assertEqual(len(source_keys), 6)
        self.assertEqual(len(provider_keys), 5)

        self.assertEqual(created_at, 1471567800)
        self.assertEqual(id, '5501cc261b79ef4535eb70bf504d6e7f')
        self.assertEqual(text, 'Power Outage')
        self.assertEqual(source_type, TYPE_CAD_API)

        self.assertEqual(provider_api_host, 'www.outageentry.com')
        self.assertEqual(provider_api_route, '/CustomerFacingAppJQM/ajaxShellOut.php')
        self.assertEqual(provider_api_name, 'LaFollette Utilities')
        self.assertEqual(provider_api_id, 'lu_power_outages')
        self.assertEqual(provider_api_url, 'https://www.outageentry.com/CustomerFacingAppJQM/outage.php?clientid=LAFOLLETTE')

    @patch('time.time', return_value=1471568199)
    def test_run(self, mock_time):
        self.scraper.publish = Mock()
        self.scraper.request = MagicMock(return_value=self.data['response'])
        self.scraper.run()

        self.scraper.publish.assert_called_once_with(self.data['incidents'])

    @patch('requests.get', return_value=MagicMock(autospec=True))
    def test_publish_no_incidents(self, post_mock):
        self.scraper.publish(incidents=[])

        post_mock.assert_not_called()

    def test_get_incidents(self):
        incidents = self.scraper.get_incidents(self.data['response'])

        size = len(incidents)

        self.assertEqual(size, 1)

    def test_get_incident(self):
        result = self.data['raw_incident']
        result = self.scraper.get_incident(result)

        incident_id = result['id']
        incident = result['incident']
        created_at = result['created_at']
        longitude = result['longitude']
        latitude = result['latitude']
        consumers_affected = result['consumers_affected']

        self.assertEqual(incident_id, '5501cc261b79ef4535eb70bf504d6e7f')
        self.assertEqual(incident, 'Power Outage')
        self.assertEqual(created_at, 1471567800)
        self.assertEqual(longitude, '-84.0379808698967')
        self.assertEqual(latitude, '36.3322327488042')
        self.assertEqual(consumers_affected, '14')

    def test_get_url(self):
        url = self.scraper.get_url()

        self.assertEqual(url, 'http://www.outageentry.com/CustomerFacingAppJQM/ajaxShellOut.php')

    def test_get_params(self):
        params = self.scraper.get_params()

        param_1 = params['target']
        param_2 = params['action']
        param_3 = params['serviceIndex']
        param_4 = params['url']
        param_5 = params['']
        param_6 = params['_']

        self.assertEqual(param_1, 'device_markers')
        self.assertEqual(param_2, 'get')
        self.assertEqual(param_3, 1)
        self.assertEqual(param_4, '10.58.27.9')
        self.assertEqual(param_5, '3A80')
        self.assertEqual(param_6, 1471567800)

    def test_is_valid_incident(self):
        valid_affected_raw_incident = {'consumers_affected': '20'}
        invalid_affected_raw_incident = {'consumers_affected': '5'}

        is_valid_affected = self.scraper.is_valid_incident(valid_affected_raw_incident)
        is_invalid_affected = self.scraper.is_valid_incident(invalid_affected_raw_incident)

        self.assertTrue(is_valid_affected)
        self.assertFalse(is_invalid_affected)
