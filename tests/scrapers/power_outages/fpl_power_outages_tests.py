import json
import unittest

from mock import MagicMock, patch, Mock

from event_indexing.scrapers.power_outages.fpl_power_outages import FPLPowerOutages
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.power_outages import get_data_path


class FPLPowerOutagesTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('fpl_power_outages.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = FPLPowerOutages(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'FPLPowerOutages')

    def test_tz_name(self):
        self.assertEqual(self.scraper.tz_name, 'US/Eastern')

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()
        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 3)

        self.assertEqual(url, 'http://www.fplmaps.com/customer/outage/StormFeedRestoration.json')
        self.assertIsNone(verify)
        self.assertIsNotNone(headers)
        self.assertIsNone(params)
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

    def test_get_params(self):
        params = self.scraper.get_params()

        self.assertIsNone(params)

    def test_get_data(self):
        data = self.scraper.get_data()

        self.assertIsNone(data)

    def test_get_max_delay(self):
        max_delay = self.scraper.get_max_delay()

        self.assertEqual(max_delay, 3600)

    @patch('time.time', return_value=1471568199)
    def test_scrape(self, mock_time):
        incidents = self.data['response']
        result = next(self.scraper.scrape(incidents))

        incident_id = result['id']
        latitude = result['latitude']
        longitude = result['longitude']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(incident_id, '8549cf681a9ec8c2c73df8112ab848ac')
        self.assertEqual(latitude, 29.332)
        self.assertEqual(longitude, -81.061)
        self.assertEqual(incident, 'Power Outage')

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
        latitude = alert['coordinates']['lat']
        longitude = alert['coordinates']['long']
        category = alert['category']
        prediction = alert['prediction']

        self.assertEqual(len(alert_keys), 6)

        self.assertEqual(detected_at, 1471567800.0)
        self.assertEqual(incident, 'Power Outage')
        self.assertEqual(latitude, 29.332)
        self.assertEqual(longitude, -81.061)
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
        coordinates = source['geo']['coordinates']
        geo_type = source['geo']['type']

        provider_api_host = source['provider']['api_host']
        provider_api_route = source['provider']['api_route']
        provider_api_name = source['provider']['name']
        provider_api_url = source['provider']['url']
        provider_api_id = source['provider']['id']

        self.assertEqual(len(source_keys), 6)
        self.assertEqual(len(provider_keys), 5)

        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(id, '8549cf681a9ec8c2c73df8112ab848ac')
        self.assertEqual(text, 'Power Outage')
        self.assertEqual(source_type, TYPE_CAD_API)
        self.assertEqual(coordinates, [-81.061, 29.332])
        self.assertEqual(geo_type, 'Point')

        self.assertEqual(provider_api_host, 'www.fplmaps.com')
        self.assertEqual(provider_api_route, '/customer/outage/StormFeedRestoration.json')
        self.assertEqual(provider_api_name, 'Florida Power and Light')
        self.assertEqual(provider_api_id, 'fpl_power_outages')
        self.assertEqual(provider_api_url, 'http://www.fplmaps.com/')

    @patch('time.time', return_value=1471568199)
    def test_run(self, mock_time):
        self.scraper.publish = Mock()
        self.scraper.request = MagicMock(return_value=self.data['response'])
        self.scraper.run()

        self.scraper.publish.assert_called_once_with(self.data['incidents'])

    @patch('requests.post', return_value=MagicMock(autospec=True))
    def test_publish_no_incidents(self, post_mock):
        self.scraper.publish(incidents=[])

        post_mock.assert_not_called()

    def test_get_incidents(self):
        incidents = self.scraper.get_incidents(self.data['response'])

        size = len(incidents)

        self.assertEqual(size, 1)

    @patch('time.time', return_value=1471568199)
    def test_get_incident(self, mock_time):
        result = self.scraper.get_incident(self.data['raw_incident'])

        latitude = result['latitude']
        longitude = result['longitude']
        incident_id = result['id']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(latitude, 29.332)
        self.assertEqual(longitude, -81.061)
        self.assertEqual(incident_id, '8549cf681a9ec8c2c73df8112ab848ac')
        self.assertEqual(incident, 'Power Outage')
        self.assertEqual(created_at, 1471567800.0)

    def test_is_valid_incident(self):
        valid_raw_incident = {'customersAffected': '10'}
        invalid_raw_incident = {'customersAffected': '5'}

        is_valid = self.scraper.is_valid_incident(valid_raw_incident)
        is_invalid = self.scraper.is_valid_incident(invalid_raw_incident)

        self.assertTrue(is_valid)
        self.assertFalse(is_invalid)
