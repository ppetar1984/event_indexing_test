import json
import unittest

from mock import MagicMock, patch, Mock

from event_indexing.scrapers.power_outages.au_power_outages import AUPowerOutages
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.power_outages import get_data_path


class AUPowerOutagesTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('au_power_outages.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = AUPowerOutages(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'AUPowerOutages')

    def test_tz_name(self):
        self.assertIsNone(self.scraper.tz_name)

    def test_get_url(self):
        url = self.scraper.get_url(directory='2017_03_21_20_01_30', index='0212310120')

        self.assertEqual(url,
                         'http://outagemap.myavista.com/resources/data/external/interval_generation_data/2017_03_21_20_01_30/outages/0212310120.json')

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

    @patch('time.time', return_value=1471568199)
    def test_get_params(self, mock_time):
        params = self.scraper.get_params()

        now = params['_']

        self.assertEqual(now, 1471568199000)

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
        self.assertEqual(incident_id, '7e45f5aaffc48209c62108a57096f57f')
        self.assertEqual(latitude, 47.7036)
        self.assertEqual(longitude, -117.37817)
        self.assertEqual(incident, 'Under Investigation')

    @patch('time.time', return_value=1471568199)
    def test_scrape_delay(self, mock_time):
        incidents = self.data['response']
        size = sum(1 for i in self.scraper.scrape(incidents))

        self.assertEqual(size, 1)

    def test_parse_alert(self):
        alert = self.scraper.parse(self.data['incident'], meta=self.data['meta'])

        alert_keys = alert.keys()

        detected_at = alert['detected_at']
        incident = alert['incident']
        latitude = alert['coordinates']['lat']
        longitude = alert['coordinates']['long']
        category = alert['category']
        prediction = alert['prediction']

        self.assertEqual(len(alert_keys), 6)

        self.assertEqual(detected_at, 1471567800.0)
        self.assertEqual(incident, 'Under Investigation')
        self.assertEqual(latitude, 47.7036)
        self.assertEqual(longitude, -117.37817)
        self.assertEqual(category, 'power_outage')
        self.assertEqual(prediction, 1.0)

    def test_parse_alert_source(self):
        alert = self.scraper.parse(self.data['incident'], meta=self.data['meta'])
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
        self.assertEqual(id, '7e45f5aaffc48209c62108a57096f57f')
        self.assertEqual(text, 'Under Investigation')
        self.assertEqual(source_type, TYPE_CAD_API)
        self.assertEqual(coordinates, [-117.37817, 47.7036])
        self.assertEqual(geo_type, 'Point')

        self.assertEqual(provider_api_host, 'outagemap.myavista.com')
        self.assertEqual(provider_api_route,
                         '/resources/data/external/interval_generation_data/2017_03_21_20_01_30/outages/0212310120.json')
        self.assertEqual(provider_api_name, 'Avista Utilities')
        self.assertEqual(provider_api_id, 'au_power_outages')
        self.assertEqual(provider_api_url, 'http://outagemap.myavista.com/external/default.html')

    @patch('time.time', return_value=1471568199)
    @patch('event_indexing.scrapers.power_outages.base_ifsc_scraper.Pool.imap_unordered')
    def test_run(self, mock_requests, mock_time):
        incidents = self.data['response']
        meta = self.data['meta']

        AUPowerOutages._indexes = [self.data['indexes']]
        AUPowerOutages._directory = meta['directory']

        mock_requests.return_value = [(incidents, meta)]
        self.scraper.publish = Mock()
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

    def test_get_incident(self):
        result = self.scraper.get_incident(self.data['raw_incident'])

        latitude = result['latitude']
        longitude = result['longitude']
        incident_id = result['id']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(latitude, 47.7036)
        self.assertEqual(longitude, -117.37817)
        self.assertEqual(incident_id, '7e45f5aaffc48209c62108a57096f57f')
        self.assertEqual(incident, 'Under Investigation')
        self.assertEqual(created_at, 1471567800.0)

    def test_get_description(self):
        raw_incident = self.data['raw_incident']
        raw_incident['desc']['cause'] = 'Under Investigation'
        description = self.scraper.get_description(raw_incident)

        self.assertEqual(description, 'Under Investigation')

    def test_get_description_unknown(self):
        description = self.scraper.get_description(self.data['raw_incident'])

        self.assertEqual(description, 'Under Investigation')

    def test_get_description_none(self):
        raw_incident = self.data['raw_incident']
        del raw_incident['desc']['cause']
        description = self.scraper.get_description(raw_incident)

        self.assertEqual(description, 'Power Outage')

    def test_get_coordinates(self):
        latitude, longitude = self.scraper.get_coordinates(self.data['raw_incident'])

        self.assertEqual(latitude, 47.7036)
        self.assertEqual(longitude, -117.37817)
