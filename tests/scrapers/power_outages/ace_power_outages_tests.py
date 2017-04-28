import datetime
import json
import unittest

from mock import MagicMock, patch, Mock

from event_indexing.scrapers.power_outages.ace_power_outages import ACEPowerOutages
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.power_outages import get_data_path


class ACEPowerOutagesTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('ace_power_outages.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = ACEPowerOutages(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'ACEPowerOutages')

    def test_tz_name(self):
        tz_name = self.scraper.tz_name

        self.assertEqual(tz_name, 'US/Eastern')

    def test_get_url(self):
        url = self.scraper.get_url(directory='2017_03_16_01_15_30', index='0320010122')

        self.assertEqual(url,
                         'http://stormcenter.atlanticcityelectric.com.s3.amazonaws.com/data/interval_generation_data/2017_03_16_01_15_30/outages/0320010122.js')

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

        now = params['timestamp']

        self.assertEqual(now, 1471568199000)

    def test_get_data(self):
        data = self.scraper.get_data()

        self.assertIsNone(data)

    def test_get_max_delay(self):
        max_delay = self.scraper.get_max_delay()

        self.assertEqual(max_delay, 3600)

    @patch('event_indexing.scrapers.power_outages.ace_power_outages.get_tz_now')
    @patch('time.time', return_value=1471568199)
    def test_scrape(self, mock_time, mock_get_tz_now):
        mock_get_tz_now.return_value = datetime.datetime(2016, 8, 18, 20, 56, 39, tzinfo=self.scraper.get_tz_info())
        incidents = self.data['response']
        result = next(self.scraper.scrape(incidents))

        incident_id = result['id']
        latitude = result['latitude']
        longitude = result['longitude']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(created_at, 1471567800)
        self.assertEqual(incident_id, '4a80d97532580676d95d41ae64b0ef56')
        self.assertEqual(latitude, 39.70962)
        self.assertEqual(longitude, -75.3273)
        self.assertEqual(incident, 'Wires Down')

    @patch('event_indexing.scrapers.power_outages.ace_power_outages.get_tz_now')
    @patch('time.time', return_value=1471568199)
    def test_scrape_delay(self, mock_time, mock_get_tz_now):
        mock_get_tz_now.return_value = datetime.datetime(2016, 8, 18, 20, 56, 39, tzinfo=self.scraper.get_tz_info())
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
        self.assertEqual(incident, 'Wires Down')
        self.assertEqual(latitude, 39.70962)
        self.assertEqual(longitude, -75.3273)
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
        self.assertEqual(id, '4a80d97532580676d95d41ae64b0ef56')
        self.assertEqual(text, 'Wires Down')
        self.assertEqual(source_type, TYPE_CAD_API)
        self.assertEqual(coordinates, [-75.3273, 39.70962])
        self.assertEqual(geo_type, 'Point')

        self.assertEqual(provider_api_host, 'stormcenter.atlanticcityelectric.com.s3.amazonaws.com')
        self.assertEqual(provider_api_route, '/data/interval_generation_data/2017_03_16_01_15_30/outages/0320010122.js')
        self.assertEqual(provider_api_name, 'Atlantic City Electric')
        self.assertEqual(provider_api_id, 'ace_power_outages')
        self.assertEqual(provider_api_url,
                         'http://www.atlanticcityelectric.com/pages/connectwithus/outages/outagemaps.aspx')

    @patch('event_indexing.scrapers.power_outages.ace_power_outages.get_tz_now')
    @patch('time.time', return_value=1471568199)
    @patch('event_indexing.scrapers.power_outages.base_ifsc_scraper.Pool.imap_unordered')
    def test_run(self, mock_requests, mock_time, mock_get_tz_now):
        mock_get_tz_now.return_value = datetime.datetime(2016, 8, 18, 20, 56, 39, tzinfo=self.scraper.get_tz_info())
        incidents = self.data['response']
        meta = self.data['meta']

        ACEPowerOutages._indexes = [self.data['indexes']]
        ACEPowerOutages._directory = meta['directory']

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

    @patch('event_indexing.scrapers.power_outages.ace_power_outages.get_tz_now')
    def test_get_incident(self, mock_get_tz_now):
        mock_get_tz_now.return_value = datetime.datetime(2016, 8, 18, 20, 56, 39, tzinfo=self.scraper.get_tz_info())
        result = self.scraper.get_incident(self.data['raw_incident'])

        latitude = result['latitude']
        longitude = result['longitude']
        incident_id = result['id']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(latitude, 39.70962)
        self.assertEqual(longitude, -75.3273)
        self.assertEqual(incident_id, '4a80d97532580676d95d41ae64b0ef56')
        self.assertEqual(incident, 'Wires Down')
        self.assertEqual(created_at, 1471567800.0)

    def test_get_description(self):
        raw_incident = self.data['raw_incident']
        description = raw_incident['desc'][-1]
        description = self.scraper.get_description(description)

        self.assertEqual(description, 'Wires Down')

    def test_get_coordinates(self):
        latitude, longitude = self.scraper.get_coordinates(self.data['raw_incident'])

        self.assertEqual(latitude, 39.70962)
        self.assertEqual(longitude, -75.3273)

    def test_get_customers_affected(self):
        raw_incident = self.data['raw_incident']
        description = raw_incident['desc'][-1]
        customers = self.scraper.get_customers_affected(description)

        self.assertEqual(customers, 55)

    def test_get_date_time(self):
        raw_incident = self.data['raw_incident']
        description = raw_incident['desc'][-1]
        date_time = self.scraper.get_date_time(description)

        self.assertEqual(date_time, 'Aug 18 2017 8:50 PM')
