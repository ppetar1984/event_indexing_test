import datetime
import json
import unittest

from mock import MagicMock, patch, Mock

from event_indexing.scrapers.ems.fayetteville_911_cad import Fayetteville911Cad
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.ems import get_data_path


class Fayetteville911CadTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('fayetteville_911_cad.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = Fayetteville911Cad(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'Fayetteville911Cad')

    def test_tz_name(self):
        self.assertEqual(self.scraper.tz_name, 'US/Central')

    @patch('event_indexing.scrapers.ems.fayetteville_911_cad.get_tz_now')
    def test_get_request_args(self, mock_get_tz_now):
        mock_get_tz_now.return_value = datetime.datetime(2016, 8, 18, 19, 56, 39, tzinfo=self.scraper.get_tz_info())
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 3)

        self.assertEqual(url,
                         'http://gis.fayetteville-ar.gov/DispatchLogs/json/getIncidents.cshtml/2016-8-17/2016-8-19')
        self.assertFalse(verify)
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
        result = next(self.scraper.scrape(self.data['response']))

        created_at = result['created_at']
        date_time = result['datetime']
        latitude = result['latitude']
        longitude = result['longitude']
        incident = result['incident']

        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(date_time, '08-18-2016 19:50:00')
        self.assertEqual(latitude, 36.079222285)
        self.assertEqual(longitude, -94.24650377)
        self.assertEqual(incident, 'THEFT')

    @patch('time.time', return_value=1471568199)
    def test_scrape_delay(self, mock_time):
        size = sum(1 for i in self.scraper.scrape(self.data['response']))

        self.assertEqual(size, 1)

    def test_parse_alert(self):
        alert = self.scraper.parse(self.data['incident'])

        alert_keys = alert.keys()

        detected_at = alert['detected_at']
        latitude = alert['coordinates']['lat']
        longitude = alert['coordinates']['long']
        incident = alert['incident']
        category = alert['category']
        prediction = alert['prediction']

        self.assertEqual(len(alert_keys), 6)

        self.assertEqual(detected_at, 1471567800.0)
        self.assertEqual(latitude, 36.079222285)
        self.assertEqual(longitude, -94.24650377)
        self.assertEqual(incident, 'THEFT')
        self.assertEqual(category, 'crime')
        self.assertEqual(prediction, 1.0)

    @patch('event_indexing.scrapers.ems.fayetteville_911_cad.get_tz_now')
    def test_parse_alert_source(self, mock_get_tz_now):
        mock_get_tz_now.return_value = datetime.datetime(2016, 8, 18, 19, 56, 39, tzinfo=self.scraper.get_tz_info())
        alert = self.scraper.parse(self.data['incident'])
        source = alert['source']

        source_keys = source.keys()
        provider_keys = source['provider'].keys()

        created_at = source['created_at']
        incident_id = source['id']
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
        self.assertEqual(incident_id, '4736e084af57137066dfdf50e6263463')
        self.assertEqual(text, 'THEFT')
        self.assertEqual(source_type, TYPE_CAD_API)
        self.assertEqual(coordinates, [-94.24650377, 36.079222285])
        self.assertEqual(geo_type, 'Point')

        self.assertEqual(provider_api_host, 'gis.fayetteville-ar.gov')
        self.assertEqual(provider_api_route, '/DispatchLogs/json/getIncidents.cshtml/2016-8-17/2016-8-19')
        self.assertEqual(provider_api_name, 'Fayetteville 911')
        self.assertEqual(provider_api_id, 'fayetteville_911_cad')
        self.assertEqual(provider_api_url, 'http://www.fayetteville-ar.gov/1333/Police-Fire-Dispatch-Logs')

    @patch('event_indexing.scrapers.ems.fayetteville_911_cad.get_tz_now')
    @patch('time.time', return_value=1471568199)
    def test_run(self, mock_time, mock_get_tz_now):
        mock_get_tz_now.return_value = datetime.datetime(2016, 8, 18, 19, 56, 39, tzinfo=self.scraper.get_tz_info())
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

        self.assertEqual(size, 2)

    def test_get_incident(self):
        incident = self.scraper.get_incident(self.data['raw_incident'])

        incident_id = incident['id']
        details = incident['incident']
        created_at = incident['created_at']
        latitude = incident['latitude']
        longitude = incident['longitude']

        self.assertEqual(incident_id, '4736e084af57137066dfdf50e6263463')
        self.assertEqual(details, 'THEFT')
        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(latitude, 36.079222285)
        self.assertEqual(longitude, -94.24650377)

    def test_get_incident_address(self):
        incident = self.scraper.get_incident(self.data['raw_incident_address'])

        incident_id = incident['id']
        details = incident['incident']
        created_at = incident['created_at']
        address = incident['address']

        self.assertEqual(incident_id, 'cb6d9573638a23206aaa31d67798b63f')
        self.assertEqual(details, 'THEFT')
        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(address, '4023 W SONG BIRD PL')

    def test_get_description(self):
        description = self.scraper.get_description(self.data['raw_incident'])

        self.assertEqual(description, 'THEFT')

    def test_get_description_code(self):
        description = self.scraper.get_description(self.data['raw_incident_code'])

        self.assertEqual(description, 'ACCIDENT, NO PERSONAL INJURIES, ROAD BLOCKED')

    def test_get_date_time(self):
        date_time = self.scraper.get_date_time(self.data['raw_incident'])

        self.assertEqual(date_time, '08-18-2016 19:50:00')

    def test_is_valid_incident(self):
        is_valid_coordinate_incident = {'Address': '<UNKNOWN>', 'lat': 36.079222285, 'lon': -94.24650377}
        is_valid_address_incident = {'Address': '4023 W SONG BIRD PL', 'lat': -361, 'lon': -361}
        is_invalid_incident = {'Address': '<UNKNOWN>', 'lat': -361, 'lon': -361}

        is_valid_coordinate = self.scraper.is_valid_incident(is_valid_coordinate_incident)
        is_valid_address = self.scraper.is_valid_incident(is_valid_address_incident)
        is_invalid = self.scraper.is_valid_incident(is_invalid_incident)

        self.assertTrue(is_valid_coordinate)
        self.assertTrue(is_valid_address)
        self.assertFalse(is_invalid)
