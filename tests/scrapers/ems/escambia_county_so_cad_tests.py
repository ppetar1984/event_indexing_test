import json
import unittest

from bs4 import BeautifulSoup
from mock import MagicMock, patch, Mock

from event_indexing.scrapers.ems.escambia_county_so_cad import EscambiaCountySOCad
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.ems import get_data_path


class EscambiaCountySOCadTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('escambia_county_so_cad.html')) as f:
            html = f.read()

        with open(get_data_path('escambia_county_so_cad.json')) as f:
            data = json.load(f)

        self.html = html
        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = EscambiaCountySOCad(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'EscambiaCountySOCad')

    def test_tz_name(self):
        self.assertEqual(self.scraper.tz_name, 'US/Central')

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 2)

        self.assertEqual(url, 'http://www.escambiaso.com/SmartWebClient/cadview.aspx')
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
        self.assertEqual(content_type, 'text/html; charset=utf-8')
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
        result = next(self.scraper.scrape(self.html))

        created_at = result['created_at']
        latitude = result['latitude']
        longitude = result['longitude']
        incident = result['incident']
        incident_id = result['id']

        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(latitude, 30.5108)
        self.assertEqual(longitude, -87.32834)
        self.assertEqual(incident, 'SUSP VEH')
        self.assertEqual(incident_id, 'ECSO16CAD276495')

    @patch('time.time', return_value=1471568199)
    def test_scrape_delay(self, mock_time):
        size = sum(1 for i in self.scraper.scrape(self.html))

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
        self.assertEqual(latitude, 30.5108)
        self.assertEqual(longitude, -87.32834)
        self.assertEqual(incident, 'SUSP VEH')
        self.assertEqual(category, 'crime')
        self.assertEqual(prediction, 1.0)

    def test_parse_alert_source(self):
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
        self.assertEqual(incident_id, 'ECSO16CAD276495')
        self.assertEqual(text, 'SUSP VEH')
        self.assertEqual(source_type, TYPE_CAD_API)
        self.assertEqual(coordinates, [-87.32834, 30.5108])
        self.assertEqual(geo_type, 'Point')

        self.assertEqual(provider_api_host, 'www.escambiaso.com')
        self.assertEqual(provider_api_route, '/SmartWebClient/cadview.aspx')
        self.assertEqual(provider_api_name, 'Escambia County Sheriff\'s Office')
        self.assertEqual(provider_api_id, 'escambia_county_so_cad')
        self.assertEqual(provider_api_url, 'http://www.escambiaso.com/index.php/crime-prevention/dispatched-calls/')

    @patch('time.time', return_value=1471568199)
    def test_run(self, mock_time):
        self.scraper.publish = Mock()
        self.scraper.request = MagicMock(return_value=self.html)
        self.scraper.run()

        self.scraper.publish.assert_called_once_with(self.data['incidents'])

    @patch('requests.post', return_value=MagicMock(autospec=True))
    def test_publish_no_incidents(self, post_mock):
        self.scraper.publish(incidents=[])

        post_mock.assert_not_called()

    def test_get_incidents(self):
        incidents = self.scraper.get_incidents(self.html)

        size = len(incidents)

        self.assertEqual(size, 19)

    def test_get_incident(self):
        incident = self.data['raw_incident']
        result = self.scraper.get_incident(incident)

        incident_id = result['id']
        latitude = result['latitude']
        longitude = result['longitude']
        date_time = result['date_time']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(incident_id, 'ECSO16CAD276495')
        self.assertEqual(latitude, 30.5108)
        self.assertEqual(longitude, -87.32834)
        self.assertEqual(date_time, '08/18/2016 7:50:00PM')
        self.assertEqual(incident, 'SUSP VEH')
        self.assertEqual(created_at, 1471567800.0)

    def test_get_text(self):
        soup = BeautifulSoup(self.data['value'], 'html.parser')
        original = soup.get_text()
        text = self.scraper.get_text(soup)

        self.assertEqual(original, 'ECSO16CAD276495')
        self.assertEqual(text, 'ECSO16CAD276495')

    def test_get_incident_id(self):
        soup = BeautifulSoup(self.data['incident_id'], 'html.parser')
        incident_id = self.scraper.get_incident_id(soup)

        self.assertEqual(incident_id, 'ECSO16CAD276495')

    def test_get_date_time(self):
        soup = BeautifulSoup(self.data['dates'], 'html.parser')
        incident_id = self.scraper.get_date_time(soup)

        self.assertEqual(incident_id, '08/18/2016 7:50:00PM')

    def test_get_description(self):
        soup = BeautifulSoup(self.data['description'], 'html.parser')
        incident_id = self.scraper.get_description(soup)

        self.assertEqual(incident_id, 'SUSP VEH')
