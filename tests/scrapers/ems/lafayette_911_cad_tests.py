import json
import unittest

from bs4 import BeautifulSoup
from mock import MagicMock, patch, Mock

from event_indexing.scrapers.ems.lafayette_911_cad import Lafayette911Cad
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.ems import get_data_path


class Lafayette911CadTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('lafayette_911_cad.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = Lafayette911Cad(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'Lafayette911Cad')

    def test_tz_name(self):
        self.assertEqual(self.scraper.tz_name, 'US/Central')

    def test_method(self):
        self.assertEqual(self.scraper.method, 'POST')

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 3)

        self.assertEqual(url, 'http://apps.lafayettela.gov/L911/Service2.svc/getTrafficIncidents')
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
        incident = result['incident']
        created_at = result['created_at']
        address = result['address']

        self.assertEqual(created_at, 1471567800)
        self.assertEqual(incident_id, 'd542355646cfea03a333610bf98a4668')
        self.assertEqual(incident, 'Incoming Call')
        self.assertEqual(address, 'PATRICIA ST & ARNOULD BL LAFAYETTE,LA')

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
        address = alert['address']

        self.assertEqual(len(alert_keys), 6)

        self.assertEqual(detected_at, 1471567800.0)
        self.assertEqual(incident, 'Incoming Call')
        self.assertEqual(category, 'crime')
        self.assertEqual(prediction, 1.0)
        self.assertEqual(address, 'PATRICIA ST & ARNOULD BL LAFAYETTE,LA')

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

        self.assertEqual(len(source_keys), 5)
        self.assertEqual(len(provider_keys), 5)

        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(id, '7f16a7e049b034dfb4ac6b996e706a20')
        self.assertEqual(text, 'Incoming Call')
        self.assertEqual(source_type, TYPE_CAD_API)

        self.assertEqual(provider_api_host, 'www.lafayette911.org')
        self.assertEqual(provider_api_route, '/default.aspx')
        self.assertEqual(provider_api_name, 'Lafayette 911')
        self.assertEqual(provider_api_id, 'lafayette_911_cad')
        self.assertEqual(provider_api_url, 'http://apps.lafayettela.gov/L911/Service2.svc/getTrafficIncidents')

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

    def test_get_incident(self):
        result = BeautifulSoup(self.data['raw_incident'], "html.parser")
        result = self.scraper.get_incident(result)

        address = result['address']
        incident_id = result['id']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(address, 'JOHNSTON ST & WOODVALE AV LAFAYETTE,LA')
        self.assertEqual(incident_id, 'd8fe85eba7b5fc02a0e3e9772164d654')
        self.assertEqual(incident, 'Vehicle Accident')
        self.assertEqual(created_at, 1471567800.0)

    def test_get_text(self):
        soup = BeautifulSoup(self.data['value'], 'html.parser')

        text = self.scraper.get_text(soup)

        self.assertEqual(text, '08/18/2016 - 7:50 PM')
