import json
import unittest

from bs4 import BeautifulSoup
from mock import MagicMock, patch, Mock

from event_indexing.scrapers.ems.clark_county_fd_cad import ClarkCountyFDCad
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.ems import get_data_path


class ClarkCountyFDCadTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('clark_county_fd_cad.html')) as f:
            html = f.read()

        with open(get_data_path('clark_county_fd_cad.json')) as f:
            data = json.load(f)

        self.html = html
        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = ClarkCountyFDCad(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'ClarkCountyFDCad')

    def test_tz_name(self):
        self.assertEqual(self.scraper.tz_name, 'US/Pacific')

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 2)

        self.assertEqual(url, 'http://fire.co.clark.nv.us/Alarm%20OfficeConverted.aspx')
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
        date_time = result['datetime']
        address = result['address']
        incident = result['incident']

        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(date_time, 'Aug 18 2016 5:50PM')
        self.assertEqual(address, '4505 S Maryland Pky, Clark County 89119')
        self.assertEqual(incident, 'Medical Aid - C Level')

    @patch('time.time', return_value=1471568199)
    def test_scrape_delay(self, mock_time):
        size = sum(1 for i in self.scraper.scrape(self.html))

        self.assertEqual(size, 1)

    def test_parse_alert(self):
        alert = self.scraper.parse(self.data['incident'])

        alert_keys = alert.keys()

        detected_at = alert['detected_at']
        address = alert['address']
        incident = alert['incident']
        category = alert['category']
        prediction = alert['prediction']

        self.assertEqual(len(alert_keys), 6)

        self.assertEqual(detected_at, 1471567800.0)
        self.assertEqual(address, '4505 S Maryland Pky, Clark County 89119')
        self.assertEqual(incident, 'Medical Aid - C Level')
        self.assertEqual(category, 'medical')
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

        provider_api_host = source['provider']['api_host']
        provider_api_route = source['provider']['api_route']
        provider_api_name = source['provider']['name']
        provider_api_url = source['provider']['url']
        provider_api_id = source['provider']['id']

        self.assertEqual(len(source_keys), 5)
        self.assertEqual(len(provider_keys), 5)

        self.assertEqual(created_at, 1471567800.0)
        self.assertEqual(incident_id, '52d35ba3d39c1ee252427252373c26c4')
        self.assertEqual(text, 'Medical Aid - C Level')
        self.assertEqual(source_type, TYPE_CAD_API)

        self.assertEqual(provider_api_host, 'fire.co.clark.nv.us')
        self.assertEqual(provider_api_route, '/Alarm%20OfficeConverted.aspx')
        self.assertEqual(provider_api_name, 'Clark County Fire Department')
        self.assertEqual(provider_api_id, 'clark_county_fd_cad')
        self.assertEqual(provider_api_url, 'http://fire.co.clark.nv.us/Alarm%20OfficeConverted.aspx')

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

        self.assertEqual(size, 8)

    def test_get_incident(self):
        soup = BeautifulSoup(self.data['row'], 'html.parser')
        result = self.scraper.get_incident(soup)

        address = result['address']
        date_time = result['datetime']
        incident_id = result['id']
        incident = result['incident']
        created_at = result['created_at']

        self.assertEqual(address, '4505 S Maryland Pky, Clark County 89119')
        self.assertEqual(date_time, 'Aug 18 2016 5:50PM')
        self.assertEqual(incident_id, '52d35ba3d39c1ee252427252373c26c4')
        self.assertEqual(incident, 'Medical Aid - C Level')
        self.assertEqual(created_at, 1471567800.0)

    def test_get_text(self):
        soup = BeautifulSoup(self.data['value'], 'html.parser')
        original = soup.get_text()
        text = self.scraper.get_text(soup)

        self.assertEqual(original, 'Medical Aid - C Level')
        self.assertEqual(text, 'Medical Aid - C Level')

    def test_get_incident_id(self):
        created_at = 1471567800.0
        incident = 'Medical Aid - C Level'
        address = '4505 S Maryland Pky, Clark County 89119'
        incident_id = self.scraper.get_incident_id([created_at, incident, address])

        self.assertEqual(incident_id, '52d35ba3d39c1ee252427252373c26c4')
