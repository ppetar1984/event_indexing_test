import json
import unittest

from mock import patch, MagicMock

from event_indexing.scrapers.power_outages.base_ifsc_directory import IFSCDirectory
from tests.scrapers.power_outages import get_data_path


class IFSCDirectoryJSONTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('base_ifsc_directory.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        url = 'http://outagemap.aepohio.com.s3.amazonaws.com/resources/data/external/interval_generation_data/metadata.json'
        self.scraper = IFSCDirectory(self.relay_host_api, self.relay_auth, self.proxy_host, url)

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 3)

        self.assertEqual(url,
                         'http://outagemap.aepohio.com.s3.amazonaws.com/resources/data/external/interval_generation_data/metadata.json')
        self.assertFalse(verify)
        self.assertIsNotNone(headers)
        self.assertIsNotNone(params)
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

    @patch('time.time', return_value=1471568199)
    def test_get_params(self, mock_time):
        params = self.scraper.get_params()

        now = params['_']

        self.assertEqual(now, 1471568199000)

    def test_get_data(self):
        data = self.scraper.get_data()

        self.assertIsNone(data)

    def test_scrape(self):
        text = json.dumps(self.data['response'])
        directory = self.scraper.scrape(text)

        self.assertEqual(directory, '2017_03_16_01_15_30')

    def test_get_provider(self):
        provider = self.scraper.get_provider()

        self.assertIsNone(provider)

    @patch('event_indexing.scrapers.power_outages.base_ifsc_directory.IFSCDirectory.request')
    def test_directory(self, mock_request):
        text = json.dumps(self.data['response'])
        mock_request.return_value = text
        directory = self.scraper.directory

        self.assertEqual(directory, '2017_03_16_01_15_30')

    def test_is_json_response(self):
        is_json_request = self.scraper.is_json_response()

        self.assertTrue(is_json_request)


class IFSCDirectoryXMLTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('base_ifsc_directory.xml')) as f:
            html = f.read()

        self.html = html

        self.relay_host_api = 'https://prod-classifier-event_indexing-01.bmasked.info/v1/relay/'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://prodinsta-proxy-farm-main.ibrdns.com'

        url = 'http://stormcenter.atlanticcityelectric.com.s3.amazonaws.com/data/interval_generation_data/metadata.xml'
        self.scraper = IFSCDirectory(self.relay_host_api, self.relay_auth, self.proxy_host, url)

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 3)

        self.assertEqual(url,
                         'http://stormcenter.atlanticcityelectric.com.s3.amazonaws.com/data/interval_generation_data/metadata.xml')
        self.assertFalse(verify)
        self.assertIsNotNone(headers)
        self.assertIsNotNone(params)
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
        self.assertEqual(content_type, 'application/xml')
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

    def test_scrape(self):
        directory = self.scraper.scrape(self.html)

        self.assertEqual(directory, '2017_03_16_01_15_30')

    def test_get_provider(self):
        provider = self.scraper.get_provider()

        self.assertIsNone(provider)

    @patch('event_indexing.scrapers.power_outages.base_ifsc_directory.IFSCDirectory.request')
    def test_directory(self, mock_request):
        mock_request.return_value = self.html
        directory = self.scraper.directory

        self.assertEqual(directory, '2017_03_16_01_15_30')

    def test_is_json_response(self):
        is_json_request = self.scraper.is_json_response()

        self.assertFalse(is_json_request)
