import json
import unittest

from mock import MagicMock, patch

from event_indexing.scrapers.power_outages.base_ifsc_service_area import IFSCServiceAreas
from tests.scrapers.power_outages import get_data_path


class IFSCServiceAreasURLTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('base_ifsc_service_areas.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        url = 'http://outagemap.aepohio.com.s3.amazonaws.com/resources/datastatic/serviceareas.json'
        self.scraper = IFSCServiceAreas(self.relay_host_api, self.relay_auth, self.proxy_host, url, None)

    def test_get_request_args(self):
        request_args = self.scraper.get_request_args()

        size = len(request_args.keys())

        url = request_args.get('url')
        verify = request_args.get('verify')
        headers = request_args.get('headers')
        params = request_args.get('params')
        data = request_args.get('data')

        self.assertEqual(size, 4)

        self.assertEqual(url,
                         'http://outagemap.aepohio.com.s3.amazonaws.com/resources/datastatic/serviceareas.json')
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
        result = next(self.scraper.scrape(self.data['response']))

        size = len(result)

        coordinate = result[0]

        latitude = coordinate[0]
        longitude = coordinate[1]

        self.assertEqual(size, 13)

        self.assertEqual(latitude, 40.19625)
        self.assertEqual(longitude, -84.80616)

    def test_get_provider(self):
        provider = self.scraper.get_provider()

        self.assertIsNone(provider)

    @patch('event_indexing.scrapers.power_outages.base_ifsc_service_area.IFSCServiceAreas.request')
    def test_segments(self, mock_request):
        mock_request.return_value = self.data['response']
        segments = self.scraper.segments
        segment = segments[0]

        self.assertEqual(len(segments), 9)

        northeast = segment['northeast']
        southwest = segment['southwest']

        ne_latitude = northeast['latitude']
        ne_longitude = northeast['longitude']

        sw_latitude = southwest['latitude']
        sw_longitude = southwest['longitude']

        self.assertEqual(ne_latitude, 40.31013)
        self.assertEqual(ne_longitude, -84.70339)

        self.assertEqual(sw_latitude, 40.247303333333335)
        self.assertEqual(sw_longitude, -84.73834)


class IFSCServiceAreasBoundsTest(unittest.TestCase):
    def setUp(self):
        with open(get_data_path('base_ifsc_service_areas.json')) as f:
            data = json.load(f)

        self.data = data

        self.relay_host_api = 'https://prod-classifier-event_indexing-01.bmasked.info/v1/relay/'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://prodinsta-proxy-farm-main.ibrdns.com'

        bounds = self.data['bounds']
        self.scraper = IFSCServiceAreas(self.relay_host_api, self.relay_auth, self.proxy_host, None, bounds)

    def test_segments(self):
        segments = self.scraper.segments
        segment = segments[0]

        self.assertEqual(len(segments), 9)

        northeast = segment['northeast']
        southwest = segment['southwest']

        ne_latitude = northeast['latitude']
        ne_longitude = northeast['longitude']

        sw_latitude = southwest['latitude']
        sw_longitude = southwest['longitude']

        self.assertEqual(ne_latitude, 40.04)
        self.assertEqual(ne_longitude, -73.99)

        self.assertEqual(sw_latitude, 39.61666666666667)
        self.assertEqual(sw_longitude, -74.56666666666666)
