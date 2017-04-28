import json
import unittest
from datetime import datetime

from mock import MagicMock, patch, Mock

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from event_indexing.scrapers.power_outages.cec_power_outages import CECPowerOutages
from event_indexing.source import TYPE_CAD_API
from tests.scrapers.power_outages import get_data_path


class CECPowerOutagesTest(unittest.TestCase):
    def setUp(self):
        # with open(get_data_path('cec_power_outages.xml')) as f:
        #     html = f.read()

        with open(get_data_path('cea_power_outages.json')) as f:
            data = json.load(f)

        # self.html = html
        self.data = data

        self.relay_host_api = 'https://relay.host'
        self.relay_auth = ('test', 'test')
        self.proxy_host = 'https://proxy.host'

        self.scraper = CECPowerOutages(self.relay_host_api, self.relay_auth, self.proxy_host)

    def test_name(self):
        self.assertEqual(self.scraper.name, 'CECPowerOutages')

    def test_tz_name(self):
        self.assertEqual(self.scraper.tz_name, 'US/Eastern')

    # def test_get_request_args(self):
    #     request_args = self.scraper.get_request_args()
    #
    #     size = len(request_args.keys())
    #
    #     url = request_args.get('url')
    #     verify = request_args.get('verify')
    #     headers = request_args.get('headers')
    #     params = request_args.get('params')
    #     data = request_args.get('data')
    #
    #     self.assertEqual(size, 2)
    #
    #     self.assertEqual(url, 'http://choptank.maps.sienatech.com/data/outages.xml')
    #     self.assertFalse(verify)
    #     self.assertIsNotNone(headers)
    #     self.assertIsNone(params)
    #     self.assertIsNone(data)
    #
    # def test_get_headers(self):
    #     self.scraper.get_user_agent = MagicMock(return_value='test')
    #     headers = self.scraper.get_headers()
    #
    #     size = len(headers.keys())
    #
    #     user_agent = headers.get('User-Agent')
    #     content_type = headers.get('Content-Type')
    #     host = headers.get('Host')
    #     ramire_purpose = headers.get('X-Ramire-Purpose')
    #
    #     self.assertEqual(size, 2)
    #
    #     self.assertEqual(user_agent, 'test')
    #     self.assertEqual(content_type, 'text/html; charset=utf-8')
    #     self.assertIsNone(host)
    #     self.assertIsNone(ramire_purpose)
    #
    # @patch('time.time', return_value=1471568199)
    # def test_get_params(self, mock_time):
    #     params = self.scraper.get_params()
    #
    #     self.assertIsNone(params)
    #
    # def test_get_data(self):
    #     data = self.scraper.get_data()
    #
    #     self.assertIsNone(data)
    #
    # def test_get_max_delay(self):
    #     max_delay = self.scraper.get_max_delay()
    #
    #     self.assertEqual(max_delay, 3600)
    #
    # @patch('event_indexing.scrapers.power_outages.cec_power_outages.get_tz_now')
    # @patch('time.time', return_value=1471568199)
    # def test_scrape(self, mock_time, mock_get_tz_now):
    #     mock_get_tz_now.return_value = datetime(2016, 8, 18, 19, 00, 00, tzinfo=self.scraper.get_tz_info())
    #     result = next(self.scraper.scrape(self.html))
    #
    #     created_at = result['created_at']
    #     incident = result['incident']
    #     incident_id = result['id']
    #     latitude = result['latitude']
    #     longitude = result['longitude']
    #
    #     self.assertEqual(created_at, 1471567800.0)
    #     self.assertEqual(incident, 'Power Outage')
    #     self.assertEqual(incident_id, '354737115')
    #     self.assertEqual(latitude, 39.3804851523661)
    #     self.assertEqual(longitude, -104.875128646716)
    #
    # @patch('event_indexing.scrapers.power_outages.cec_power_outages.get_tz_now')
    # @patch('time.time', return_value=1471568199)
    # def test_scrape_delay(self, mock_time, mock_get_tz_now):
    #     mock_get_tz_now.return_value = datetime(2016, 8, 18, 19, 00, 00, tzinfo=self.scraper.get_tz_info())
    #     size = sum(1 for i in self.scraper.scrape(self.html))
    #
    #     self.assertEqual(size, 1)
    #
    # def test_parse_alert(self):
    #     alert = self.scraper.parse(self.data['incident'])
    #
    #     alert_keys = alert.keys()
    #
    #     detected_at = alert['detected_at']
    #     incident = alert['incident']
    #     latitude = alert['coordinates']['lat']
    #     longitude = alert['coordinates']['long']
    #     category = alert['category']
    #     prediction = alert['prediction']
    #
    #     self.assertEqual(len(alert_keys), 6)
    #
    #     self.assertEqual(detected_at, 1471567800.0)
    #     self.assertEqual(incident, 'Power Outage')
    #     self.assertEqual(latitude, 39.3804851523661)
    #     self.assertEqual(longitude, -104.875128646716)
    #     self.assertEqual(category, 'power_outage')
    #     self.assertEqual(prediction, 1.0)
    #
    # def test_parse_alert_source(self):
    #     alert = self.scraper.parse(self.data['incident'])
    #     source = alert['source']
    #
    #     source_keys = source.keys()
    #     provider_keys = source['provider'].keys()
    #
    #     created_at = source['created_at']
    #     id = source['id']
    #     text = source['text']
    #     source_type = source['type']
    #     coordinates = source['geo']['coordinates']
    #     geo_type = source['geo']['type']
    #
    #     provider_api_host = source['provider']['api_host']
    #     provider_api_route = source['provider']['api_route']
    #     provider_api_name = source['provider']['name']
    #     provider_api_url = source['provider']['url']
    #     provider_api_id = source['provider']['id']
    #
    #     self.assertEqual(len(source_keys), 6)
    #     self.assertEqual(len(provider_keys), 5)
    #
    #     self.assertEqual(created_at, 1471567800.0)
    #     self.assertEqual(id, '354737115')
    #     self.assertEqual(text, 'Power Outage')
    #     self.assertEqual(source_type, TYPE_CAD_API)
    #     self.assertEqual(coordinates, [-104.875128646716, 39.3804851523661])
    #     self.assertEqual(geo_type, 'Point')
    #
    #     self.assertEqual(provider_api_host, 'irea.maps.sienatech.com')
    #     self.assertEqual(provider_api_route, '/data/outages.xml')
    #     self.assertEqual(provider_api_name, 'Intermountain Rural Electric Association')
    #     self.assertEqual(provider_api_id, 'irea_power_outages')
    #     self.assertEqual(provider_api_url, 'http://irea.maps.sienatech.com/')
    #
    # @patch('event_indexing.scrapers.power_outages.cec_power_outages.get_tz_now')
    # @patch('time.time', return_value=1471568199)
    # def test_run(self, mock_time, mock_get_tz_now):
    #     mock_get_tz_now.return_value = datetime(2016, 8, 18, 19, 00, 00, tzinfo=self.scraper.get_tz_info())
    #     self.scraper.publish = Mock()
    #     self.scraper.request = MagicMock(return_value=self.html)
    #     self.scraper.run()
    #
    #     self.scraper.publish.assert_called_once_with(self.data['incidents'])
    #
    # @patch('requests.post', return_value=MagicMock(autospec=True))
    # def test_publish_no_incidents(self, post_mock):
    #     self.scraper.publish(incidents=[])
    #
    #     post_mock.assert_not_called()
    #
    # def test_get_incidents(self):
    #     incidents = self.scraper.get_incidents(self.html)
    #
    #     size = len(incidents)
    #
    #     self.assertEqual(size, 1)
    #
    # @patch('event_indexing.scrapers.power_outages.cec_power_outages.get_tz_now')
    # @patch('time.time', return_value=1471568199)
    # def test_get_incident(self, mock_time, mock_get_tz_now):
    #     mock_get_tz_now.return_value = datetime(2016, 8, 18, 19, 00, 00, tzinfo=self.scraper.get_tz_info())
    #     soup = self.scraper.get_soup(self.data['raw_incident'])
    #     incident = soup.find('outage')
    #     result = self.scraper.get_incident(incident)
    #
    #     latitude = result['latitude']
    #     longitude = result['longitude']
    #     incident_id = result['id']
    #     incident = result['incident']
    #     created_at = result['created_at']
    #
    #     self.assertEqual(latitude, 39.3804851523661)
    #     self.assertEqual(longitude, -104.875128646716)
    #     self.assertEqual(incident_id, '354737115')
    #     self.assertEqual(incident, 'Power Outage')
    #     self.assertEqual(created_at, 1471567800.0)
    #
    # def test_is_valid_incident(self):
    #     soup = self.scraper.get_soup(self.html)
    #     raw_incidents = soup.findAll("outage")
    #
    #     valid_raw_incident = raw_incidents[0]
    #     invalid_raw_incident = raw_incidents[1]
    #
    #     is_valid_affected = self.scraper.is_valid_incident(valid_raw_incident)
    #     is_invalid_affected = self.scraper.is_valid_incident(invalid_raw_incident)
    #
    #     self.assertTrue(is_valid_affected)
    #     self.assertFalse(is_invalid_affected)
    #
    # @patch('event_indexing.scrapers.power_outages.cec_power_outages.get_tz_now')
    # def test_get_date_time(self, mock_get_tz_now):
    #     mock_get_tz_now.return_value = datetime(2016, 8, 18, 19, 00, 00, tzinfo=self.scraper.get_tz_info())
    #
    #     soup = self.scraper.get_soup(self.html)
    #     raw_incident = soup.find("outage")
    #
    #     date_time = self.scraper.get_date_time(raw_incident)
    #     created_at = self.scraper.get_created_at(date_time)
    #
    #     self.assertEqual(created_at, 1471567800.0)


if __name__ == '__main__':
    unittest.main()
    # scraper = CECPowerOutagesTest(None, None, None)
    # scraper.run()

# scraper = CECPowerOutagesTest(None, None, None)
# scraper.
