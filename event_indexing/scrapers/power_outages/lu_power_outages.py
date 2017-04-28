from urlparse import urljoin

from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper
from event_indexing.util.time_utils import now_milliseconds

MINIMUM_CUSTOMERS_AFFECTED = 10


class LUPowerOutages(IncidentJsonScraper):
    name = 'LUPowerOutages'
    tz_name = 'US/Eastern'
    method = 'GET'

    def get_provider(self, **kwargs):
        return {
            'id': 'lu_power_outages',
            'name': 'LaFollette Utilities',
            'api_host': 'www.outageentry.com',
            'api_route': '/CustomerFacingAppJQM/ajaxShellOut.php',
            'url': 'https://www.outageentry.com/CustomerFacingAppJQM/outage.php?clientid=LAFOLLETTE'
        }

    def get_params(self, **kwargs):
        now = now_milliseconds()

        return {
            'target': 'device_markers',
            'action': 'get',
            'serviceIndex': 1,
            'url': '10.58.27.9',
            '': '3A80',
            '_': now
        }

    def get_url(self, **kwargs):
        '''
        url is different from api_host + api_route, format and use
        '''
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def is_valid_incident(self, incident):
        affected = int(incident['consumers_affected'])

        return affected >= MINIMUM_CUSTOMERS_AFFECTED

    def get_incidents(self, content, **kwargs):
        incidents = content.get('markers', [])

        return [self.get_incident(incident) for incident in incidents if self.is_valid_incident(incident)]

    def get_incident(self, raw_incident, **kwargs):
        incident = 'Power Outage'
        start_date = raw_incident['start_date']
        longitude = raw_incident['lon']
        latitude = raw_incident['lat']
        consumers_affected = raw_incident['consumers_affected']

        created_at = self.get_created_at(start_date)

        incident_id = self.get_incident_id([created_at, incident, latitude, longitude])

        return {
            'id': incident_id,
            'incident': incident,
            'created_at': created_at,
            'start_date': start_date,
            'longitude': longitude,
            'latitude': latitude,
            'consumers_affected': consumers_affected
        }
