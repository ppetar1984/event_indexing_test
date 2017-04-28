from urlparse import urljoin

from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper
from event_indexing.util.time_utils import now_milliseconds

MINIMUM_CUSTOMERS_AFFECTED = 10

class WOVScraper(IncidentJsonScraper):
    def get_params(self, **kwargs):
        now = now_milliseconds()

        return {
            'Start': '',
            'End': '',
            'Duration': '0',
            'CustomerResponsible': 'false',
            'Historical': 'false',
            '_': now
        }

    def get_url(self, **kwargs):
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def get_incidents(self, content, **kwargs):
        raw_incidents = content['Outages']

        return [self.get_incident(raw_incident) for raw_incident in raw_incidents if self.is_valid_incident(raw_incident)]

    def is_valid_incident(self, raw_incident):
        affected = raw_incident['CustomersOutNow']

        return affected >= MINIMUM_CUSTOMERS_AFFECTED

    def get_incident(self, raw_incident, **kwargs):
        incident = 'Power Outage'
        incident_id = raw_incident['OutageRecID']
        coordinates = raw_incident['OutageLocation']
        latitude = coordinates['Y']
        longitude = coordinates['X']
        date_time = raw_incident['OutageStartTime']
        created_at = self.get_created_at(date_time)

        return {
            'incident': incident,
            'id': incident_id,
            'created_at': created_at,
            'latitude': latitude,
            'longitude': longitude
        }