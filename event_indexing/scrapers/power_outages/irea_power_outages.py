from datetime import timedelta
from urlparse import urljoin

from event_indexing.scrapers.base_dom_scraper import IncidentDomScraper
from event_indexing.util.time_utils import get_tz_now

MINIMUM_CUSTOMERS_AFFECTED = 10


class IREAPowerOutages(IncidentDomScraper):
    name = 'IREAPowerOutages'
    tz_name = 'US/Mountain'

    def get_provider(self, **kwargs):
        return {
            'id': 'irea_power_outages',
            'name': 'Intermountain Rural Electric Association',
            'api_host': 'irea.maps.sienatech.com',
            'api_route': '/data/outages.xml',
            'url': 'http://irea.maps.sienatech.com/'
        }

    def get_url(self, **kwargs):
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def get_incidents(self, content, **kwargs):
        soup = self.get_soup(content)
        incidents = soup.findAll('outage')

        return [self.get_incident(raw_incident) for raw_incident in incidents if self.is_valid_incident(raw_incident)]

    def is_valid_incident(self, raw_incident):
        affected = int(raw_incident['affected'])

        return affected >= MINIMUM_CUSTOMERS_AFFECTED

    def get_incident(self, raw_incident, **kwargs):
        incident = 'Power Outage'
        incident_id = raw_incident['id']
        latitude = float(raw_incident['lat'])
        longitude = float(raw_incident['lng'])
        date_time = self.get_date_time(raw_incident)
        created_at = self.get_created_at(date_time)

        return {
            'incident': incident,
            'id': incident_id,
            'latitude': latitude,
            'longitude': longitude,
            'created_at': created_at
        }

    def get_date_time(self, raw_incident):
        duration = int(raw_incident['duration'])
        now = get_tz_now(self.get_tz_info())

        date_time = now - timedelta(seconds=duration)
        date_time = date_time.strftime('%Y-%m-%d %H:%M:%S')

        return date_time
