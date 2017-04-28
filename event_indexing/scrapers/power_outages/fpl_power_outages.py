from urlparse import urljoin

from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper

MINIMUM_CUSTOMERS_AFFECTED = 10


class FPLPowerOutages(IncidentJsonScraper):
    name = 'FPLPowerOutages'
    tz_name = 'US/Eastern'

    def get_provider(self, **kwargs):
        return {
            'id': 'fpl_power_outages',
            'name': 'Florida Power and Light',
            'api_host': 'www.fplmaps.com',
            'api_route': '/customer/outage/StormFeedRestoration.json',
            'url': 'http://www.fplmaps.com/'
        }

    def get_url(self, **kwargs):
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def get_incidents(self, content, **kwargs):
        outages = content.get('outages', [])

        return [self.get_incident(raw_incident) for raw_incident in outages if self.is_valid_incident(raw_incident)]

    def is_valid_incident(self, raw_incident):
        affected = int(raw_incident['customersAffected'])

        return affected >= MINIMUM_CUSTOMERS_AFFECTED

    def get_incident(self, raw_incident, **kwargs):
        date_time = raw_incident['dateReported']
        created_at = self.get_created_at(date_time)
        latitude = float(raw_incident['lat'])
        longitude = float(raw_incident['lng'])

        incident = "Power Outage"
        incident_id = self.get_incident_id([created_at, latitude, longitude, incident])

        return {
            'id': incident_id,
            'incident': incident,
            'latitude': latitude,
            'longitude': longitude,
            'created_at': created_at,
        }
