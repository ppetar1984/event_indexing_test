from urlparse import urljoin

from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper
from event_indexing.scrapers.util import is_valid_coordinate
from event_indexing.util.time_utils import now_milliseconds

MINIMUM_CUSTOMERS_AFFECTED = 10


class CEAPowerOutages(IncidentJsonScraper):
    name = 'CEAPowerOutages'
    tz_name = 'US/Alaska'

    def get_provider(self, **kwargs):
        api_route = '/outage/Incidents.js?t={}'.format(now_milliseconds())

        return {
            'id': 'cea_power_outages',
            'name': 'Chugach Electric Association Inc',
            'api_host': 'www.chugachelectric.com',
            'api_route': api_route,
            'url': 'https://www.chugachelectric.com/outage/outage_map.html',
        }

    def get_url(self, **kwargs):
        '''
        url is different from api_host + api_route, format and use
        '''
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def is_valid_incident(self, raw_incident):
        '''
        Some incidents are not valid, for example, this source has `<UNKNOWN>` and invalid coordinates,
        reject before parsing
        '''
        affected = int(raw_incident['CUSTOMERS_AFFECTED'])
        latitude = float(raw_incident['LATITUDE'])
        longitude = float(raw_incident['LONGITUDE'])

        if not affected and not is_valid_coordinate(latitude, longitude):
            return False

        return True

    def get_incidents(self, content, **kwargs):
        self.content = content
        description = self.get_description(content)
        return [self.get_incident(incident) for incident in description if self.is_valid_incident(incident)]

    def get_incident(self, raw_incident, **kwargs):
        date_time = self.get_date_time(self.content)
        created_at = self.get_created_at(date_time)

        result = {
            'datetime': date_time,
            'incident': "Power Outage",
            'created_at': created_at,
        }

        affected = raw_incident['CUSTOMERS_AFFECTED']
        outage_start = raw_incident['OUTAGE_START']
        latitude = float(raw_incident['LATITUDE'])
        longitude = float(raw_incident['LONGITUDE'])

        # If coordinate is valid, use, if not use address
        if is_valid_coordinate(latitude, longitude):
            result['latitude'] = latitude
            result['longitude'] = longitude

        result['affected'] = affected
        result['outage_start'] = outage_start

        result['id'] = self.get_incident_id(
            [created_at, result.get('outage_start'), result.get('latitude'), result.get('longitude')])

        return result

    def get_description(self, raw_incident):
        '''
        Some calls use `CODE #`, convert to true meaning
        '''
        incident = raw_incident['Items']

        return incident

    def get_date_time(self, raw_incident):
        '''
        date, time used to generate created_at is 2 separate fields, merge them
        '''
        date = raw_incident['Datetime']

        return '{}'.format(date)


if __name__ == '__main__':
    scraper = CEAPowerOutages(None, None, None)
    scraper.run()
