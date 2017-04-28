import json
import re
from urlparse import urljoin

from event_indexing.scrapers.base_dom_scraper import IncidentDomScraper


class EscambiaCountySOCad(IncidentDomScraper):
    name = 'EscambiaCountySOCad'
    tz_name = 'US/Central'

    def get_url(self, **kwargs):
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def get_provider(self, **kwargs):
        return {
            'id': 'escambia_county_so_cad',
            'name': 'Escambia County Sheriff\'s Office',
            'api_host': 'www.escambiaso.com',
            'api_route': '/SmartWebClient/cadview.aspx',
            'url': 'http://www.escambiaso.com/index.php/crime-prevention/dispatched-calls/'
        }

    def get_incidents(self, content, **kwargs):
        pattern = re.compile(r'new\sArray\((?P<incidents>.+)\)\;', re.IGNORECASE)
        match = pattern.search(content)
        components = match.groupdict()

        raw_incidents = json.loads(components['incidents'])

        return [self.get_incident(raw_incident) for raw_incident in raw_incidents]

    def get_incident(self, raw_incident, **kwargs):
        latitude = raw_incident['Latitude']
        longitude = raw_incident['Longitude']
        window_info = raw_incident['WindowInfo']

        soup = self.get_soup(window_info)
        items = soup.find_all('tr')

        incident_id = items[0]
        date_time = items[1]
        incident = items[4]

        incident_id = self.get_incident_id(incident_id)
        date_time = self.get_date_time(date_time)
        incident = self.get_description(incident)

        created_at = self.get_created_at(date_time)

        return {
            'id': incident_id,
            'latitude': latitude,
            'longitude': longitude,
            'date_time': date_time,
            'incident': incident,
            'created_at': created_at,
        }

    def get_incident_id(self, item):
        items = item.find_all('td')

        return self.get_text(items[1])

    def get_date_time(self, item):
        date_time = self.get_text(item)
        pattern = re.compile(r'Received\:(?P<date_time>.+)Dispatched\:', re.IGNORECASE)
        match = pattern.search(date_time)
        components = match.groupdict()
        date_time = components['date_time']

        splits = date_time.split(' ')
        meridiem = splits.pop()
        date_time = ' '.join(splits)

        return '{}{}'.format(date_time, meridiem)

    def get_description(self, item):
        items = item.find_all('td')

        return self.get_text(items[1])
