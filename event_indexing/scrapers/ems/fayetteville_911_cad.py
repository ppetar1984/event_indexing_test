import datetime
from urlparse import urljoin

from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper
from event_indexing.scrapers.util import is_valid_coordinate
from event_indexing.util.time_utils import get_tz_now

CODES = {
    'CODE 1': 'ARMED, USE CAUTION',
    'CODE 2': 'DWI',
    'CODE 3': 'ABANDONED VEHICLE',
    'CODE 4': 'RECKLESS DRIVER',
    'CODE 5': 'OBSTRUCTION ON HIGHWAY',
    'CODE 6': 'ACCIDENT, NO PERSONAL INJURIES, ROAD NOT BLOCKED',
    'CODE 7': 'ACCIDENT, NO PERSONAL INJURIES, ROAD BLOCKED',
    'CODE 8': 'ACCIDENT, PERSONAL INJURIES, NO ROAD BLOCKED',
    'CODE 9': 'ACCIDENT, PERSONAL INJURIES, ROAD BLOCKED',
    'CODE 10': 'SEND AMBULANCE',
    'CODE 11': 'SEND WRECKER',
    'CODE 12': 'SEND AMBULANCE AND WRECKER'
}

INVALID_ADDRESSES = {'<UNKNOWN>', }


class Fayetteville911Cad(IncidentJsonScraper):
    name = 'Fayetteville911Cad'
    tz_name = 'US/Central'

    def get_provider(self, **kwargs):
        today = get_tz_now(self.get_tz_info())
        tomorrow = today + datetime.timedelta(days=1)
        yesterday = today - datetime.timedelta(days=1)

        tomorrow_string = '{}-{}-{}'.format(tomorrow.year, tomorrow.month, tomorrow.day)
        yesterday_string = '{}-{}-{}'.format(yesterday.year, yesterday.month, yesterday.day)

        api_route = '/DispatchLogs/json/getIncidents.cshtml/{}/{}'.format(yesterday_string, tomorrow_string)

        return {
            'id': 'fayetteville_911_cad',
            'name': 'Fayetteville 911',
            'api_host': 'gis.fayetteville-ar.gov',
            'api_route': api_route,
            'url': 'http://www.fayetteville-ar.gov/1333/Police-Fire-Dispatch-Logs'
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
        address = raw_incident['Address']
        latitude = raw_incident['lat']
        longitude = raw_incident['lon']

        if address in INVALID_ADDRESSES and not is_valid_coordinate(latitude, longitude):
            return False

        return True

    def get_incidents(self, content, **kwargs):
        return [self.get_incident(incident) for incident in content if self.is_valid_incident(incident)]

    def get_incident(self, raw_incident, **kwargs):
        incident = self.get_description(raw_incident)
        date_time = self.get_date_time(raw_incident)
        created_at = self.get_created_at(date_time)

        result = {
            'datetime': date_time,
            'incident': incident,
            'created_at': created_at,
        }

        latitude = raw_incident['lat']
        longitude = raw_incident['lon']
        address = raw_incident['Address']

        # If coordinate is valid, use, if not use address
        if is_valid_coordinate(latitude, longitude):
            result['latitude'] = latitude
            result['longitude'] = longitude
        else:
            result['address'] = address

        result['id'] = self.get_incident_id(
            [created_at, incident, result.get('latitude'), result.get('longitude'), result.get('address')])

        return result

    def get_description(self, raw_incident):
        '''
        Some calls use `CODE #`, convert to true meaning
        '''
        incident = raw_incident['CallType']

        return CODES.get(incident, incident)

    def get_date_time(self, raw_incident):
        '''
        date, time used to generate created_at is 2 separate fields, merge them
        '''
        date = raw_incident['DispatchTime']
        time = raw_incident['DispatchTime2']

        return '{} {}'.format(date, time)
