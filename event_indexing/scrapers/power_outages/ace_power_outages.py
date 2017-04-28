from urlparse import urljoin

from event_indexing.scrapers.power_outages.base_ifsc_scraper import IFSCScraper, MINIMUM_CUSTOMERS_AFFECTED
from event_indexing.scrapers.power_outages.ifsc_util import decode_line
from event_indexing.util.time_utils import now_milliseconds, get_tz_now

INVALID_INCIDENTS = {'Planned Maintenance', }
VALID_INCIDENTS = {'Under Evaluation', 'Unknown'}

DIRECTORY_API_ROUTE = '/data/interval_generation_data/metadata.xml'
SERVICE_AREAS_BOUNDS = {
    'southwest': {
        'latitude': 38.77,
        'longitude': -75.72
    },
    'northeast': {
        'latitude': 40.04,
        'longitude': -73.99
    }
}
API_ROUTE = '/data/interval_generation_data/{}/outages/{}.js'


class ACEPowerOutages(IFSCScraper):
    name = 'ACEPowerOutages'
    use_proxy = False
    tz_name = 'US/Eastern'

    def get_params(self, **kwargs):
        now = now_milliseconds()

        return {
            'timestamp': now
        }

    def get_provider(self, **kwargs):
        directory = kwargs.get('directory')
        index = kwargs.get('index')

        provider = {
            'id': 'ace_power_outages',
            'name': 'Atlantic City Electric',
            'api_host': 'stormcenter.atlanticcityelectric.com.s3.amazonaws.com',
            'url': 'http://www.atlanticcityelectric.com/pages/connectwithus/outages/outagemaps.aspx',
        }

        if directory is None or index is None:
            return provider

        provider['api_route'] = self.get_api_route(directory, index)

        return provider

    def get_directory_url(self):
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, DIRECTORY_API_ROUTE)

    def get_service_areas_bounds(self):
        return SERVICE_AREAS_BOUNDS

    def get_api_route(self, directory, index):
        return API_ROUTE.format(directory, index)

    def get_incident(self, raw_incident, **kwargs):
        description = raw_incident['desc'][-1]
        incident = self.get_description(description)
        latitude, longitude = self.get_coordinates(raw_incident)

        date_time = self.get_date_time(description)
        created_at = self.get_created_at(date_time)

        incident_id = self.get_incident_id([created_at, incident, latitude, longitude])

        return {
            'id': incident_id,
            'incident': incident,
            'latitude': latitude,
            'longitude': longitude,
            'created_at': created_at,
        }

    def get_coordinates(self, raw_incident):
        geometry = raw_incident['geom']
        coordinate = None

        for geo in geometry:
            if isinstance(geo, dict):
                for key in geo.keys():
                    if key == 'p':
                        coordinate = geo[key]
                        break

        if not coordinate:
            return None, None

        coordinate = decode_line(coordinate)
        coordinate = coordinate[0]

        latitude = coordinate[0]
        longitude = coordinate[1]

        return latitude, longitude

    def get_description(self, description):
        incident = description.get('cause', 'Power Outage')

        if any(token.lower() in incident.lower() for token in VALID_INCIDENTS):
            incident = 'Power Outage'

        return incident

    def get_customers_affected(self, description):
        customers = description['cust_a']
        customers = customers.replace('Less than', '')

        return int(customers.strip())

    def get_date_time(self, description):
        date_time = description.get('start')
        tz_info = self.get_tz_info()

        # Find now for given timezone (to get current year)
        tz_now = get_tz_now(tz_info)

        # Now year in tz
        year = ' {}'.format(tz_now.year)

        return date_time.replace(',', year)

    def is_valid_incident(self, raw_incident):
        description = raw_incident['desc'][-1]
        cluster = description.get('n_out')

        if cluster:
            return False

        latitude, longitude = self.get_coordinates(raw_incident)

        if latitude is None or longitude is None:
            return False

        incident = description.get('cause')
        affected = self.get_customers_affected(description)

        if incident and any(token.lower() in incident.lower() for token in INVALID_INCIDENTS):
            return False

        return affected >= MINIMUM_CUSTOMERS_AFFECTED
