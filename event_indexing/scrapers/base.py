import hashlib
import json
import random
from urlparse import urljoin

import requests
from dateutil.tz import gettz
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from event_indexing.scrapers.category_maps import INCIDENT_CATEGORY_MAP
from event_indexing.source import TYPE_CAD_API
from event_indexing.util.time_utils import now_seconds, parse_timestamp

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

CHUNK_SIZE = 50
MAX_DELAY = 60 * 60  # 1 hour delay

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:32.0) Gecko/20100101 Firefox/32.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25"
]


class IncidentScraper(object):
    name = None
    tz_name = None
    use_proxy = False
    _category_map = None

    def __init__(self, relay_host_api, relay_auth, proxy_host):
        self._relay_host_api = relay_host_api
        self._relay_auth = relay_auth
        self.proxy_host = proxy_host

    def request(self):
        '''
        Loads a page and returns the raw text
        '''
        request_args = self.get_request_args()
        r = requests.get(**request_args)
        r.raise_for_status()
        return r.text

    def get_request_args(self):
        '''
        Creates all arguments needed to load page, you can override
        this if you need to add more args. Always call super
        if you override.
        '''
        url = self.get_url()

        args = {
            'url': url,
        }

        # Proxy
        if self.use_proxy:
            args['verify'] = False

        # Headers
        headers = self.get_headers()
        if headers:
            args['headers'] = headers

        # Params
        params = self.get_params()
        if params:
            args['params'] = params

        # Data
        data = self.get_data()
        if data:
            args['data'] = data

        return args

    def get_url(self, **kwargs):
        '''
        Builds the url based on if source is using proxy or not. Don't worry about using proxy,
        leave as is. Will need to override this for if url is not same as api_host and api_route.
        See clark_county_fd_cad.py for override example.
        '''
        provider = self.get_provider()

        if self.use_proxy:
            route = provider['api_route']

            return urljoin(self.proxy_host, route)

        return provider['url']

    def get_headers(self, **kwargs):
        '''
        Builds headers. You can override this if needed. Always call super and add on that object.
        '''
        provider = self.get_provider()
        user_agent = self.get_user_agent()

        headers = {
            'User-Agent': user_agent
        }

        # Content Type
        content_type = self.get_content_type()
        if content_type:
            headers['Content-Type'] = content_type

        if self.use_proxy:
            host = provider['api_host']

            headers['Host'] = host
            headers['X-Ramire-Purpose'] = 'General'

        return headers

    def get_params(self, **kwargs):
        '''
        Returns a dictionary of params (eg. {'sort': 'desc'})
        '''
        return None

    def get_data(self, **kwargs):
        '''
        Returns a dictionary of data, used with POST (eg. {'sort': 'desc'})
        '''
        return None

    def get_content_type(self):
        '''
        Overridden in BaseDomScraper and BaseJsonScraper. Override as needed (eg. 'application/x-www-form-urlencoded')
        '''
        return None

    def get_user_agent(self):
        '''
        Randomly selects a header. Override only if source isn't accepting header.
        '''
        return random.choice(USER_AGENTS)

    def get_provider(self, **kwargs):
        '''
        Override on every class. See clark_county_fd_cad.py
        '''
        raise NotImplementedError

    def get_max_delay(self):
        '''
        Limits indexing to last 1 hour. Do not override.
        '''
        return MAX_DELAY

    def scrape(self, content, **kwargs):
        '''
        Takes in content (text/json), sends to class through get_incidents
        and yields raw_incidents (formatted through get_incident, see
        clark_county_fd_cad.py for example). No need to override.
        '''
        now = now_seconds()
        max_delay = self.get_max_delay()
        incidents = self.get_incidents(content)

        for raw_incident in incidents:
            created_at = raw_incident['created_at']

            if now - created_at > max_delay or created_at > now:
                continue

            yield raw_incident

    def parse(self, raw_incident, **kwargs):
        '''
        Parses raw_incident into formatted object. No need to override.
        '''
        provider = self.get_provider()

        incident = raw_incident['incident']
        created_at = raw_incident['created_at']
        incident_id = raw_incident['id']

        alert = {
            'detected_at': created_at,
            'incident': incident,
            'source': {
                'provider': provider,
                'text': incident,
                'created_at': created_at,
                'type': TYPE_CAD_API,
                'id': incident_id,
            }
        }

        # Details
        details = raw_incident.get('details')
        if details:
            alert['details'] = details

        # Address
        address = raw_incident.get('address')
        if address:
            alert['address'] = address

        # City
        city = raw_incident.get('city')
        if city:
            alert['city'] = city

        # Locality
        locality = raw_incident.get('locality')
        if locality:
            alert['locality'] = locality

        # Coordinates
        latitude = raw_incident.get('latitude')
        longitude = raw_incident.get('longitude')
        if latitude and longitude:
            alert['coordinates'] = {
                'lat': latitude,
                'long': longitude
            }

            alert['source']['geo'] = {
                'type': 'Point',
                'coordinates': [
                    longitude,
                    latitude
                ]
            }

        # Category
        category = self.category_map.get(incident)
        if category:
            alert['category'] = category
            alert['prediction'] = 1.0

        return alert

    def run(self):
        '''
        Runs an indexing job.
        '''
        content = self.request()
        incidents = []

        try:
            for raw_incident in self.scrape(content):
                incidents.append(self.parse(raw_incident))
            self.publish(incidents)
        except:
            print 'Parser {}: Failed to index source'.format(self.name)

    def publish(self, incidents):
        '''
        Publishes formatted incidents (for this purpose, will only print incidents)
        '''
        for incident in incidents:
            print json.dumps(incident)

    def get_incidents(self, content, **kwargs):
        '''
        Override on every class, see clark_county_fd_cad.py for example
        '''
        raise NotImplementedError

    def get_incident(self, raw_incident, **kwargs):
        '''
        Override on every class, see clark_county_fd_cad.py for example
        '''
        raise NotImplementedError

    def get_incident_id(self, values):
        '''
        Generates unique ID for every incident if not provided, see clark_county_fd_cad.py for example
        '''
        values = [str(value) for value in values if value is not None]

        return hashlib.md5('-'.join(values)).hexdigest()

    def get_created_at(self, time_string):
        '''
        Converts a time string into a epoch timestamp. See clark_county_fd_cad.py for example
        '''
        return parse_timestamp(time_string, self.get_tz_info())

    def get_tz_info(self):
        '''
        Returns tzinfo object from string. See clark_county_fd_cad.py tz_name
        '''
        if self.tz_name:
            return gettz(self.tz_name)

        return None

    @property
    def category_map(self):
        '''
        Returns category map. No need to override.
        '''
        if self._category_map is None:
            self._category_map = INCIDENT_CATEGORY_MAP.get(self.name, {})
        return self._category_map


class ScraperException(Exception):
    pass
