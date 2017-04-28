import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from event_indexing.scrapers.base import IncidentScraper


class IncidentJsonScraper(IncidentScraper):
    '''
    Default method is GET, you can change this on class to POST (or what ever you need)
    '''
    method = 'GET'

    def request(self):
        '''
        Returns JSON object.
        '''
        request_args = self.get_request_args()
        r = requests.request(**request_args)
        r.raise_for_status()
        return r.json()

    def get_request_args(self):
        '''
        Returns arguments for request, adds method.
        '''
        args = super(IncidentJsonScraper, self).get_request_args()

        args['method'] = self.method

        return args

    def get_content_type(self):
        '''
        Returns JSON request content type. Can override on class as needed.
        '''
        return 'application/json'
