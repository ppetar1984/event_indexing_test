from urlparse import urljoin

from event_indexing.scrapers.power_outages.base_ifsc_scraper import IFSCScraper
from event_indexing.util.time_utils import now_milliseconds

DIRECTORY_API_ROUTE = '/resources/data/external/interval_generation_data/metadata.json'
SERVICE_AREAS_ROUTE = '/resources/datastatic/serviceareas.json'
API_ROUTE = '/resources/data/external/interval_generation_data/{}/outages/{}.json'


class APPowerOutages(IFSCScraper):
    name = 'APPowerOutages'
    use_proxy = False

    def get_params(self, **kwargs):
        now = now_milliseconds()

        return {
            '_': now
        }

    def get_provider(self, **kwargs):
        directory = kwargs.get('directory')
        index = kwargs.get('index')

        provider = {
            'id': 'ap_power_outages',
            'name': 'Appalachian American Electric Power Co',
            'api_host': 'outagemap.appalachianpower.com.s3.amazonaws.com',
            'url': 'http://outagemap.appalachianpower.com.s3.amazonaws.com/external/default.html',
        }

        if directory is None or index is None:
            return provider

        provider['api_route'] = self.get_api_route(directory, index)

        return provider

    def get_directory_url(self):
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, DIRECTORY_API_ROUTE)

    def get_service_areas_url(self):
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, SERVICE_AREAS_ROUTE)

    def get_api_route(self, directory, index):
        return API_ROUTE.format(directory, index)
