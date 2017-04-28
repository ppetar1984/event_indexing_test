from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper
from event_indexing.scrapers.power_outages.ifsc_util import decode_line, merge_service_areas, get_service_area_segments, \
    get_bounds_segments
from event_indexing.util.time_utils import now_milliseconds

LATITUDE_SEGMENTS = 3
LONGITUDE_SEGMENTS = 3


class IFSCServiceAreas(IncidentJsonScraper):
    use_proxy = False
    url = None
    bounds = None
    _segments = None

    def __init__(self, relay_host_api, relay_auth, proxy_host, url, bounds):
        super(IFSCServiceAreas, self).__init__(relay_host_api, relay_auth, proxy_host)
        self.url = url
        self.bounds = bounds

    def get_provider(self, **kwargs):
        return None

    def get_params(self, **kwargs):
        now = now_milliseconds()

        return {
            '_': now
        }

    def get_url(self, **kwargs):
        return self.url

    def scrape(self, content, **kwargs):
        for service_area in content.get('file_data', []):
            geometry = service_area['geom']
            lines = geometry['l']

            yield decode_line(lines[0])

    @property
    def segments(self):
        '''
         Get all service areas, merge them, extract total area bounds, split bounds into a 3x3 grid
        '''
        if self._segments is None:
            if self.bounds is None:
                content = self.request()

                service_areas = []
                for service_area in self.scrape(content):
                    service_areas.append(service_area)

                merged_service_areas = merge_service_areas(service_areas)
                self._segments = get_service_area_segments(merged_service_areas, LATITUDE_SEGMENTS, LONGITUDE_SEGMENTS)
            else:
                self._segments = get_bounds_segments(self.bounds, LATITUDE_SEGMENTS, LONGITUDE_SEGMENTS)

        return self._segments
