import json

from bs4 import BeautifulSoup

from event_indexing.scrapers.base import IncidentScraper
from event_indexing.util.time_utils import now_milliseconds


class IFSCDirectory(IncidentScraper):
    use_proxy = False
    url = None
    _directory = None

    def __init__(self, relay_host_api, relay_auth, proxy_host, url):
        super(IFSCDirectory, self).__init__(relay_host_api, relay_auth, proxy_host)
        self.url = url

    def get_provider(self, **kwargs):
        return None

    def get_url(self, **kwargs):
        return self.url

    def get_params(self, **kwargs):
        key = '_' if self.is_json_response() else 'timestamp'
        now = now_milliseconds()

        return {
            key: now
        }

    def get_content_type(self):
        return 'application/json' if self.is_json_response() else 'application/xml'

    def is_json_response(self):
        return 'json' in self.url

    def scrape(self, content, **kwargs):
        # JSON response
        if self.is_json_response():
            content = json.loads(content)

            return content['directory']

        # XML response
        soup = BeautifulSoup(content, 'html.parser')
        directory = soup.find('directory')

        return directory.get_text().strip()

    @property
    def directory(self):
        if self._directory is None:
            content = self.request()

            self._directory = self.scrape(content)
        return self._directory
