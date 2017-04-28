import re

from bs4 import BeautifulSoup

from event_indexing.scrapers.base import IncidentScraper


class IncidentDomScraper(IncidentScraper):
    def get_soup(self, content):
        '''
        Returns BeautifulSoup object. No need to override.
        '''
        return BeautifulSoup(content, 'html.parser')

    def get_text(self, item):
        '''
        Formats DOM text elements. No need to override.
        '''
        text = item.get_text().strip()

        return re.sub(' +', ' ', text)

    def get_content_type(self):
        '''
        Returns content type, override as needed.
        '''
        return 'text/html; charset=utf-8'
