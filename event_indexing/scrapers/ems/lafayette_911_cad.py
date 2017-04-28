import re

from bs4 import BeautifulSoup

from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper


class Lafayette911Cad(IncidentJsonScraper):
    name = 'Lafayette911Cad'
    tz_name = 'US/Central'
    method = 'POST'

    def get_provider(self, **kwargs):
        return {
            'id': 'lafayette_911_cad',
            'name': 'Lafayette 911',
            'api_host': 'www.lafayette911.org',
            'api_route': '/default.aspx',
            'url': 'http://apps.lafayettela.gov/L911/Service2.svc/getTrafficIncidents'
        }

    def get_incidents(self, content, **kwargs):
        incidents = content.get('d', [])

        soup = BeautifulSoup(incidents, "html.parser")
        rows = soup.find_all('tr')

        if rows:
            rows = rows[1:]

        return [self.get_incident(row) for row in rows]

    def get_incident(self, row, **kwargs):
        items = row.find_all('td')

        address = items[0]
        incident = items[1]
        date_time = items[2]

        date_time = self.get_text(date_time)
        incident = self.get_text(incident)

        address = self.get_text(address)
        address = address.replace(u'\xa0', ' ')
        address = address.strip()
        address = re.sub(' +', ' ', address)

        created_at = self.get_created_at(date_time)

        incident_id = self.get_incident_id([created_at, incident, address])

        return {
            'id': incident_id,
            'incident': incident,
            'address': address,
            'created_at': created_at,
        }

    def get_text(self, item):
        text = item.get_text().strip()

        return re.sub(' +', ' ', text)
