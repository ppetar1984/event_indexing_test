from urlparse import urljoin

from event_indexing.scrapers.base_dom_scraper import IncidentDomScraper


class ClarkCountyFDCad(IncidentDomScraper):
    name = 'ClarkCountyFDCad'
    tz_name = 'US/Pacific'

    def get_provider(self, **kwargs):
        '''
        id: id of source, should be same as file name
        name: Full name of source
        api_host: api host (without protocol). This can be different than URL
        api_route: route to the api
        url: url to visible data set

        ** Route can be host/route can be different from URL in the case
        where the website displaying the data is not the same as the website
        providing the data. For example, don't index an iframe, go to the source
        and index from there but keep url as the original website (the one that has the iframe)
        '''
        return {
            'id': 'clark_county_fd_cad',
            'name': 'Clark County Fire Department',
            'api_host': 'fire.co.clark.nv.us',
            'api_route': '/Alarm%20OfficeConverted.aspx',
            'url': 'http://fire.co.clark.nv.us/Alarm%20OfficeConverted.aspx'
        }

    def get_url(self, **kwargs):
        '''
        Example of how you can override get_url, not needed in this case.
        '''
        provider = self.get_provider()
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def get_incidents(self, content, **kwargs):
        '''
        Convert content to BeautifulSoup object (for DOM scraper). Find elements
        and convert them into raw_incidents.
        '''
        soup = self.get_soup(content)
        table = soup.find(id='grdData')
        rows = table.find_all('tr')[1:]

        return [self.get_incident(row) for row in rows]

    def get_incident(self, row, **kwargs):
        '''
        Converts DOM element into a raw_incident. Must always return: id, incident, datetime,
        created_at, address or lat/lon (see oakland_pd_cad.py for lat/lon example).
        Optional can return details.

        '''
        # Get all elements for given row
        items = row.find_all('td')

        # Breakdown elements into variables
        date_time = items[0]
        incident = items[2]
        address = items[3]

        # Use get_text to get text from element
        date_time = self.get_text(date_time)
        incident = self.get_text(incident)
        address = self.get_text(address)

        # Get created_at from date_time (string from website)
        created_at = self.get_created_at(date_time)

        # Create id, always use following order created_at, incident, address or created_at, incident, latitude, longitude
        incident_id = self.get_incident_id([created_at, incident, address])

        return {
            'id': incident_id,
            'incident': incident,
            'address': address,
            'datetime': date_time,
            'created_at': created_at,
        }
