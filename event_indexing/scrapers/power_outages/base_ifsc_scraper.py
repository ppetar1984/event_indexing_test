import math
import multiprocessing
from multiprocessing.pool import Pool
from urlparse import urljoin

import requests

from event_indexing.scrapers.base_json_scraper import IncidentJsonScraper
from event_indexing.scrapers.power_outages.base_ifsc_directory import IFSCDirectory
from event_indexing.scrapers.power_outages.base_ifsc_service_area import IFSCServiceAreas
from event_indexing.scrapers.power_outages.ifsc_util import decode_line, get_map_spatial_indexes
from event_indexing.util.time_utils import get_tz_now

INVALID_INCIDENTS = {'Planned Maintenance', }
VALID_INCIDENTS = {'Under Evaluation', 'Unknown'}
MINIMUM_CUSTOMERS_AFFECTED = 10

TIME_OFFSET = 3  # 3 min polling

ZOOM_LEVEL = 11

PROCESSES = multiprocessing.cpu_count() * 3

'''
See Christian for details
'''


def request(data):
    url = data['url']
    headers = data['headers']
    params = data['params']
    meta = data['meta']

    r = requests.get(url=url, headers=headers, params=params, verify=False)
    # source returns 403 or 404 for "successful" response
    if r.status_code != 404 and r.status_code != 403:
        r.raise_for_status()
    elif r.status_code == 404 or r.status_code == 403:
        return {}, meta

    return r.json(), meta


class IFSCScraper(IncidentJsonScraper):
    _indexes = None
    _directory = None

    def run(self):
        incidents = []
        p = Pool(PROCESSES)

        try:
            for raw_incidents, meta in p.imap_unordered(request, self.indexes):
                for raw_incident in self.scrape(raw_incidents):
                    incidents.append(self.parse(raw_incident, meta=meta))
            self.publish(incidents)
        except:
            print 'Parser {}: Failed to index source'.format(self.name)

    def get_url(self, **kwargs):
        directory = kwargs['directory']
        index = kwargs['index']

        provider = self.get_provider(directory=directory, index=index)
        host = 'http://{}'.format(provider['api_host'])

        return urljoin(host, provider['api_route'])

    def parse(self, raw_incident, **kwargs):
        meta = kwargs['meta']

        index = meta['index']
        directory = meta['directory']

        alert = super(IFSCScraper, self).parse(raw_incident, **kwargs)
        provider = self.get_provider(directory=directory, index=index)

        alert['source']['provider'] = provider

        return alert

    def is_valid_incident(self, raw_incident):
        description = raw_incident['desc']
        cluster = description['cluster']

        # Cluster event
        if cluster:
            return False

        incident = description.get('cause')
        affected = description['cust_a']['val']

        if incident and any(token.lower() in incident.lower() for token in INVALID_INCIDENTS):
            return False

        return affected >= MINIMUM_CUSTOMERS_AFFECTED

    def get_incidents(self, content, **kwargs):
        incidents = content.get('file_data', [])

        return [self.get_incident(incident) for incident in incidents if self.is_valid_incident(incident)]

    def get_incident(self, raw_incident, **kwargs):
        description = raw_incident['desc']
        incident = self.get_description(raw_incident)
        latitude, longitude = self.get_coordinates(raw_incident)

        date_time = description.get('start')

        if date_time:
            created_at = self.get_created_at(date_time)
        else:
            created_at = 0

        incident_id = self.get_incident_id([created_at, incident, latitude, longitude])

        return {
            'id': incident_id,
            'incident': incident,
            'latitude': latitude,
            'longitude': longitude,
            'created_at': created_at,
        }

    def get_description(self, raw_incident):
        description = raw_incident['desc']
        incident = description.get('cause', 'Power Outage')

        if any(token.lower() in incident.lower() for token in VALID_INCIDENTS):
            incident = 'Power Outage'

        return incident

    def get_coordinates(self, raw_incident):
        geometry = raw_incident['geom']
        coordinate = geometry['p'][0]

        coordinate = decode_line(coordinate)
        coordinate = coordinate[0]

        latitude = coordinate[0]
        longitude = coordinate[1]

        return latitude, longitude

    def get_directory_url(self):
        raise NotImplementedError

    def get_service_areas_url(self):
        return None

    def get_service_areas_bounds(self):
        return None

    def get_api_route(self, directory, index):
        raise NotImplementedError

    @property
    def indexes(self):
        if self._indexes is None:
            url = self.get_service_areas_url()
            bounds = self.get_service_areas_bounds()

            if url is None and bounds is None:
                raise NotImplementedError

            scraper = IFSCServiceAreas(None, None, self.proxy_host, url, bounds)
            self._indexes = []

            segments = scraper.segments
            size = len(segments)

            now = get_tz_now()
            limit = int(math.ceil(size / float(TIME_OFFSET)))
            index = (now.minute % TIME_OFFSET) * limit

            # directory
            directory = self.directory

            # limiting the calls by doing 3 segments per scrape
            for segment in segments[index:(index + limit)]:
                indexes = get_map_spatial_indexes(segment, ZOOM_LEVEL)

                for index in indexes:
                    url = self.get_url(directory=directory, index=index)
                    headers = self.get_headers()
                    params = self.get_params()

                    self._indexes.append({
                        'url': url,
                        'headers': headers,
                        'params': params,
                        'meta': {
                            'index': index,
                            'directory': directory
                        }
                    })

        return self._indexes

    @property
    def directory(self):
        if self._directory is None:
            url = self.get_directory_url()

            scraper = IFSCDirectory(None, None, self.proxy_host, url)
            self._directory = scraper.directory
        return self._directory
