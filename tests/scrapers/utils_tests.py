import unittest

from event_indexing.scrapers.util import project_coordinates


class UtilsTests(unittest.TestCase):
    def test_project_coordinates(self):
        projection = 'epsg:2276'
        x = 2959481.00
        y = 6821096.00

        lat, lon = project_coordinates(projection, x, y)

        self.assertEqual(lat, 32.33813692)
        self.assertEqual(lon, -95.29096343)
