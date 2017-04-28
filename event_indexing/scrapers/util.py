from pyproj import Proj, transform

COORDINATES_PRECISION = 8


def project_coordinates(projection, x, y):
    '''
    Converts a projected coordinate to a lat/lon.
    '''
    inProj = Proj(init=projection, preserve_units=True)
    outProj = Proj(init='epsg:4326')
    lon, lat = transform(inProj, outProj, x, y)

    lat = round(lat, COORDINATES_PRECISION)
    lon = round(lon, COORDINATES_PRECISION)

    return lat, lon


def is_valid_coordinate(latitude, longitude):
    '''
    Checks if coordinate is valide
    '''
    return latitude >= -90.0 and latitude <= 90.0 and longitude >= -180.0 and longitude <= 180.0
