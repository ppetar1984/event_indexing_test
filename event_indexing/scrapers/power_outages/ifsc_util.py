from math import ceil, sin, pi, log, floor

from shapely.geometry import Polygon
from shapely.ops import cascaded_union


def decode_line(line):
    coordinates = []
    size = len(line)
    latitude = 0
    longitude = 0
    index = 0

    while size > index:
        i = 0
        j = 0

        while True:
            char = line[index]
            char_index = ord(char) - 63
            j |= (31 & char_index) << i
            i += 5
            index += 1

            if char_index < 32 or index >= len(line):
                break

        if 1 & j:
            shift = ~(j >> 1)
        else:
            shift = j >> 1

        latitude += shift

        i = 0
        j = 0

        while True:
            char = line[index]
            char_index = ord(char) - 63
            j |= (31 & char_index) << i
            i += 5
            index += 1

            if char_index < 32 or index >= len(line):
                break

        if 1 & j:
            shift = ~(j >> 1)
        else:
            shift = j >> 1

        longitude += shift

        coordinate = [latitude / 1e5, longitude / 1e5]
        coordinates.append(coordinate)

    return coordinates


def get_map_spatial_indexes(bounds, zoom):
    indexes = set()
    tile_size = 256
    screen_size = {
        'width': 2560.0,
        'height': 1440.0
    }

    corrected_zoom = zoom - 1
    multiplier = 1
    zoom_multiplier = multiplier
    size_multiplier = multiplier

    if zoom - 1 > corrected_zoom:
        zoom_multiplier = multiplier * pow(2, zoom - 1 - corrected_zoom)
    else:
        size_multiplier = multiplier * pow(2, corrected_zoom - (zoom - 1))

    corrected_bounds = get_corrected_bounds(bounds, zoom_multiplier)

    width = ceil(1 + ceil(screen_size['width'] / tile_size) * size_multiplier)
    height = ceil(1 + ceil(screen_size['height'] / tile_size) * size_multiplier)

    bound_coordinates = get_bound_coordinates(corrected_bounds)

    longitude_step = (bound_coordinates[3] - bound_coordinates[1]) / (width - 1)
    latitude_step = (bound_coordinates[2] - bound_coordinates[0]) / (height - 1)

    width_index = 0
    while width > width_index:
        longitude = bound_coordinates[1] + width_index * longitude_step

        height_index = 0
        while height > height_index:
            latitude = bound_coordinates[0] + height_index * latitude_step

            spatial_index_key = get_spatial_index_key(latitude, longitude, corrected_zoom, tile_size)

            if spatial_index_key:
                indexes.add(spatial_index_key)

            height_index += 1
        width_index += 1

    return indexes


def get_corrected_bounds(bounds, zoom_multiplier):
    if zoom_multiplier == 1:
        return bounds

    bound_coordinates = get_bound_coordinates(bounds)

    longitude_size = abs(bound_coordinates[3] - bound_coordinates[1])
    latitude_size = abs(bound_coordinates[2] - bound_coordinates[0])

    longitude = bound_coordinates[1] + (longitude_size / 2)
    latitude = bound_coordinates[0] + (latitude_size / 2)

    longitude_step = longitude_size * zoom_multiplier / 2
    latitude_step = latitude_size * zoom_multiplier / 2

    return {
        'southwest': {
            'latitude': latitude - latitude_step,
            'longitude': longitude - longitude_step
        },
        'northeast': {
            'latitude': latitude + latitude_step,
            'longitude': longitude + longitude_step
        }
    }


def get_bound_coordinates(bounds):
    southwest = bounds['southwest']
    northeast = bounds['northeast']

    return [southwest['latitude'], southwest['longitude'], northeast['latitude'], northeast['longitude']]


def get_spatial_index_key(latitude, longitude, zoom, tile_size):
    spacial_index_tile = get_spatial_index_tile(latitude, longitude, zoom, tile_size)

    return get_index_key(spacial_index_tile[0], spacial_index_tile[1], zoom)


def get_spatial_index_tile(latitude, longitude, zoom, tile_size):
    pixel = convert_coordinates_to_pixels(latitude, longitude, zoom, tile_size)

    return convert_pixels_to_tile(pixel[0], pixel[1], tile_size)


def get_index_key(x, y, zoom):
    index = zoom
    key = ''

    while index > 0:
        position = 0
        check = 1 << index - 1

        if (x & check) != 0:
            position += 1

        if (y & check) != 0:
            position += 2

        key += str(position)

        index -= 1

    return key


def convert_coordinates_to_pixels(latitude, longitude, zoom, tile_size=None):
    latitude = clamp(latitude, -90.0, 90.0)
    longitude = clamp(longitude, -180.0, 180.0)

    converted_longitude = (longitude + 180) / 360
    converted_latitude = sin(latitude * pi / 180)
    converted_latitude = 0.5 - log((1 + converted_latitude) / (1 - converted_latitude)) / (4 * pi)

    if tile_size is None:
        tile_size = 256
    else:
        tile_size = tile_size << zoom

    x = clamp(converted_longitude * tile_size + 0.5, 0, tile_size - 1)
    y = clamp(converted_latitude * tile_size + 0.5, 0, tile_size - 1)

    return [x, y]


def convert_pixels_to_tile(x, y, tile_size=None):
    if tile_size is None:
        tile_size = 256

    x = int(floor(x / tile_size))
    y = int(floor(y / tile_size))

    return [x, y]


def clamp(value, min_value, max_value):
    return max(min(value, max_value), min_value)


def merge_service_areas(service_areas):
    polygons = []
    for service_area in service_areas:
        coordinates = [(coordinate[1], coordinate[0]) for coordinate in service_area]
        polygon = Polygon(coordinates)
        polygons.append(polygon)

    return cascaded_union(polygons)


def get_bounds(service_area):
    bounds = service_area.bounds

    return {
        'southwest': {
            'latitude': bounds[1],
            'longitude': bounds[0]
        },
        'northeast': {
            'latitude': bounds[3],
            'longitude': bounds[2]
        }
    }


def get_service_area_segments(service_area, latitude_segments, longitude_segments):
    bounds = get_bounds(service_area)

    return get_bounds_segments(bounds, latitude_segments, longitude_segments)


def get_bounds_segments(bounds, latitude_segments, longitude_segments):
    segments = []

    northeast = bounds['northeast']
    southwest = bounds['southwest']

    ne_latitude = float(northeast['latitude'])
    sw_latitude = float(southwest['latitude'])

    ne_longitude = float(northeast['longitude'])
    sw_longitude = float(southwest['longitude'])

    latitude_size = abs(ne_latitude - sw_latitude)
    longitude_size = abs(ne_longitude - sw_longitude)

    latitude_step = latitude_size / latitude_segments
    longitude_step = longitude_size / longitude_segments

    for y in range(0, latitude_segments):
        top = ne_latitude - (latitude_step * y)
        bottom = top - latitude_step

        for x in range(0, longitude_segments):
            right = ne_longitude - (longitude_step * x)
            left = right - longitude_step

            segments.append({
                'southwest': {
                    'latitude': bottom,
                    'longitude': left
                },
                'northeast': {
                    'latitude': top,
                    'longitude': right
                }
            })

    return segments
