import datetime
import time

from dateutil import parser
from dateutil.tz import tzutc

EPOCH = datetime.datetime.fromtimestamp(0, tzutc())


def parse_timestamp(time_string, tzinfo=None):
    '''
    Converts a time string with timezone to epoch timestamp
    '''
    try:
        parsed = parser.parse(time_string)
    except:
        return 0  # dateutil parser chokes on leap seconds, return 0 so the alert will be filtered out as an "old alert"

    if tzinfo:
        parsed = parsed.replace(tzinfo=tzinfo)

    return (parsed - EPOCH).total_seconds()


def convert_from_12_to_24_format(time_string, time_format):
    '''
    Converts 12 hour time to 24 hour time.
    '''
    return '{:%H:%M:%S}'.format(datetime.datetime.strptime(time_string, time_format))


def add_time_period(time_string, tzinfo=None):
    '''
    Figures out if time is AM or PM if not provided on source
    '''
    # Find now for given timezone
    tz_now = get_tz_now(tzinfo)

    # Get parsed date
    parsed = parser.parse(time_string)
    if tzinfo:
        parsed = parsed.replace(tzinfo=tzinfo)

    # now hour
    tz_hour = tz_now.hour
    # now hour adjusted for 12 hour clock (13:01 > 1:01)
    adjusted_tz_hour = tz_hour - 12
    # parsed hour
    parsed_hour = parsed.hour

    # now is later than 12 (noon) and parsed hour is less than or equal to adjusted tz hour
    if tz_hour > 12 and parsed_hour <= adjusted_tz_hour:
        parsed = parsed + datetime.timedelta(hours=12)
    # now is midnight and parsed hour is 12 (assuming it's midnight)
    elif tz_hour == 0 and parsed_hour == 12:
        parsed = parsed - datetime.timedelta(hours=12)

    return '{:%m/%d/%Y %H:%M:%S}'.format(parsed)


def get_tz_now(tzinfo=None):
    '''
    Unix now timestamp for timezone
    '''
    return datetime.datetime.now(tzinfo)


def now_seconds():
    '''
    Unix now timestamp in seconds
    '''
    return time.time()


def now_milliseconds():
    '''
    Unix now timestamp in milliseconds
    '''
    return int(round(now_seconds() * 1000))
