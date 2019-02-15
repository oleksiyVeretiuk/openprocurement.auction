import os
from datetime import time
from pytz import timezone

from datetime import timedelta, datetime


def read_json(name):
    import inspect
    import os.path
    from json import loads
    caller_file = inspect.stack()[1][1]
    caller_dir = os.path.dirname(os.path.realpath(caller_file))
    file_path = os.path.join(caller_dir, name)
    with open(file_path) as lang_file:
        data = lang_file.read()
    return loads(data)


STREAMS_ID = 'streams'
WORKING_DAY_START = time(11, 0)
INSIDER_WORKING_DAY_START = time(9, 30)
TEXAS_WORKING_DAY_START = time(10, 0)
WORKING_DAY_END = time(16, 0)
INSIDER_WORKING_DAY_DURATION = timedelta(minutes=480)
TEXAS_WORKING_DAY_DURATION = timedelta(hours=7)  # Does not affect anything
DEFAULT_STREAMS_DOC = {
    '_id': STREAMS_ID,
    'streams': 10,
    'dutch_streams': 15,
    'texas_streams': 20
}


CALENDAR_ID = 'calendar'
STREAMS_ID = 'streams'
ROUNDING = timedelta(minutes=29)
MIN_PAUSE = timedelta(minutes=3)
BIDDER_TIME = timedelta(minutes=6)
SERVICE_TIME = timedelta(minutes=9)
STAND_STILL_TIME = timedelta(days=1)
SMOOTHING_MIN = 10
SMOOTHING_REMIN = 60
# value should be greater than SMOOTHING_MIN and SMOOTHING_REMIN
SMOOTHING_MAX = 300
NOT_CLASSIC_AUCTIONS = ['dgfInsider', 'sellout.insider']
STREAMS_KEYS = ['streams', 'dutch_streams', 'texas_streams']


TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')
# True means holiday
WORKING_DAYS = read_json('working_days.json')
