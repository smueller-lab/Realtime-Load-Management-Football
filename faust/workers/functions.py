import json
from datetime import datetime
# general functions

TIME_FORMAT = '%Y-%m-%d''T''%H:%M:%S.%f%z'

# convert faust Record to a dictionary for faster Kafka writing
def faustRecord_toDict(record):
    return json.loads(record.dumps().decode('UTF-8'))

# get time difference between two events in seconds
def get_timediff_event(earlier_event, later_event):
    time_diff = datetime.strptime(later_event, TIME_FORMAT) - datetime.strptime(earlier_event, TIME_FORMAT)
    return time_diff.total_seconds()