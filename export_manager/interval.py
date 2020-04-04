from datetime import timedelta
import re

UNITS = {
    'w': 'weeks',
    'wk': 'weeks',
    'week': 'weeks',
    'weeks': 'weeks',
    'd': 'days',
    'day': 'days',
    'days': 'days',
    'h': 'hours',
    'hour': 'hours',
    'hours': 'hours',
    'hr': 'hours',
    'm': 'minutes',
    'min': 'minutes',
    'mins': 'minutes',
    'minute': 'minutes',
    'minutes': 'minutes',
    's': 'seconds',
    'sec': 'seconds',
    'secs': 'seconds',
    'second': 'seconds',
    'seconds': 'seconds'
}
UNIT_PATTERN = '|'.join(UNITS.keys())
PART_RE = re.compile(f'(\\d+)\\s*({UNIT_PATTERN})')
DELTA_RE = re.compile(f'\\A(\\s*{PART_RE.pattern}\\s*,?\\s*)+\\Z')


def parse_delta(s):
    """A simple time interval parser. Given a string such as
    "1 week" or "5 minutes 3 seconds" or "1 day, 2 hours" or "1w2d3h5m4s",
    returns the corresponding timedelta.
    """
    if not DELTA_RE.match(s):
        raise ValueError(f'Unrecognized duration: {s}')
    units = {
        'weeks': 0,
        'days': 0,
        'hours': 0,
        'minutes': 0,
        'seconds': 0
    }

    for match in PART_RE.findall(s):
        qty, unit = match
        units[UNITS[unit]] = float(qty)
    return timedelta(weeks=units['weeks'],
                     days=units['days'],
                     hours=units['hours'],
                     minutes=units['minutes'],
                     seconds=units['seconds'])
