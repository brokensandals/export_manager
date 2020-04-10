from datetime import timedelta
import re

_UNITS = {
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
_UNIT_PATTERN = '|'.join(_UNITS.keys())
_PART_RE = re.compile(f'(\\d+)\\s*({_UNIT_PATTERN})')
_DELTA_RE = re.compile(f'\\A(\\s*{_PART_RE.pattern}\\s*,?\\s*)+\\Z')


def parse_delta(s):
    """Returns a timedelta parsed from a human-friendly string.

    Example inputs:
    "1 week"
    "5 minutes 3 seconds"
    "1 day, 2 hours"
    "1w2d3h5m4s"

    Raises ValueError if it can't parse the string.
    """
    if not _DELTA_RE.match(s):
        raise ValueError(f'Unrecognized duration: {s}')
    units = {
        'weeks': 0,
        'days': 0,
        'hours': 0,
        'minutes': 0,
        'seconds': 0
    }

    for match in _PART_RE.findall(s):
        qty, unit = match
        units[_UNITS[unit]] = float(qty)
    return timedelta(weeks=units['weeks'],
                     days=units['days'],
                     hours=units['hours'],
                     minutes=units['minutes'],
                     seconds=units['seconds'])
