from datetime import timedelta
import pytest
from export_manager._interval import parse_delta


def test_empty():
    with pytest.raises(ValueError):
        parse_delta('')


def test_blank():
    with pytest.raises(ValueError):
        parse_delta(' \t ')


def test_invalid_chars():
    with pytest.raises(ValueError):
        parse_delta('3 minutes + 5 seconds')


def test_multi():
    assert (parse_delta('3 minutes 5 seconds')
            == timedelta(minutes=3, seconds=5))


def test_comma():
    assert (parse_delta('3 minutes, 5 seconds')
            == timedelta(minutes=3, seconds=5))


def test_all():
    assert (parse_delta('1 week 2 days 3 hours 4 minutes 5 seconds')
            == timedelta(weeks=1, days=2, hours=3, minutes=4, seconds=5))


def test_short():
    assert (parse_delta('1w2d3h4m5s')
            == timedelta(weeks=1, days=2, hours=3, minutes=4, seconds=5))
