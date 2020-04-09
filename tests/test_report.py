from contextlib import contextmanager
from freezegun import freeze_time
from pathlib import Path
from tempfile import TemporaryDirectory
from export_manager.dataset import DatasetAccessor
from export_manager.report import Report


@contextmanager
def tempdatasets(count):
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath)
        dsas = []
        for i in range(count):
            dsa = DatasetAccessor(path.joinpath(f'ds{i}'))
            dsa.initialize()
            dsas.append(dsa)
        yield dsas


def touch_data(dsa, parcel_id='2000-01-01T010101Z'):
    dsa.data_path.joinpath(f'{parcel_id}.txt').touch()


def touch_incomplete(dsa, parcel_id='2000-01-01T010101Z'):
    dsa.incomplete_path.joinpath(f'{parcel_id}.txt').touch()


def warnlines(s):
    return [l for l in s.splitlines() if 'WARNING' in l]


def test_empty():
    with tempdatasets(0):
        r = Report([])
        assert r.has_no_complete == []
        assert r.is_overdue == []
        assert r.last_is_incomplete == []
        assert r.plaintext() == ''


def test_all_good():
    with tempdatasets(2) as dsas:
        touch_data(dsas[0])
        touch_data(dsas[1])
        r = Report(dsas)
        assert r.has_no_complete == []
        assert r.is_overdue == []
        assert r.last_is_incomplete == []
        assert 'WARNING' not in r.plaintext()


def test_has_no_complete():
    with tempdatasets(3) as dsas:
        touch_incomplete(dsas[0])
        touch_data(dsas[1])
        r = Report(dsas)
        assert r.has_no_complete == [dsas[0], dsas[2]]
        assert (warnlines(r.plaintext()) ==
                ['WARNING: no complete parcels for: ds0, ds2',
                 'WARNING: most recent parcels are incomplete for: ds0'])


@freeze_time('2010-01-05T050000Z')
def test_is_overdue():
    with tempdatasets(5) as dsas:
        dsas[0].write_config({'interval': '1 hour'})
        dsas[1].write_config({'interval': '1 hour'})
        touch_data(dsas[1], '2010-01-05T033000Z')
        dsas[2].write_config({'interval': '1 hour'})
        touch_data(dsas[2], '2010-01-05T033000Z')
        touch_data(dsas[2], '2010-01-05T040500Z')
        dsas[3].write_config({'interval': '1 hour'})
        touch_data(dsas[3], '2010-01-05T033000Z')
        touch_incomplete(dsas[3], '2010-01-05T040500Z')
        r = Report(dsas)
        assert r.is_overdue == [dsas[0], dsas[1]]
        assert (warnlines(r.plaintext()) ==
                ['WARNING: no complete parcels for: ds0, ds4'],
                ['WARNING: overdue: ds0, ds1'],
                ['WARNING: most recent parcels are incomplete for: ds3'])


def test_last_is_incomplete():
    with tempdatasets(3) as dsas:
        touch_data(dsas[0], '2001-01-01T000000Z')
        touch_incomplete(dsas[0], '2005-01-01T000000Z')
        touch_data(dsas[1])
        touch_data(dsas[2], '2003-04-05T000000Z')
        touch_incomplete(dsas[2], '2004-01-01T000000Z')
        r = Report(dsas)
        assert r.last_is_incomplete == [dsas[0], dsas[2]]
        assert (warnlines(r.plaintext()) ==
                ['WARNING: most recent parcels are incomplete for: ds0, ds2'])
