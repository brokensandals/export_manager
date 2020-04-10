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


def touch_metrics(dsa, parcel_id='2000-01-01T010101Z', row=None):
    if not row:
        row = {'files': '1', 'bytes': '10'}
    row = {'parcel_id': parcel_id, **row}
    dsa.update_metrics({parcel_id: row})


def warnlines(s):
    return [l for l in s.splitlines() if 'WARNING' in l]


def test_empty():
    with tempdatasets(0):
        r = Report([])
        assert r.has_no_complete == []
        assert r.is_overdue == []
        assert r.last_is_incomplete == []
        assert not r.has_warnings
        assert r.plaintext() == 'No datasets were specified :/'


def test_all_good():
    with tempdatasets(2) as dsas:
        touch_data(dsas[0])
        touch_metrics(dsas[0])
        touch_data(dsas[1])
        touch_metrics(dsas[1])
        r = Report(dsas)
        assert r.has_no_complete == []
        assert r.is_overdue == []
        assert r.last_is_incomplete == []
        assert not r.has_warnings
        assert 'WARNING' not in r.plaintext()


def test_has_no_complete():
    with tempdatasets(3) as dsas:
        touch_incomplete(dsas[0])
        touch_data(dsas[1])
        touch_metrics(dsas[1])
        r = Report(dsas)
        assert r.has_no_complete == [dsas[0], dsas[2]]
        assert r.has_warnings
        assert (warnlines(r.plaintext()) ==
                ['WARNING: no complete parcel for: ds0, ds2',
                 'WARNING: most recent parcel is incomplete for: ds0'])


@freeze_time('2010-01-05T050000Z')
def test_is_overdue():
    with tempdatasets(5) as dsas:
        dsas[0].write_config({'interval': '1 hour'})
        dsas[1].write_config({'interval': '1 hour'})
        touch_data(dsas[1], '2010-01-05T033000Z')
        touch_metrics(dsas[1], '2010-01-05T033000Z')
        dsas[2].write_config({'interval': '1 hour'})
        touch_data(dsas[2], '2010-01-05T033000Z')
        touch_data(dsas[2], '2010-01-05T040500Z')
        touch_metrics(dsas[2], '2010-01-05T040500Z')
        dsas[3].write_config({'interval': '1 hour'})
        touch_data(dsas[3], '2010-01-05T033000Z')
        touch_metrics(dsas[3], '2010-01-05T033000Z')
        touch_incomplete(dsas[3], '2010-01-05T040500Z')
        r = Report(dsas)
        assert r.is_overdue == [dsas[0], dsas[1]]
        assert r.has_warnings
        assert (warnlines(r.plaintext()) ==
                ['WARNING: no complete parcel for: ds0, ds4',
                 'WARNING: overdue: ds0, ds1',
                 'WARNING: most recent parcel is incomplete for: ds3'])


def test_last_complete_missing_metrics():
    with tempdatasets(5) as dsas:
        touch_data(dsas[1])
        touch_data(dsas[2])
        touch_metrics(dsas[2])
        touch_data(dsas[3], '2000-01-01T010101Z')
        touch_metrics(dsas[3], '2000-01-01T010101Z')
        touch_data(dsas[3], '2000-02-01T010101Z')
        touch_data(dsas[4], '2000-01-01T010101Z')
        touch_metrics(dsas[4], '2000-01-01T010101Z',
                      {'foo': '1', 'bar': 'ERROR', 'baz': '3'})
        r = Report(dsas)
        assert r.missing_metrics == [dsas[1], dsas[3], dsas[4]]
        assert r.has_warnings
        assert (warnlines(r.plaintext()) ==
                ['WARNING: no complete parcel for: ds0',
                 'WARNING: missing metrics in last complete parcel for:'
                 ' ds1, ds3, ds4'])


def test_last_is_incomplete():
    with tempdatasets(3) as dsas:
        touch_data(dsas[0], '2001-01-01T000000Z')
        touch_metrics(dsas[0], '2001-01-01T000000Z')
        touch_incomplete(dsas[0], '2005-01-01T000000Z')
        touch_data(dsas[1])
        touch_metrics(dsas[1])
        touch_data(dsas[2], '2003-04-05T000000Z')
        touch_metrics(dsas[2], '2003-04-05T000000Z')
        touch_incomplete(dsas[2], '2004-01-01T000000Z')
        r = Report(dsas)
        assert r.last_is_incomplete == [dsas[0], dsas[2]]
        assert r.has_warnings
        assert (warnlines(r.plaintext()) ==
                ['WARNING: most recent parcel is incomplete for: ds0, ds2'])


def test_last_success():
    with tempdatasets(4) as dsas:
        touch_data(dsas[0], '2001-01-01T000000Z')
        touch_metrics(dsas[0], '2001-01-01T000000Z', {'success': 'Y'})
        touch_metrics(dsas[0], '2001-02-01T000000Z', {'success': 'Y'})
        touch_data(dsas[1], '2001-02-01T000000Z')
        touch_metrics(dsas[1], '2001-01-01T000000Z', {'success': 'Y'})
        touch_metrics(dsas[1], '2001-02-01T000000Z', {'success': 'Y'})
        touch_metrics(dsas[2], '2001-01-01T000000Z', {'success': 'N'})
        touch_incomplete(dsas[3], '2001-02-01T000000Z')
        touch_metrics(dsas[3], '2001-01-01T000000Z', {'success': 'Y'})
        touch_metrics(dsas[3], '2001-02-01T000000Z', {'success': 'Y'})
        r = Report(dsas)
        assert r.last_success_gone == [dsas[0], dsas[3]]
        assert r.has_warnings
        assert (warnlines(r.plaintext()) ==
                ['WARNING: no complete parcel for: ds2, ds3',
                 'WARNING: most recent parcel is incomplete for: ds3',
                 'WARNING: most recent successful parcel is missing for:'
                 ' ds0, ds3'])
        table = """Newest successes:
------------------------------
ds0  2001-02-01T000000Z (GONE)
ds1  2001-02-01T000000Z
ds2  NONE
ds3  2001-02-01T000000Z (GONE)
"""
        assert table in r.plaintext()


@freeze_time('2001-10-10T12:00:00Z')
def test_highlighted_metrics():
    with tempdatasets(3) as dsas:
        touch_metrics(dsas[0], '2001-01-01T000000Z',
                      {'success': 'N', 'bytes': '1'})
        touch_metrics(dsas[1], '2001-01-01T000000Z',
                      {'success': 'Y', 'bytes': '1'})
        touch_metrics(dsas[1], '2001-01-02T000000Z',
                      {'success': 'Y', 'bytes': '2'})
        touch_metrics(dsas[1], '2001-10-01T000000Z',
                      {'success': 'Y', 'bytes': '3'})
        touch_metrics(dsas[1], '2001-10-08T100000Z',
                      {'success': 'Y', 'bytes': '4'})
        touch_metrics(dsas[2], '2001-01-01T000000Z',
                      {'success': 'N', 'bytes': '1'})
        touch_metrics(dsas[2], '2001-01-02T000000Z',
                      {'success': 'Y', 'bytes': '2'})
        r = Report(dsas)
        assert r.highlighted_metrics == {
            dsas[1]: [{'parcel_id': '2001-10-08T100000Z',
                       'success': 'Y', 'bytes': '4'},
                      {'parcel_id': '2001-10-01T000000Z',
                       'success': 'Y', 'bytes': '3'},
                      {'parcel_id': '2001-01-02T000000Z',
                       'success': 'Y', 'bytes': '2'}],
            dsas[2]: [{'parcel_id': '2001-01-02T000000Z',
                       'success': 'Y', 'bytes': '2'}],
        }
        expected = """Metrics for ds1:

  name   2 days ago  9 days ago  281 days ago
  --------------------------------------------
  bytes  4           3           2           

Metrics for ds2:

  name   281 days ago
  --------------------
  bytes  2"""
        assert expected in r.plaintext()
