from contextlib import contextmanager
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from freezegun import freeze_time
from git import Repo
import os
from pathlib import Path
import pytest
from tempfile import TemporaryDirectory
from export_manager import dataset
from export_manager.dataset import DatasetAccessor
from export_manager.dataset import ParcelExistsException


@contextmanager
def tempdatasetdir(git=False):
    with TemporaryDirectory() as rawpath:
        dsa = DatasetAccessor(rawpath)
        dsa.initialize(git=git)
        yield dsa


def test_initialize():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath).joinpath('ds')
        dsa = DatasetAccessor(path)
        dsa.initialize()
        assert path.is_dir()
        assert path.joinpath('data').is_dir()
        assert (path.joinpath('metrics.csv').read_text()
                == dataset._INITIAL_METRICS_CSV)
        assert (path.joinpath('config.toml').read_text()
                == dataset._DEFAULT_CONFIG_TOML)
        assert not path.joinpath('.git').exists()
        assert not path.joinpath('.gitignore').exists()


def test_initialize_git():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath).joinpath('ds')
        dsa = DatasetAccessor(path)
        dsa.initialize(git=True)
        repo = Repo(str(path))
        assert not repo.is_dirty()
        assert (path.joinpath('.gitignore').read_text()
                == dataset._DEFAULT_GITIGNORE)
        assert repo.head.commit.message == '[export_manager] initialize'
        assert (sorted([b.name for b in repo.head.commit.tree.blobs])
                == ['.gitignore', 'config.toml', 'metrics.csv'])
        assert (path.joinpath('config.toml').read_text()
                == dataset._DEFAULT_CONFIG_TOML + 'git = true\n')


def test_read_config():
    with tempdatasetdir() as dsa:
        dsa.config_path.write_text('cmd = "echo hi"')
        assert dsa.read_config() == {'cmd': 'echo hi'}


def test_read_metrics_nonexistent():
    with tempdatasetdir() as dsa:
        dsa.metrics_path.unlink()
        assert dsa.read_metrics() == {}


def test_update_metrics():
    with tempdatasetdir() as dsa:
        initial = {
            '1999-01-02T030405Z': {
                'parcel_id': '1999-01-02T030405Z',
                'party_intensity': '100',
            },
            '2000-01-02T030405Z': {
                'parcel_id': '2000-01-02T030405Z',
                'party_intensity': '1',
            },
        }
        dsa._update_metrics(initial)
        assert dsa.read_metrics() == initial

        updates = {
            '1999-01-02T030405Z': {
                'parcel_id': '1999-01-02T030405Z',
                'party_intensity': '9000',
                'bugs': '1000000000',
            },
            '2020-01-02T030405Z': {
                'parcel_id': '2020-01-02T030405Z',
                'bugs': '2000000000',
            },
        }
        dsa._update_metrics(updates)
        expected = {
            '1999-01-02T030405Z': {
                'parcel_id': '1999-01-02T030405Z',
                'party_intensity': '9000',
                'bugs': '1000000000',
            },
            '2000-01-02T030405Z': {
                'parcel_id': '2000-01-02T030405Z',
                'party_intensity': '1',
                'bugs': '',
            },
            '2020-01-02T030405Z': {
                'parcel_id': '2020-01-02T030405Z',
                'party_intensity': '',
                'bugs': '2000000000',
            },
        }
        assert dsa.read_metrics() == expected


def test_run_export_no_cmd():
    with tempdatasetdir() as dsa:
        dsa._run_export(dataset.new_parcel_id())
        assert sum(1 for x in dsa.data_path.glob('*')) == 0
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0


def test_run_export_no_data():
    with tempdatasetdir() as dsa:
        dsa.write_config({'cmd': 'echo hi && (echo muahaha >&2)'})
        with pytest.raises(Exception):
            dsa._run_export(dataset.new_parcel_id())
        assert sum(1 for x in dsa.data_path.glob('*')) == 0
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert next(dsa.log_path.glob('*.out')).read_text() == 'hi\n'
        assert next(dsa.log_path.glob('*.err')).read_text() == 'muahaha\n'


def test_run_export_existing_parcel():
    with tempdatasetdir() as dsa:
        parcel_id = '2000-01-02T030400Z'
        dsa.data_path.joinpath(f'{parcel_id}.txt').write_text('old')
        dsa.write_config({'cmd': 'echo hi > $PARCEL_PATH.txt'})
        with pytest.raises(ParcelExistsException):
            dsa._run_export(parcel_id)


def test_run_export():
    with tempdatasetdir() as dsa:
        dsa.write_config({'cmd': 'echo hi > $PARCEL_PATH.txt'})
        parcel_id = dataset.new_parcel_id()
        dsa._run_export(parcel_id)
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert (dsa.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hi\n')
        assert dsa.log_path.joinpath(f'{parcel_id}.out').read_text() == ''
        assert dsa.log_path.joinpath(f'{parcel_id}.err').read_text() == ''


def test_perform_export_no_cmd():
    with tempdatasetdir() as dsa:
        dsa.perform_export()
        assert sum(1 for x in dsa.data_path.glob('*')) == 0
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0


def test_perform_export_no_cmd_git():
    with tempdatasetdir(git=True) as dsa:
        dsa.perform_export()
        assert sum(1 for x in dsa.data_path.glob('*')) == 0
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        repo = Repo(str(dsa.path))
        assert repo.head.commit.message == '[export_manager] initialize'


def test_perform_export():
    with tempdatasetdir() as dsa:
        dsa.write_config({'cmd': 'echo hi > $PARCEL_PATH.txt'})
        parcel_id = dsa.perform_export()
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert (dsa.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hi\n')
        assert dsa.log_path.joinpath(f'{parcel_id}.out').read_text() == ''
        assert dsa.log_path.joinpath(f'{parcel_id}.err').read_text() == ''


def test_perform_export_given_id():
    with tempdatasetdir() as dsa:
        dsa.write_config({'cmd': 'echo hi > $PARCEL_PATH.txt'})
        parcel_id = '2001-01-01T010101Z'
        assert dsa.perform_export(parcel_id) == parcel_id
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert (dsa.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hi\n')
        assert dsa.log_path.joinpath(f'{parcel_id}.out').read_text() == ''
        assert dsa.log_path.joinpath(f'{parcel_id}.err').read_text() == ''


def test_perform_export_git():
    with tempdatasetdir(git=True) as dsa:
        dsa.write_config({'cmd': 'echo hi > $PARCEL_PATH.txt', 'git': True})
        parcel_id = dsa.perform_export()
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert (dsa.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hi\n')
        repo = Repo(str(dsa.path))
        assert (repo.head.commit.message
                == f'[export_manager] add new export {parcel_id}')
        assert (list(repo.head.commit.stats.files.keys())
                == [f'data/{parcel_id}.txt', 'metrics.csv'])


def test_auto_ingest_no_paths():
    with tempdatasetdir() as dsa:
        dsa._run_ingest() # does nothing


def test_auto_ingest_no_matches():
    with tempdatasetdir() as dsa:
        dsa.write_config({
            'ingest': {
                'paths': [
                    'foo', 'bar/*.txt', str(Path('bogus').resolve())]}})
        dsa._run_ingest()  # does nothing


def test_auto_ingest_absolute():
    with tempdatasetdir() as dsa:
        oldpath = dsa.path.joinpath('foo', 'blah.txt')
        pathglob = str(oldpath.resolve()).replace('blah.txt', '*.txt')
        dsa.write_config({'ingest': {'paths': pathglob}})
        dsa.path.joinpath('foo').mkdir()
        oldpath.write_text('hello')
        parcel_paths = dsa._run_ingest()
        assert len(parcel_paths) == 1
        assert not oldpath.exists()
        assert (dsa.data_path.joinpath(
            f'{list(parcel_paths.keys())[0]}.txt').read_text() == 'hello')


def test_auto_ingest_relative():
    with tempdatasetdir() as dsa:
        dsa.write_config({'ingest': {'paths': 'foo/*.txt'}})
        dsa.path.joinpath('foo').mkdir()
        oldpath = dsa.path.joinpath('foo', 'blah.txt')
        oldpath.write_text('hello')
        parcel_paths = dsa._run_ingest()
        assert len(parcel_paths) == 1
        assert not oldpath.exists()
        assert (dsa.data_path.joinpath(
            f'{list(parcel_paths.keys())[0]}.txt').read_text() == 'hello')


def test_auto_ingest_mtime():
    with tempdatasetdir() as dsa:
        dsa.write_config({'ingest': {'paths': 'foo/*.txt',
                                     'time_source': 'mtime'}})
        dsa.path.joinpath('foo').mkdir()
        oldpath = dsa.path.joinpath('foo', 'blah.txt')
        oldpath.write_text('hello')
        os.utime(oldpath, (10, 1578189722))
        parcel_paths = dsa._run_ingest()
        assert list(parcel_paths.keys()) == ['2020-01-05T020202Z']
        assert not oldpath.exists()
        assert (dsa.data_path.joinpath(
            f'{list(parcel_paths.keys())[0]}.txt').read_text() == 'hello')


def test_ingest_path_existing_parcel():
    with tempdatasetdir() as dsa:
        parcel_id = '2000-01-02T030400Z'
        dsa.data_path.joinpath(f'{parcel_id}.txt').write_text('old')
        path = dsa.path.joinpath('foo')
        with pytest.raises(ParcelExistsException):
            dsa.ingest_path(path, parcel_id)


def test_ingest_path_bad_path():
    with tempdatasetdir() as dsa:
        path = dsa.path.joinpath('foo.txt')
        with pytest.raises(FileNotFoundError):
            dsa.ingest_path(path, dataset.new_parcel_id())


def test_ingest_path():
    with tempdatasetdir() as dsa:
        path = dsa.path.joinpath('foo.txt')
        path.write_text('hello')
        parcel_id = dataset.new_parcel_id()
        dsa.ingest_path(path, parcel_id)
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert (dsa.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hello')
        assert dsa.read_metrics()[parcel_id]['success'] == 'Y'


def test_ingest_path_git():
    with tempdatasetdir(git=True) as dsa:
        path = dsa.path.joinpath('foo.txt')
        path.write_text('hello')
        parcel_id = dataset.new_parcel_id()
        dsa.ingest_path(path, parcel_id)
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert (dsa.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hello')
        assert dsa.read_metrics()[parcel_id]['success'] == 'Y'
        repo = Repo(dsa.path)
        assert ([b.name for b in repo.head.commit.tree['data']]
                == [f'{parcel_id}.txt'])


def test_clean_no_keep():
    with tempdatasetdir() as dsa:
        for i in range(10):
            dsa.data_path.joinpath(f'2000-01-02T03040{i}Z.txt').touch()
        dsa.clean()
        assert sum(1 for x in dsa.data_path.glob('*.txt')) == 10


def test_clean():
    with tempdatasetdir() as dsa:
        dsa.write_config({'keep': 4})
        for i in range(10):
            p_id = f'2000-01-02T03040{i}Z'
            if i % 2 == 0:
                dsa.data_path.joinpath(f'{p_id}.txt').touch()
            else:
                dsa.incomplete_path.joinpath(f'{p_id}.txt').touch()
            if i % 3 == 0:
                dsa.log_path.joinpath(f'{p_id}.out').touch()
            if i % 4 == 0:
                dsa.log_path.joinpath(f'{p_id}.err').touch()
        dsa.clean()
        assert (list(p.name for p in dsa.data_path.glob('*'))
                == ['2000-01-02T030406Z.txt', '2000-01-02T030408Z.txt'])
        assert (list(p.name for p in dsa.incomplete_path.glob('*'))
                == ['2000-01-02T030407Z.txt', '2000-01-02T030409Z.txt'])
        assert (list(p.name for p in dsa.log_path.glob('*.out'))
                == ['2000-01-02T030406Z.out', '2000-01-02T030409Z.out'])
        assert (list(p.name for p in dsa.log_path.glob('*.err'))
                == ['2000-01-02T030408Z.err'])


def test_clean_git():
    with tempdatasetdir(git=True) as dsa:
        dsa.write_config({'keep': 4, 'git': True})
        repo = Repo(str(dsa.path))
        for i in range(10):
            p_id = f'2000-01-02T03040{i}Z'
            dsa.data_path.joinpath(f'{p_id}.txt').touch()
            dsa.incomplete_path.joinpath(f'{p_id}.txt').touch()
            dsa.log_path.joinpath(f'{p_id}.out').touch()
            dsa.log_path.joinpath(f'{p_id}.err').touch()
        repo.index.add(['data'])
        repo.index.commit('commit data for test')
        assert len(repo.head.commit.tree['data'].blobs) == 10
        dsa.clean()
        assert (dsa.find_parcel_ids() ==
                ['2000-01-02T030406Z',
                 '2000-01-02T030407Z',
                 '2000-01-02T030408Z',
                 '2000-01-02T030409Z'])
        assert len(repo.head.commit.tree['data'].blobs) == 4


def test_collect_metrics_no_data():
    with tempdatasetdir() as dsa:
        pid = '2000-01-02T030405Z'
        assert dsa._collect_metrics(pid) == {'parcel_id': pid, 'success': 'N'}


def test_collect_metrics_incomplete():
    with tempdatasetdir() as dsa:
        pid = '2000-01-02T030405Z'
        dsa.incomplete_path.joinpath(f'{pid}.txt').write_text('hello')
        assert dsa._collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'N',
            'bytes': '5',
            'files': '1',
        }


def test_collect_metrics_complete():
    with tempdatasetdir() as dsa:
        pid = '2000-01-02T030405Z'
        dsa.data_path.joinpath(f'{pid}.txt').write_text('hello')
        assert dsa._collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'Y',
            'bytes': '5',
            'files': '1',
        }


def test_collect_metrics_custom():
    with tempdatasetdir() as dsa:
        pid = '2000-01-02T030405Z'
        dsa.data_path.joinpath(f'{pid}.txt').write_text('hello\nhi\nhola\n')
        dsa.write_config(
            {'metrics': {'lines': {'cmd': 'wc -l < $PARCEL_PATH'}}})
        assert dsa._collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'Y',
            'bytes': '14',
            'files': '1',
            'lines': '3',
        }


def test_collect_metrics_error():
    with tempdatasetdir() as dsa:
        pid = '2000-01-02T030405Z'
        dsa.data_path.joinpath(f'{pid}.txt').write_text('hello\nhi\nhola\n')
        dsa.write_config(
            {'metrics': {'lines': {'cmd': 'wc -l < $PARCEL_PATH.oops'}}})
        assert dsa._collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'Y',
            'bytes': '14',
            'files': '1',
            'lines': 'ERROR',
        }


def test_reprocess_metrics():
    with tempdatasetdir() as dsa:
        dsa._update_metrics(
            {'2000-01-01T010101Z': {
                'parcel_id': '2000-01-01T010101Z',
                'success': 'Y',
                'foo': 'bar'}})
        pid = '2000-01-02T030405Z'
        ipath = dsa.incomplete_path.joinpath(f'{pid}.txt')
        ipath.write_text('hi')
        dsa.reprocess_metrics([pid])
        assert dsa.read_metrics() == {
            '2000-01-01T010101Z': {
                'parcel_id': '2000-01-01T010101Z',
                'success': 'Y',
                'foo': 'bar',
                'bytes': '',
                'files': '',
            },
            pid: {
                'parcel_id': pid,
                'success': 'N',
                'foo': '',
                'bytes': '2',
                'files': '1',
            },
        }
        cpath = dsa.data_path.joinpath(f'{pid}.txt')
        ipath.rename(cpath)
        dsa.reprocess_metrics([pid])
        assert dsa.read_metrics()[pid]['success'] == 'Y'


def test_reprocess_metrics_git():
    with tempdatasetdir(git=True) as dsa:
        pid = '2000-01-02T030405Z'
        Path(dsa.data_path.joinpath(f'{pid}.txt')).write_text('hi')
        dsa.reprocess_metrics([pid])
        repo = Repo(str(dsa.path))
        assert (repo.head.commit.message
                == f'[export_manager] reprocess metrics for: {pid}')
        assert list(repo.head.commit.stats.files.keys()) == ['metrics.csv']
        assert dsa.read_metrics()[pid]['bytes'] == '2'


def test_is_due_no_interval():
    with tempdatasetdir() as dsa:
        assert not dsa.is_due()


def test_is_due_no_parcels():
    with tempdatasetdir() as dsa:
        dsa.write_config({'interval': '1000 days'})
        assert dsa.is_due()


def test_is_due_false():
    with tempdatasetdir() as dsa:
        dsa.write_config({'interval': '1 hour'})
        old = datetime.strptime('2000-01-02T030405Z', '%Y-%m-%dT%H%M%S%z')
        now = datetime.now(old.tzinfo)
        last = now - timedelta(minutes=30)
        for dt in [old, last]:
            pid = dt.strftime('%Y-%m-%dT%H%M%SZ')
            dsa.data_path.joinpath(f'{pid}.txt').touch()
        assert not dsa.is_due()


def test_is_due_true():
    with tempdatasetdir() as dsa:
        dsa.write_config({'interval': '1 hour'})
        old = datetime.strptime('2000-01-02T030405Z', '%Y-%m-%dT%H%M%S%z')
        now = datetime.now(old.tzinfo)
        last = now - timedelta(minutes=57)  # within the 5-minute margin
        for dt in [old, last]:
            pid = dt.strftime('%Y-%m-%dT%H%M%SZ')
            dsa.data_path.joinpath(f'{pid}.txt').touch()
        assert dsa.is_due()


def test_parcel_accessors():
    with tempdatasetdir() as dsa:
        id1 = '2001-01-01T010101Z'
        data1 = dsa.data_path.joinpath(f'{id1}.txt')
        out1 = dsa.log_path.joinpath(f'{id1}.out')
        err1 = dsa.log_path.joinpath(f'{id1}.err')
        data1.touch()
        out1.touch()
        err1.touch()

        id2 = '2002-02-02T020202Z'
        incomplete2 = dsa.incomplete_path.joinpath(f'{id2}.txt')
        incomplete2.touch()

        id3 = '2003-03-03T030303Z'
        data3 = dsa.data_path.joinpath(id3)
        data3.mkdir()

        id4 = '2004-04-04T040404Z'
        err4 = dsa.log_path.joinpath(f'{id4}.err')
        err4.touch()

        parcels = dsa.parcel_accessors()
        assert len(parcels) == 4
        assert parcels[0].parcel_id == id1
        assert parcels[0].datetime == datetime(
            2001, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
        assert parcels[0].is_complete()
        assert parcels[0].find_data() == data1
        assert parcels[0].find_incomplete() is None
        assert parcels[0].find_stdout() == out1
        assert parcels[0].find_stderr() == err1
        assert parcels[1].parcel_id == id2
        assert parcels[1].datetime == datetime(
            2002, 2, 2, 2, 2, 2, tzinfo=timezone.utc)
        assert not parcels[1].is_complete()
        assert parcels[1].find_data() is None
        assert parcels[1].find_incomplete() == incomplete2
        assert parcels[1].find_stdout() is None
        assert parcels[1].find_stderr() is None
        assert parcels[2].parcel_id == id3
        assert parcels[2].datetime == datetime(
            2003, 3, 3, 3, 3, 3, tzinfo=timezone.utc)
        assert parcels[2].is_complete()
        assert parcels[2].find_data() == data3
        assert parcels[2].find_incomplete() is None
        assert parcels[2].find_stdout() is None
        assert parcels[2].find_stderr() is None
        assert parcels[3].parcel_id == id4
        assert parcels[3].datetime == datetime(
            2004, 4, 4, 4, 4, 4, tzinfo=timezone.utc)
        assert not parcels[3].is_complete()
        assert parcels[3].find_data() is None
        assert parcels[3].find_incomplete() is None
        assert parcels[3].find_stdout() is None
        assert parcels[3].find_stderr() == err4


def test_process_nothing():
    with tempdatasetdir() as dsa:
        assert dsa.process() == ([], [])


def test_process_nothing_git():
    with tempdatasetdir(git=True) as dsa:
        assert dsa.process() == ([], [])
        repo = Repo(str(dsa.path))
        assert repo.head.commit.message == '[export_manager] initialize'


def test_process_ingest_and_export():
    with tempdatasetdir() as dsa:
        dsa.write_config({
            'cmd': 'echo yay > $PARCEL_PATH.txt',
            'ingest': {
                'paths': 'ingest/*.txt',
                'time_source': 'mtime',
            },
            'interval': '1 day',
        })
        dsa.path.joinpath('ingest').mkdir()
        ipath = dsa.path.joinpath('ingest', 'foo.txt')
        ipath.write_text('hooray')
        os.utime(ipath, (10, 1578189722))
        with freeze_time('2020-03-05T010101Z'):
            ids, errs = dsa.process()
        assert ids == ['2020-01-05T020202Z', '2020-03-05T010101Z']
        assert not errs
        assert not ipath.exists()
        pas = dsa.parcel_accessors()
        assert [p.parcel_id for p in pas] == ids
        assert pas[0].find_data().read_text() == 'hooray'
        assert pas[1].find_data().read_text() == 'yay\n'
        metrics = dsa.read_metrics()
        assert all([i in metrics for i in ids])


def test_process_ingest_obviates_export():
    with tempdatasetdir() as dsa:
        dsa.write_config({
            'cmd': 'echo yay > $PARCEL_PATH.txt',
            'ingest': {
                'paths': 'ingest/*.txt',
                'time_source': 'mtime',
            },
            'interval': '1 day',
        })
        dsa.path.joinpath('ingest').mkdir()
        ipath = dsa.path.joinpath('ingest', 'foo.txt')
        ipath.write_text('hooray')
        os.utime(ipath, (10, 1578189722))
        with freeze_time('2020-01-06T010101Z'):
            ids, errs = dsa.process()
        assert ids == ['2020-01-05T020202Z']
        assert not errs
        assert not ipath.exists()
        pas = dsa.parcel_accessors()
        assert [p.parcel_id for p in pas] == ids
        assert pas[0].find_data().read_text() == 'hooray'
        metrics = dsa.read_metrics()
        assert all([i in metrics for i in ids])


def test_process_not_due():
    with tempdatasetdir() as dsa:
        dsa.write_config({
            'cmd': 'echo yay > $PARCEL_PATH.txt',
            'interval': '1 day',
        })
        dsa.data_path.joinpath('2020-01-01T010101Z.txt').write_text('ok')
        with freeze_time('2020-01-02T000000Z'):
            ids, errs = dsa.process()
        assert not ids
        assert not errs


def test_process_git():
    with tempdatasetdir(git=True) as dsa:
        dsa.write_config({
            'cmd': 'echo yay > $PARCEL_PATH.txt',
            'git': True,
            'ingest': {
                'paths': 'ingest/*.txt',
                'time_source': 'mtime',
            },
            'interval': '1 day',
        })
        dsa.path.joinpath('ingest').mkdir()
        ipath = dsa.path.joinpath('ingest', 'foo.txt')
        ipath.write_text('hooray')
        os.utime(ipath, (10, 1578189722))
        with freeze_time('2020-03-05T010101Z'):
            ids, errs = dsa.process()
        assert ids == ['2020-01-05T020202Z', '2020-03-05T010101Z']
        assert not errs
        repo = Repo(str(dsa.path))
        assert (repo.head.commit.message ==
                '[export_manager] process new parcels: 2020-01-05T020202Z, '
                '2020-03-05T010101Z\n2020-01-05T020202Z was ingested from '
                + str(ipath))
        assert (list(repo.head.commit.stats.files.keys())
                == [f'data/{i}.txt' for i in ids] + ['metrics.csv'])
