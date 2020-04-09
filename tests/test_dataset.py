from contextlib import contextmanager
from datetime import datetime
from datetime import timedelta
from git import Repo
from pathlib import Path
import pytest
from tempfile import TemporaryDirectory
from export_manager import dataset
from export_manager.dataset import DatasetAccessor


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
                == dataset.INITIAL_METRICS_CSV)
        assert (path.joinpath('config.toml').read_text()
                == dataset.DEFAULT_CONFIG_TOML)
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
                == dataset.DEFAULT_GITIGNORE)
        assert repo.head.commit.message == '[export_manager] initialize'
        assert (sorted([b.name for b in repo.head.commit.tree.blobs])
                == ['.gitignore', 'config.toml', 'metrics.csv'])
        assert (path.joinpath('config.toml').read_text()
                == dataset.DEFAULT_CONFIG_TOML + 'git = true\n')


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
        dsa.update_metrics(initial)
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
        dsa.update_metrics(updates)
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
        dsa.run_export(dataset.new_parcel_id())
        assert sum(1 for x in dsa.data_path.glob('*')) == 0
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0


def test_run_export_no_data():
    with tempdatasetdir() as dsa:
        dsa.write_config({'cmd': 'echo hi && (echo muahaha >&2)'})
        with pytest.raises(Exception):
            dsa.run_export(dataset.new_parcel_id())
        assert sum(1 for x in dsa.data_path.glob('*')) == 0
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert next(dsa.log_path.glob('*.out')).read_text() == 'hi\n'
        assert next(dsa.log_path.glob('*.err')).read_text() == 'muahaha\n'


def test_run_export():
    with tempdatasetdir() as dsa:
        dsa.write_config({'cmd': 'echo hi > $PARCEL_PATH.txt'})
        parcel_id = dataset.new_parcel_id()
        dsa.run_export(parcel_id)
        assert sum(1 for x in dsa.incomplete_path.glob('*')) == 0
        assert (dsa.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hi\n')
        assert dsa.log_path.joinpath(f'{parcel_id}.out').read_text() == ''
        assert dsa.log_path.joinpath(f'{parcel_id}.err').read_text() == ''


def test_clean_no_keep():
    with tempdatasetdir() as dsa:
        for i in range(10):
            dsa.data_path.joinpath(f'2000-01-02T03040{i}Z.txt').touch()
        dsa.clean()
        assert sum(1 for x in dsa.data_path.glob('*.txt')) == 10


def test_clean():
    with tempdatasetdir() as dsa:
        dsa.write_config({'keep': 4})
        dsa.incomplete_path.mkdir(exist_ok=True)
        dsa.log_path.mkdir(exist_ok=True)
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
        dsa.incomplete_path.mkdir(exist_ok=True)
        dsa.log_path.mkdir(exist_ok=True)
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
        assert dsa.collect_metrics(pid) == {'parcel_id': pid, 'success': 'N'}


def test_collect_metrics_incomplete():
    with tempdatasetdir() as dsa:
        pid = '2000-01-02T030405Z'
        dsa.incomplete_path.mkdir(exist_ok=True)
        dsa.incomplete_path.joinpath(f'{pid}.txt').write_text('hello')
        assert dsa.collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'N',
            'bytes': '5',
            'files': '1',
        }


def test_collect_metrics_complete():
    with tempdatasetdir() as dsa:
        pid = '2000-01-02T030405Z'
        dsa.data_path.joinpath(f'{pid}.txt').write_text('hello')
        assert dsa.collect_metrics(pid) == {
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
        assert dsa.collect_metrics(pid) == {
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
        assert dsa.collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'Y',
            'bytes': '14',
            'files': '1',
            'lines': 'ERROR',
        }


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


def test_parsel_accessors():
    with tempdatasetdir() as dsa:
        dsa.incomplete_path.mkdir(exist_ok=True)
        dsa.log_path.mkdir(exist_ok=True)

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
        assert parcels[0].is_complete()
        assert parcels[0].find_data() == data1
        assert parcels[0].find_incomplete() is None
        assert parcels[0].find_stdout() == out1
        assert parcels[0].find_stderr() == err1
        assert parcels[1].parcel_id == id2
        assert not parcels[1].is_complete()
        assert parcels[1].find_data() is None
        assert parcels[1].find_incomplete() == incomplete2
        assert parcels[1].find_stdout() is None
        assert parcels[1].find_stderr() is None
        assert parcels[2].parcel_id == id3
        assert parcels[2].is_complete()
        assert parcels[2].find_data() == data3
        assert parcels[2].find_incomplete() is None
        assert parcels[2].find_stdout() is None
        assert parcels[2].find_stderr() is None
        assert parcels[3].parcel_id == id4
        assert not parcels[3].is_complete()
        assert parcels[3].find_data() is None
        assert parcels[3].find_incomplete() is None
        assert parcels[3].find_stdout() is None
        assert parcels[3].find_stderr() == err4

