from contextlib import contextmanager
from datetime import datetime
from datetime import timedelta
from git import Repo
from pathlib import Path
import pytest
from tempfile import TemporaryDirectory
from export_manager import dataset
from export_manager.dataset import DatasetDir


@contextmanager
def tempdatasetdir(git=False):
    with TemporaryDirectory() as rawpath:
        dsd = DatasetDir(rawpath)
        dsd.initialize(git=git)
        yield dsd


def test_initialize():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath).joinpath('ds')
        dsd = DatasetDir(path)
        dsd.initialize()
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
        dsd = DatasetDir(path)
        dsd.initialize(git=True)
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
    with tempdatasetdir() as dsd:
        dsd.config_path.write_text('cmd = "echo hi"')
        assert dsd.read_config() == {'cmd': 'echo hi'}


def test_read_metrics_nonexistent():
    with tempdatasetdir() as dsd:
        dsd.metrics_path.unlink()
        assert dsd.read_metrics() == {}


def test_update_metrics():
    with tempdatasetdir() as dsd:
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
        dsd.update_metrics(initial)
        assert dsd.read_metrics() == initial

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
        dsd.update_metrics(updates)
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
        assert dsd.read_metrics() == expected


def test_run_export_no_cmd():
    with tempdatasetdir() as dsd:
        dsd.run_export(dataset.new_parcel_id())
        assert sum(1 for x in dsd.data_path.glob('*')) == 0
        assert sum(1 for x in dsd.incomplete_path.glob('*')) == 0


def test_run_export_no_data():
    with tempdatasetdir() as dsd:
        dsd.write_config({'cmd': 'echo hi && (echo muahaha >&2)'})
        with pytest.raises(Exception):
            dsd.run_export(dataset.new_parcel_id())
        assert sum(1 for x in dsd.data_path.glob('*')) == 0
        assert sum(1 for x in dsd.incomplete_path.glob('*')) == 0
        assert next(dsd.log_path.glob('*.out')).read_text() == 'hi\n'
        assert next(dsd.log_path.glob('*.err')).read_text() == 'muahaha\n'


def test_run_export():
    with tempdatasetdir() as dsd:
        dsd.write_config({'cmd': 'echo hi > $PARCEL_PATH.txt'})
        parcel_id = dataset.new_parcel_id()
        dsd.run_export(parcel_id)
        assert sum(1 for x in dsd.incomplete_path.glob('*')) == 0
        assert (dsd.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hi\n')
        assert dsd.log_path.joinpath(f'{parcel_id}.out').read_text() == ''
        assert dsd.log_path.joinpath(f'{parcel_id}.err').read_text() == ''


def test_clean_no_keep():
    with tempdatasetdir() as dsd:
        for i in range(10):
            dsd.data_path.joinpath(f'2000-01-02T03040{i}Z.txt').touch()
        dsd.clean()
        assert sum(1 for x in dsd.data_path.glob('*.txt')) == 10


def test_clean():
    with tempdatasetdir() as dsd:
        dsd.write_config({'keep': 4})
        dsd.incomplete_path.mkdir(exist_ok=True)
        dsd.log_path.mkdir(exist_ok=True)
        for i in range(10):
            p_id = f'2000-01-02T03040{i}Z'
            if i % 2 == 0:
                dsd.data_path.joinpath(f'{p_id}.txt').touch()
            else:
                dsd.incomplete_path.joinpath(f'{p_id}.txt').touch()
            if i % 3 == 0:
                dsd.log_path.joinpath(f'{p_id}.out').touch()
            if i % 4 == 0:
                dsd.log_path.joinpath(f'{p_id}.err').touch()
        dsd.clean()
        assert (list(p.name for p in dsd.data_path.glob('*'))
                == ['2000-01-02T030406Z.txt', '2000-01-02T030408Z.txt'])
        assert (list(p.name for p in dsd.incomplete_path.glob('*'))
                == ['2000-01-02T030407Z.txt', '2000-01-02T030409Z.txt'])
        assert (list(p.name for p in dsd.log_path.glob('*.out'))
                == ['2000-01-02T030406Z.out', '2000-01-02T030409Z.out'])
        assert (list(p.name for p in dsd.log_path.glob('*.err'))
                == ['2000-01-02T030408Z.err'])


def test_clean_git():
    with tempdatasetdir(git=True) as dsd:
        dsd.write_config({'keep': 4, 'git': True})
        dsd.incomplete_path.mkdir(exist_ok=True)
        dsd.log_path.mkdir(exist_ok=True)
        repo = Repo(str(dsd.path))
        for i in range(10):
            p_id = f'2000-01-02T03040{i}Z'
            dsd.data_path.joinpath(f'{p_id}.txt').touch()
            dsd.incomplete_path.joinpath(f'{p_id}.txt').touch()
            dsd.log_path.joinpath(f'{p_id}.out').touch()
            dsd.log_path.joinpath(f'{p_id}.err').touch()
        repo.index.add(['data'])
        repo.index.commit('commit data for test')
        assert len(repo.head.commit.tree['data'].blobs) == 10
        dsd.clean()
        assert (dsd.find_parcel_ids() ==
                ['2000-01-02T030406Z',
                 '2000-01-02T030407Z',
                 '2000-01-02T030408Z',
                 '2000-01-02T030409Z'])
        assert len(repo.head.commit.tree['data'].blobs) == 4


def test_collect_metrics_no_data():
    with tempdatasetdir() as dsd:
        pid = '2000-01-02T030405Z'
        assert dsd.collect_metrics(pid) == {'parcel_id': pid, 'success': 'N'}


def test_collect_metrics_incomplete():
    with tempdatasetdir() as dsd:
        pid = '2000-01-02T030405Z'
        dsd.incomplete_path.mkdir(exist_ok=True)
        dsd.incomplete_path.joinpath(f'{pid}.txt').write_text('hello')
        assert dsd.collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'N',
            'bytes': '5',
            'files': '1',
        }


def test_collect_metrics_complete():
    with tempdatasetdir() as dsd:
        pid = '2000-01-02T030405Z'
        dsd.data_path.joinpath(f'{pid}.txt').write_text('hello')
        assert dsd.collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'Y',
            'bytes': '5',
            'files': '1',
        }


def test_collect_metrics_custom():
    with tempdatasetdir() as dsd:
        pid = '2000-01-02T030405Z'
        dsd.data_path.joinpath(f'{pid}.txt').write_text('hello\nhi\nhola\n')
        dsd.write_config(
            {'metrics': {'lines': {'cmd': 'wc -l < $PARCEL_PATH'}}})
        assert dsd.collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'Y',
            'bytes': '14',
            'files': '1',
            'lines': '3',
        }


def test_collect_metrics_error():
    with tempdatasetdir() as dsd:
        pid = '2000-01-02T030405Z'
        dsd.data_path.joinpath(f'{pid}.txt').write_text('hello\nhi\nhola\n')
        dsd.write_config(
            {'metrics': {'lines': {'cmd': 'wc -l < $PARCEL_PATH.oops'}}})
        assert dsd.collect_metrics(pid) == {
            'parcel_id': pid,
            'success': 'Y',
            'bytes': '14',
            'files': '1',
            'lines': 'ERROR',
        }


def test_is_due_no_interval():
    with tempdatasetdir() as dsd:
        assert not dsd.is_due()


def test_is_due_no_parcels():
    with tempdatasetdir() as dsd:
        dsd.write_config({'interval': '1000 days'})
        assert dsd.is_due()


def test_is_due_false():
    with tempdatasetdir() as dsd:
        dsd.write_config({'interval': '1 hour'})
        old = datetime.strptime('2000-01-02T030405Z', '%Y-%m-%dT%H%M%S%z')
        now = datetime.now(old.tzinfo)
        last = now - timedelta(minutes=30)
        for dt in [old, last]:
            pid = dt.strftime('%Y-%m-%dT%H%M%SZ')
            dsd.data_path.joinpath(f'{pid}.txt').touch()
        assert not dsd.is_due()


def test_is_due_true():
    with tempdatasetdir() as dsd:
        dsd.write_config({'interval': '1 hour'})
        old = datetime.strptime('2000-01-02T030405Z', '%Y-%m-%dT%H%M%S%z')
        now = datetime.now(old.tzinfo)
        last = now - timedelta(minutes=57)  # within the 5-minute margin
        for dt in [old, last]:
            pid = dt.strftime('%Y-%m-%dT%H%M%SZ')
            dsd.data_path.joinpath(f'{pid}.txt').touch()
        assert dsd.is_due()
