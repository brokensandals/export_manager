from contextlib import contextmanager
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
        dsd.run_export()
        assert sum(1 for x in dsd.data_path.glob('*')) == 0
        assert sum(1 for x in dsd.incomplete_path.glob('*')) == 0


def test_run_export_no_data():
    with tempdatasetdir() as dsd:
        dsd.write_config({'cmd': 'echo hi && (echo muahaha >&2)'})
        with pytest.raises(Exception):
            dsd.run_export()
        assert sum(1 for x in dsd.data_path.glob('*')) == 0
        assert sum(1 for x in dsd.incomplete_path.glob('*')) == 0
        assert next(dsd.log_path.glob('*.out')).read_text() == 'hi\n'
        assert next(dsd.log_path.glob('*.err')).read_text() == 'muahaha\n'


def test_run_export():
    with tempdatasetdir() as dsd:
        dsd.write_config({'cmd': 'echo hi > $PARCEL_DEST.txt'})
        parcel_id = dsd.run_export()
        assert sum(1 for x in dsd.incomplete_path.glob('*')) == 0
        assert (dsd.data_path.joinpath(f'{parcel_id}.txt').read_text()
                == 'hi\n')
        assert dsd.log_path.joinpath(f'{parcel_id}.out').read_text() == ''
        assert dsd.log_path.joinpath(f'{parcel_id}.err').read_text() == ''
