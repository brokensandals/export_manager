from freezegun import freeze_time
from git import Repo
from pathlib import Path
from tempfile import TemporaryDirectory
from export_manager import cli


def test_clean():
    with TemporaryDirectory() as rawpath:
        assert cli.main(['init', rawpath]) == 0
        path = Path(rawpath)
        path.joinpath('config.toml').write_text('keep = 5')
        ids = [f'2001-01-0{i}T000000Z' for i in range(10)]
        for pid in ids:
            path.joinpath('data', f'{pid}.txt').touch()
        assert cli.main(['clean', rawpath]) == 0
        assert sum(1 for p in path.glob('data/*.txt')) == 5


def test_export():
    with TemporaryDirectory() as rawpath:
        assert cli.main(['init', rawpath]) == 0
        path = Path(rawpath)
        path.joinpath('config.toml').write_text(
            'cmd = "echo hello > $PARCEL_PATH.txt"')
        with freeze_time('2020-04-01T010203Z'):
            assert cli.main(['export', rawpath]) == 0
        assert (path.joinpath('data', '2020-04-01T010203Z.txt').read_text()
                == 'hello\n')


def test_export_given_id():
    with TemporaryDirectory() as rawpath:
        assert cli.main(['init', rawpath]) == 0
        path = Path(rawpath)
        path.joinpath('config.toml').write_text(
            'cmd = "echo hello > $PARCEL_PATH.txt"')
        assert cli.main(['export', '-p', '2015-01-02T030405Z', rawpath]) == 0
        assert (path.joinpath('data', '2015-01-02T030405Z.txt').read_text()
                == 'hello\n')


def test_init():
    with TemporaryDirectory() as rawpath:
        path1 = Path(rawpath).joinpath('alpha')
        path2 = Path(rawpath).joinpath('beta')
        assert cli.main(['init', str(path1), str(path2)]) == 0
        assert path1.is_dir()
        assert path1.joinpath('config.toml').is_file()
        assert path1.joinpath('metrics.csv').is_file()
        assert path2.is_dir()
        assert path2.joinpath('config.toml').is_file()
        assert path2.joinpath('metrics.csv').is_file()


def test_init_git():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath).joinpath('gamma')
        assert cli.main(['init', '-g', str(path)]) == 0
        assert path.is_dir()
        assert path.joinpath('config.toml').is_file()
        assert path.joinpath('metrics.csv').is_file()
        assert path.joinpath('.gitignore').is_file()
        repo = Repo(str(path))
        assert repo.head.commit.message == '[export_manager] initialize'


def test_process():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath)
        config = """
        interval = "1 day"
        cmd = "touch $PARCEL_PATH.txt"
        """
        path.joinpath('config.toml').write_text(config)
        times = [f'2000-01-0{d+1}T000000Z' for d in range(9)]
        for time in times:
            with freeze_time(time):
                assert cli.main(['process', rawpath]) == 0
        assert sorted([f.stem for f in path.glob('data/*.txt')]) == times
        assert (len(path.joinpath('metrics.csv').read_text().splitlines())
                == 10)

        # don't produce another export before it's due
        with freeze_time('2000-01-09T120000Z'):
            assert cli.main(['process', rawpath]) == 0
        assert sorted([f.stem for f in path.glob('data/*.txt')]) == times

        # do clean old data even when no export is due
        config += 'keep = 3'
        path.joinpath('config.toml').write_text(config)
        with freeze_time('2000-01-09T120000Z'):
            assert cli.main(['process', rawpath]) == 0
        assert sorted([f.stem for f in path.glob('data/*.txt')]) == times[6:]


def test_reprocess_metrics():
    with TemporaryDirectory() as rawpath:
        assert cli.main(['init', rawpath]) == 0
        path = Path(rawpath)
        path.joinpath('data', '2000-01-02T030405Z.txt').write_text('hi!')
        path.joinpath('data', '2000-01-03T030405Z.txt').write_text('hola!')
        assert cli.main(['reprocess_metrics', rawpath]) == 0
        expected = """parcel_id,success,files,bytes
2000-01-02T030405Z,Y,1,3
2000-01-03T030405Z,Y,1,5
"""
        assert path.joinpath('metrics.csv').read_text() == expected


def test_reprocess_metrics_given_parcel():
    with TemporaryDirectory() as rawpath:
        assert cli.main(['init', rawpath]) == 0
        path = Path(rawpath)
        path.joinpath('data', '2000-01-02T030405Z.txt').write_text('hi!')
        path.joinpath('data', '2000-01-03T030405Z.txt').write_text('hola!')
        assert (cli.main(['reprocess_metrics', '-p', '2000-01-02T030405Z',
                          rawpath])
                == 0)
        expected = """parcel_id,success,files,bytes
2000-01-02T030405Z,Y,1,3
"""
        assert path.joinpath('metrics.csv').read_text() == expected
