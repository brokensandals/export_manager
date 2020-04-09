from git import Repo
from pathlib import Path
from tempfile import TemporaryDirectory
from export_manager import cli


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





