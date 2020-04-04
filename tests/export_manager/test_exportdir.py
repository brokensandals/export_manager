import unittest
from git import Repo
from tempfile import TemporaryDirectory
from pathlib import Path
from datetime import datetime
from datetime import timedelta

from ...export_manager.exportdir import ExportDir


class MyTestCase(unittest.TestCase):
    def test_nonexistent_dir(self):
        exdir = ExportDir('./bogusbogusbogus')
        self.assertFalse(exdir.is_valid())

    def test_no_config_toml(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            self.assertFalse(exdir.is_valid())

    def test_valid(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            Path(path).joinpath('config.toml').touch()
            self.assertTrue(exdir.is_valid())

    def test_initialize(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(Path(path).joinpath('inner'))
            self.assertFalse(exdir.is_valid())
            exdir.initialize()
            self.assertTrue(exdir.is_valid())

    def test_get_versions_no_data_dir(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            self.assertEqual(exdir.get_versions(), [])

    def test_get_versions(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            datadir = Path(path).joinpath('data')
            datadir.joinpath('notaversion.json').touch()
            datadir.joinpath('2000-01-02T030405Z.json').touch()
            datadir.joinpath('2000-06-07T080910Z').mkdir()
            self.assertEqual(exdir.get_versions(), ['2000-01-02T030405Z', '2000-06-07T080910Z'])

    def test_get_version_path_invalid(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            self.assertIsNone(exdir.get_version_path('invalid'))

    def test_get_version_nonexistent(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            self.assertIsNone(exdir.get_version_path('2000-01-02T030405Z'))

    def test_get_version(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            verpath = Path(path).joinpath('data', '2000-01-02T030405Z.json')
            verpath.touch()
            self.assertEqual(exdir.get_version_path('2000-01-02T030405Z'), verpath)

    def test_delete_version_nonexistent(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            exdir.delete_version("2000-01-02T030405Z") # should not raise error

    def test_delete_version_file(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            verpath = Path(path).joinpath('data', '2000-01-02T030405Z.json')
            verpath.touch()
            exdir.delete_version("2000-01-02T030405Z")
            self.assertFalse(verpath.exists())

    def test_delete_version_dir(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            verpath = Path(path).joinpath('data', '2000-01-02T030405Z')
            verpath.mkdir()
            verpath.joinpath('foo.json').touch()
            exdir.delete_version("2000-01-02T030405Z")
            self.assertFalse(verpath.exists())

    def test_delete_version_dir_git(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            Path(path).joinpath('config.toml').write_text('git = true')
            verpath = Path(path).joinpath('data', '2000-01-02T030405Z')
            verpath.mkdir()
            verpath.joinpath('foo.json').touch()
            repo = Repo.init(path)
            repo.index.add('data/2000-01-02T030405Z/foo.json')
            repo.index.commit('add file')
            exdir.delete_version("2000-01-02T030405Z")
            self.assertFalse(verpath.exists())
            commit = repo.head.commit
            self.assertEqual(commit.message, '[export-manager] delete ' +
                             'version 2000-01-02T030405Z')
            self.assertEqual(len(commit.tree), 0)
            self.assertEqual(len(commit.parents[0].tree), 1)

    def test_clean_no_keep(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            dpath = Path(path).joinpath('data')
            for i in range(10):
                dpath.joinpath(f'2000-01-02T03040{i}Z.json').touch()
            exdir.clean()
            self.assertEqual(sum(1 for p in dpath.glob('*.json')), 10)

            Path(path).joinpath('config.toml').write_text('keep = 0')
            exdir.clean()
            self.assertEqual(sum(1 for p in dpath.glob('*.json')), 10)

            Path(path).joinpath('config.toml').write_text('keep = -1')
            exdir.clean()
            self.assertEqual(sum(1 for p in dpath.glob('*.json')), 10)

    def test_clean(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            dpath = Path(path).joinpath('data')
            verpaths = [dpath.joinpath(f'2000-01-02T03040{i}Z.json') for i
                        in range(10)]
            for verpath in verpaths:
                verpath.touch()
            Path(path).joinpath('config.toml').write_text('keep = 3')
            exdir.clean()
            self.assertEqual(sorted(dpath.glob('*.json')), verpaths[7:])

    def test_is_due_no_interval(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            self.assertFalse(exdir.is_due())

    def test_is_due_no_versions(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            Path(path).joinpath('config.toml').write_text('interval = "1000 weeks"')
            self.assertTrue(exdir.is_due())

    def test_is_due_no(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            Path(path).joinpath('config.toml').write_text('interval = "1 hour"')
            old = datetime.strptime('2000-01-02T030405Z', '%Y-%m-%dT%H%M%S%z')
            now = datetime.now(old.tzinfo)
            last = now - timedelta(minutes=30)
            Path(path).joinpath('data', '2000-01-02T030405Z.json').touch()
            Path(path).joinpath('data',
                                f'{last.strftime("%Y-%m-%dT%H%M%SZ")}.json')\
                .touch()
            self.assertFalse(exdir.is_due())

    def test_is_due_yes(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            Path(path).joinpath('config.toml').write_text('interval = "1 hour"')
            old = datetime.strptime('2000-01-02T030405Z', '%Y-%m-%dT%H%M%S%z')
            now = datetime.now(old.tzinfo)
            last = now - timedelta(minutes=57) # within the default 5-minute margin
            Path(path).joinpath('data', '2000-01-02T030405Z.json').touch()
            Path(path).joinpath('data',
                                f'{last.strftime("%Y-%m-%dT%H%M%SZ")}.json')\
                .touch()
            self.assertTrue(exdir.is_due())


if __name__ == '__main__':
    unittest.main()
