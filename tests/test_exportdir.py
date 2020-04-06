import unittest
from git import Repo
from tempfile import TemporaryDirectory
from pathlib import Path
from datetime import datetime
from datetime import timedelta

from export_manager.exportdir import ExportDir
from export_manager.exportdir import ExportDirSet


class ExportDirTests(unittest.TestCase):
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
            self.assertEqual(commit.message, '[export_manager] delete ' +
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

    def test_do_export_no_cmd(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            self.assertRaises(Exception, exdir.do_export)

    def test_do_export_no_data(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            Path(path).joinpath('config.toml').write_text('exportcmd = "echo hi > /dev/null"')
            self.assertRaises(Exception, exdir.do_export)

    def test_do_export(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            Path(path).joinpath('config.toml')\
                .write_text('exportcmd = "echo hi from $EXPORT_DIR ' +
                            '> $EXPORT_DEST.txt"')
            exdir.do_export()
            vers = exdir.get_versions()
            self.assertEqual(len(vers), 1)
            verpath = exdir.get_version_path(vers[0])
            self.assertEqual(verpath.read_text().strip(), f'hi from {path}')

    def test_do_export_git(self):
        with TemporaryDirectory() as path:
            exdir = ExportDir(path)
            exdir.initialize()
            Path(path).joinpath('config.toml').write_text(
                'exportcmd = "mkdir $EXPORT_DEST; echo hi > $EXPORT_DEST/foo.txt"\n' +
                'git = true'
            )
            repo = Repo.init(path)
            exdir.do_export()
            vers = exdir.get_versions()
            self.assertEqual(len(vers), 1)
            verpath = exdir.get_version_path(vers[0])
            self.assertEqual(verpath.joinpath('foo.txt').read_text().strip(), 'hi')
            commit = repo.head.commit
            self.assertEqual(commit.message, '[export_manager] add data ' +
                             f'version {vers[0]}')
            self.assertEqual(len(commit.tree), 1)

    def test_collect_metrics_default(self):
        with TemporaryDirectory() as rawpath:
            exdir = ExportDir(rawpath)
            exdir.initialize()
            path = Path(rawpath)
            ver = '2000-01-02T030405Z'
            path.joinpath('data', f'{ver}.txt').write_text('Hello world!')
            expected = {'version': ver, 'bytes': '12', 'files': '1'}
            self.assertEqual(exdir.collect_metrics(ver), expected)

    def test_collect_metrics_custom(self):
        with TemporaryDirectory() as rawpath:
            exdir = ExportDir(rawpath)
            exdir.initialize()
            path = Path(rawpath)
            path.joinpath('config.toml')\
                .write_text('metrics.words.cmd = ' +
                            '"cat $EXPORT_PATH | wc -w"')
            ver = '2000-01-02T030405Z'
            path.joinpath('data', f'{ver}.txt').write_text('Hello world!')
            expected = {'version': ver, 'bytes': '12', 'files': '1',
                        'words': '2'}
            self.assertEqual(exdir.collect_metrics(ver), expected)

    def test_read_metrics_and_save_metrics_row(self):
        with TemporaryDirectory() as rawpath:
            exdir = ExportDir(rawpath)
            exdir.initialize()
            path = Path(rawpath)
            content = """version,bytes,foo,bar
2000-01-02T030405Z,12,,9000
2000-01-03T010101Z,55,80,"""
            path.joinpath('metrics.csv').write_text(content)
            expected = {
                '2000-01-02T030405Z': {
                    'version': '2000-01-02T030405Z',
                    'bytes': '12',
                    'foo': '',
                    'bar': '9000',
                },
                '2000-01-03T010101Z': {
                    'version': '2000-01-03T010101Z',
                    'bytes': '55',
                    'foo': '80',
                    'bar': '',
                },
            }
            self.assertEqual(exdir.read_metrics(), expected)

            row = {'version': '2020-10-10T121212Z',
                   'bytes': '101',
                   'baz': '301',
                   'bar': '201'}
            exdir.save_metrics_row(row)

            expected['2000-01-02T030405Z']['baz'] = ''
            expected['2000-01-03T010101Z']['baz'] = ''
            row['foo'] = ''
            expected[row['version']] = row
            self.assertEqual(exdir.read_metrics(), expected)

            expected_c = """version,bytes,foo,bar,baz
2000-01-02T030405Z,12,,9000,
2000-01-03T010101Z,55,80,,
2020-10-10T121212Z,101,,201,301
"""
            self.assertEqual(path.joinpath('metrics.csv').read_text(),
                             expected_c)


class ExportDirSetTests(unittest.TestCase):
    def test_get_dirs(self):
        with TemporaryDirectory() as path:
            rpath = Path(path).joinpath('real', 'config.toml')
            rpath.parent.mkdir()
            rpath.touch()
            cpath = Path(path).joinpath('bogus', 'whatever.txt')
            cpath.parent.mkdir()
            cpath.touch()
            dirs = ExportDirSet(path).get_dirs()
            self.assertEqual([d.path for d in dirs], [rpath.parent])


if __name__ == '__main__':
    unittest.main()
