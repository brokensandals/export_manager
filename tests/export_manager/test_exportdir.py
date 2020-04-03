import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

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


if __name__ == '__main__':
    unittest.main()
