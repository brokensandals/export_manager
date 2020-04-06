import unittest
from export_manager.fsutil import total_size_bytes
from pathlib import Path
from tempfile import TemporaryDirectory


class FSUtilTests(unittest.TestCase):
    def test_total_size_bytes_file(self):
        with TemporaryDirectory() as rawpath:
            path = Path(rawpath).joinpath('foo.txt')
            path.write_text('Hello world!')
            self.assertEqual(total_size_bytes(path), 12)

    def test_total_size_bytes_dir(self):
        with TemporaryDirectory() as rawpath:
            path = Path(rawpath)
            path.joinpath('f').write_text('hello')  # 5
            a = path.joinpath('a')
            a.mkdir()
            a.joinpath('f').write_text('how are you')  # 11
            b = a.joinpath('b')
            b.mkdir()
            b.joinpath('f1').write_text('good')  # 4
            b.joinpath('f2').write_text('bad')  # 3
            c = a.joinpath('c')
            c.mkdir()
            c.joinpath('f').write_text('indifferent')  # 11
            expected = 5 + 11 + 4 + 3 + 11
            self.assertEqual(total_size_bytes(path), expected)


if __name__ == '__main__':
    unittest.main()
