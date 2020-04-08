from export_manager.fsutil import total_size_bytes
from export_manager.fsutil import total_file_count
from pathlib import Path
from tempfile import TemporaryDirectory


def test_total_size_bytes_file():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath).joinpath('foo.txt')
        path.write_text('Hello world!')
        assert total_size_bytes(path) == 12


def test_total_size_bytes_dir():
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
        assert total_size_bytes(path) == expected


def test_total_file_count_file():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath).joinpath('foo.txt')
        path.write_text('Hello world!')
        assert total_file_count(path) == 1


def test_total_file_count_dir():
    with TemporaryDirectory() as rawpath:
        path = Path(rawpath)
        path.joinpath('f').write_text('hello')
        a = path.joinpath('a')
        a.mkdir()
        a.joinpath('f').write_text('how are you')
        b = a.joinpath('b')
        b.mkdir()
        b.joinpath('f1').write_text('good')
        b.joinpath('f2').write_text('bad')
        c = a.joinpath('c')
        c.mkdir()
        c.joinpath('f').write_text('indifferent')
        assert total_file_count(path) == 5
