def total_size_bytes(path):
    """Returns total size in bytes of specified file or directory.

    The path should be a pathlib.Path instance. It will be traversed
    recursively.
    """
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child
               in path.glob('**/*')
               if child.is_file())


def total_file_count(path):
    """Returns total number of files under specified path.

    The path should be a pathlib.Path instance referring to a file
    or directory. For a file this will return 1. Directories will be
    traversed recursively.
    """
    if path.is_file():
        return 1
    return sum(1 for child in path.glob('**/*') if child.is_file())
