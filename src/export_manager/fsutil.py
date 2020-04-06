def total_size_bytes(path):
    """Given a pathlib.Path instance referring to a file or directory,
    returns the total size in bytes of the file or all descendants.
    """
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child
               in path.glob('**/*')
               if child.is_file())


def total_file_count(path):
    """Given a pathlib.Path instance referring to a file or directory,
    returns the total number of descendant files.
    """
    if path.is_file():
        return 1
    return sum(1 for child in path.glob('**/*') if child.is_file())
