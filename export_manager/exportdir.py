from git import Repo
from pathlib import Path
import re
import toml
import shutil

VERSION_FORMAT = re.compile('\\A\\d{4}-\\d{2}-\\d{2}T\\d{6}Z\\Z')


class ExportDir:
    """Provides access to an export directory, which should have the
    the following structure:

    name-of-service/
        config.toml
        metrics.csv
        data/
            2019-01-02T030405Z.json # file can have any extension, or may be a directory
            2019-02-03T040506Z.json
            # ...
        screenshots/
            2019-01-02T030405Z.png # file can be any type of image, or be a directory of images
            2019-02-03T040506Z.png
    """

    def __init__(self, path):
        self.path = Path(path)
        self.config_path = self.path.joinpath('config.toml')
        self.data_path = self.path.joinpath('data')
        self.metrics_path = self.path.joinpath('metrics.csv')

    def is_valid(self):
        return self.path.is_dir() and self.config_path.is_file()

    def initialize(self):
        """Creates any of the following that don't already exist:
        - the directory
        - config.toml
        - data/ dir
        """
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)
        if not self.config_path.exists():
            self.config_path.touch()
        if not self.data_path.exists():
            self.data_path.mkdir()

    def get_versions(self):
        """Returns a list of data versions that exist in the directory."""
        vers = [path.stem for path
                in self.data_path.glob('*')
                if VERSION_FORMAT.match(path.stem)]
        return sorted(vers)

    def get_version_path(self, version):
        """Returns the Path of the directory or file for the given data
        version, or None.
        """
        if not VERSION_FORMAT.match(version):
            return None
        return next((path for path
                     in self.data_path.glob(version + '*')
                     if path.stem == version),
                    None)

    def get_config(self):
        """Returns a dictionary containing the contents of config.toml;
        returns an empty dict if config.toml is missing.
        """
        if not self.config_path.exists():
            return {}
        return toml.load(self.config_path)

    def delete_version(self, version):
        """Deletes the data for the given version. If git=true in config.toml,
        also makes a commit removing the data.
        """
        verpath = self.get_version_path(version)
        if not verpath:
            return

        cfg = self.get_config()
        if cfg.get('git', False):
            repo = Repo(self.path)
            index = repo.index
            index.remove(str(verpath), r=True)
            # TODO delete screenshots/etc
            index.commit(f'[export-manager] delete version {version}')

        if verpath.is_file():
            verpath.unlink()
        elif verpath.is_dir():
            shutil.rmtree(verpath)

    def clean(self):
        """Calls delete_version for outdated versions. This looks at the
        'keep' config in config.toml: if it's a positive integer, the oldest
        versions will be deleted until only that number are left. Otherwise,
        this method does nothing.
        """
        cfg = self.get_config()
        if cfg.get('keep', 0) > 0:
            vers = self.get_versions()
            while len(vers) > cfg['keep']:
                self.delete_version(vers[0])
                del vers[0]
