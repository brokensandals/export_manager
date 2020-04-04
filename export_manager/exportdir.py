from git import Repo
from pathlib import Path
import re
import toml
import shutil
from datetime import datetime
from datetime import timedelta
import subprocess
from .interval import parse_delta

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

    def is_due(self, margin=timedelta(minutes=5)):
        """Returns true if an interval is defined in config.toml and that
        interval has passed since the most recent export.
        The margin param is subtracted from the interval, so that this method
        will return true if the export is at least close to being due.
        """
        interval_str = self.get_config().get('interval', None)
        if not interval_str:
            return False
        interval = parse_delta(interval_str) - margin
        versions = self.get_versions()
        if len(versions) == 0:
            return True
        last = datetime.strptime(versions[-1], '%Y-%m-%dT%H%M%S%z')
        now = datetime.now(last.tzinfo)
        return (now - last) >= interval

    def do_export(self):
        """Runs the export using the exportcmd defined in config.toml.
        Raises an exception if the command is missing, fails, or does not
        write data to the expected location. If git=true in config.toml,
        the new data will be committed to git.
        """
        cfg = self.get_config()
        cmd = cfg.get('exportcmd', '')
        if not cmd:
            raise Exception('exportcmd is not defined in config.toml')
        ver = datetime.utcnow().strftime('%Y-%m-%dT%H%M%SZ')
        env = {'EXPORT_DEST': str(self.data_path.joinpath(ver)),
               'EXPORT_ROOT': str(self.path)}
        subprocess.check_call(cmd, shell=True, env=env)
        verpath = self.get_version_path(ver)
        if not verpath:
            raise Exception(f'export did not produce data in {verpath}')

        if cfg.get('git', False):
            repo = Repo(self.path)
            index = repo.index
            index.add(str(verpath))
            index.commit(f'[export-manager] add data version {ver}')


class ExportDirSet:
    def __init__(self, path):
        self.path = Path(path)

    def get_dirs(self):
        configs = self.path.glob('*/config.toml')
        return [ExportDir(p.parent) for p in configs]

    def process_all(self):
        errors = []
        for exdir in self.get_dirs():
            if exdir.is_due():
                try:
                    exdir.do_export()
                except Exception as e:
                    errors.append(('export', exdir, e))
            try:
                exdir.clean()
            except Exception as e:
                errors.append(('clean', exdir, e))
        return errors
