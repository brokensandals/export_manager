from git import Repo
from pathlib import Path
import csv
import re
import toml
import shutil
from datetime import datetime
from datetime import timedelta
import subprocess
from .interval import parse_delta
from .fsutil import total_size_bytes
from .fsutil import total_file_count

VERSION_FORMAT = re.compile('\\A\\d{4}-\\d{2}-\\d{2}T\\d{6}Z\\Z')
SAMPLE_CONFIG_TOML = """# exportcmd = "echo hi"
# git = true
# interval = "1d"
# keep = 5
"""


def find_data_path(parent, version):
    matches = list(parent.glob(f'{version}*'))
    if not matches:
        return None
    if len(matches) > 1:
        raise Exception(
            f'multiple data files or dirs exist for {version} in {parent}')
    return matches[0]


class VersionStatus:
    def __init__(self, export_dir, version):
        self.version = version
        self.data_path = find_data_path(export_dir.data_path, version)
        self.incomplete_data_path = find_data_path(
            export_dir.incomplete_data_path, version)
        out = export_dir.log_path.joinpath(f'{version}.out')
        if out.is_file():
            self.out_path = out
        else:
            self.out_path = None
        err = export_dir.log_path.joinpath(f'{version}.err')
        if err.is_file():
            self.err_path = err
        else:
            self.err_path = None


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
        incomplete/
            2019-03-04T050607Z.json
        log/
            2019-01-02T030405Z.out
            2019-01-02T030405Z.err
            2019-02-03T040506Z.out
            2019-02-03T040506Z.err
            2019-03-04T050607Z.out
            2019-03-04T050607Z.err
    """

    def __init__(self, path):
        self.path = Path(path)
        self.config_path = self.path.joinpath('config.toml')
        self.data_path = self.path.joinpath('data')
        self.incomplete_data_path = self.path.joinpath('incomplete')
        self.log_path = self.path.joinpath('log')
        self.metrics_path = self.path.joinpath('metrics.csv')

    def is_valid(self):
        return self.path.is_dir() and self.config_path.is_file()

    def initialize(self):
        """Creates any of the following that don't already exist:
        - the directory
        - config.toml
        - data/ dir
        - incomplete/ dir
        - log/ dir
        """
        dirs = [self.path,
                self.data_path,
                self.incomplete_data_path,
                self.log_path]
        for path in dirs:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
        if not self.config_path.exists():
            self.config_path.write_text(SAMPLE_CONFIG_TOML)

    def version_status(self, version):
        if not VERSION_FORMAT.match(version):
            return None
        return VersionStatus(self, version)

    def all_version_statuses(self):
        dirs = [self.data_path, self.incomplete_data_path, self.log_path]
        vers = set()
        for path in dirs:
            vers.update(p.stem for p in path.glob('*')
                        if VERSION_FORMAT.match(p.stem))
        return [VersionStatus(self, v) for v in sorted(vers)]

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
        vs = self.version_status(version)

        if vs.data_path:
            cfg = self.get_config()
            if cfg.get('git', False):
                repo = Repo(self.path)
                index = repo.index
                index.remove(str(vs.data_path), r=True)
                index.commit(f'[export_manager] delete version {version}')

        paths = [vs.data_path, vs.incomplete_data_path,
                 vs.out_path, vs.err_path]
        for path in paths:
            if path and path.is_file():
                path.unlink()
            elif path and path.is_dir():
                shutil.rmtree(path)

    def clean(self):
        """Calls delete_version for outdated versions. This looks at the
        'keep' config in config.toml: if it's a positive integer, the oldest
        versions will be deleted until only that number are left. Otherwise,
        this method does nothing.
        """
        cfg = self.get_config()
        if cfg.get('keep', 0) > 0:
            vers = self.all_version_statuses()
            # TODO: prioritize complete versions, or allow separate
            # configuration of how many incomplete versions to keep
            while len(vers) > cfg['keep']:
                self.delete_version(vers[0].version)
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
        vers = self.all_version_statuses()
        if len(vers) == 0:
            return True
        last = datetime.strptime(vers[-1].version, '%Y-%m-%dT%H%M%S%z')
        now = datetime.now(last.tzinfo)
        return (now - last) >= interval

    def do_export(self):
        """Runs the export using the exportcmd defined in config.toml.
        Raises an exception if the command is missing, fails, or does not
        write data to the expected location. If git=true in config.toml,
        the new data will be committed to git.

        Returns the version identifier that was used.
        """
        cfg = self.get_config()
        cmd = cfg.get('exportcmd', '')
        if not cmd:
            raise Exception('exportcmd is not defined in config.toml')
        ver = datetime.utcnow().strftime('%Y-%m-%dT%H%M%SZ')
        dest = self.incomplete_data_path.joinpath(ver)
        env = {'EXPORT_DEST': str(dest),
               'EXPORT_DIR': str(self.path)}
        subprocess.check_call(cmd, shell=True, env=env)
        oldpath = find_data_path(self.incomplete_data_path, ver)
        if not oldpath:
            raise Exception(f'export did not produce data in {dest}*')
        newpath = self.data_path.joinpath(oldpath.name)
        oldpath.rename(newpath)

        if cfg.get('git', False):
            repo = Repo(self.path)
            index = repo.index
            index.add(str(newpath))
            if repo.is_dirty():
                index.commit(f'[export_manager] add data version {ver}')

        return ver

    def process(self):
        errors = []

        ver = None
        try:
            if self.is_due():
                ver = self.do_export()
        except Exception as e:
            errors.append(Exception(f'export failed: {self.path}', e))

        if ver:
            try:
                self.save_metrics_row(self.collect_metrics(ver))
            except Exception as e:
                errors.append(Exception(f'metrics update failed: {self.path}',
                                        e))

        try:
            self.clean()
        except Exception as e:
            errors.append(Exception(f'clean failed: {self.path}', e))

        return errors

    def collect_metrics(self, version):
        vs = self.version_status(version)
        path = vs.data_path
        if not path:
            raise ValueError(f'cannot find version data: {version}')

        metrics = {
            'version': version,
            'bytes': str(total_size_bytes(path)),
            'files': str(total_file_count(path)),
        }

        cfg = self.get_config()
        m_cfgs = cfg.get('metrics', {})
        for name in m_cfgs:
            cmd = m_cfgs[name]['cmd']
            env = {'EXPORT_PATH': str(path),
                   'EXPORT_DIR': str(self.path)}
            out = subprocess.check_output(cmd, shell=True, env=env)
            metrics[name] = str(out, 'utf-8').strip()

        return metrics

    def read_metrics(self):
        if not self.metrics_path.exists():
            return {}
        results = {}
        with open(self.metrics_path) as file:
            for row in csv.DictReader(file):
                ver = row['version']
                if ver in results:
                    raise Exception('version has multiple entries in ' +
                                    f'metrics.csv: {ver}')
                results[ver] = row
        return results

    def save_metrics_row(self, row):
        metrics = self.read_metrics()
        fields = list(row.keys())
        if metrics:
            existing_fields = next(iter(metrics.values())).keys()
            fields = list(existing_fields)
            for field in row.keys():
                if field not in existing_fields:
                    fields.append(field)
        metrics[row['version']] = row
        with open(self.metrics_path, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(metrics.values())

        cfg = self.get_config()
        if cfg.get('git', False):
            repo = Repo(self.path)
            index = repo.index
            index.add(str(self.metrics_path))
            if repo.is_dirty():
                index.commit(f'[export_manager] update metrics')


class ExportDirSet:
    def __init__(self, path):
        self.path = Path(path)

    def get_dirs(self):
        configs = self.path.glob('*/config.toml')
        return [ExportDir(p.parent) for p in configs]
