import csv
from datetime import datetime
from datetime import timedelta
from git import Repo
from operator import itemgetter
from pathlib import Path
import re
import shutil
import subprocess
import sys
import toml
from export_manager import fsutil
from export_manager import interval


DEFAULT_GITIGNORE = """.DS_Store
incomplete/
log/
/secrets*
"""

DEFAULT_CONFIG_TOML = """# cmd = "echo example > $PARCEL_DEST.txt"
# keep = 5
# interval = "1 day"
"""

INITIAL_METRICS_CSV = "parcel_id,success,files,bytes"

PARCEL_ID_FORMAT = re.compile('\\A\\d{4}-\\d{2}-\\d{2}T\\d{6}Z\\Z')


def find_parcel_data_path(parent, parcel_id):
    matches = list(parent.glob(f'{parcel_id}*'))
    if not matches:
        return None
    if len(matches) > 1:
        raise Exception(
            f'multiple data files or dirs exist for {parcel_id} in {parent}')
    return matches[0]


class DatasetDir:
    def __init__(self, path):
        self.path = Path(path)
        self.config_path = self.path.joinpath('config.toml')
        self.data_path = self.path.joinpath('data')
        self.incomplete_path = self.path.joinpath('incomplete')
        self.log_path = self.path.joinpath('log')
        self.metrics_path = self.path.joinpath('metrics.csv')

    def initialize(self, *, git=False):
        self.path.mkdir(exist_ok=True, parents=True)
        self.data_path.mkdir(exist_ok=True)

        if not self.config_path.exists():
            config = DEFAULT_CONFIG_TOML
            if git:
                config += 'git = true\n'
            self.config_path.write_text(config)

        if not self.metrics_path.exists():
            self.metrics_path.write_text(INITIAL_METRICS_CSV)

        if git:
            if not self.path.joinpath('.git').exists():
                Repo.init(str(self.path))
            gitignore_path = self.path.joinpath('.gitignore')
            if not gitignore_path.exists():
                gitignore_path.write_text(DEFAULT_GITIGNORE)
            self.commit('[export_manager] initialize',
                        ['.gitignore', 'config.toml', 'metrics.csv'])

    def read_config(self):
        if not self.config_path.is_file():
            raise Exception(f'{self.config_path} is not a file')
        return toml.load(self.config_path)

    def write_config(self, cfg):
        with open(self.config_path, 'w') as file:
            toml.dump(cfg, file)

    def is_git(self):
        return self.read_config().get('git', False)

    def commit(self, message, add=[], *, rm=[]):
        if self.is_git():
            repo = Repo(str(self.path))
            if add:
                repo.index.add(add)
            if rm:
                repo.index.remove(rm, r=True)
            if repo.is_dirty():
                repo.index.commit(message)

    def read_metrics(self):
        if not self.metrics_path.is_file():
            return {}

        results = {}
        with open(self.metrics_path) as file:
            for row in csv.DictReader(file):
                parcel_id = row['parcel_id']
                if parcel_id in results:
                    raise Exception('parcel_id appears multiple times in '
                                    + f'metrics.csv: {parcel_id}')
                results[parcel_id] = row
        return results

    def update_metrics(self, updates):
        metrics = self.read_metrics()
        fields = set()
        if metrics:
            fields.update(next(iter(metrics.values())).keys())
        for row in updates.values():
            fields.update(row.keys())
            metrics[row['parcel_id']] = row
        rows = sorted(metrics.values(), key=itemgetter('parcel_id'))

        with open(self.metrics_path, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

        self.commit('[export_manager] update metrics', ['metrics.csv'])

    def collect_metrics(self, parcel_id):
        results = {'parcel_id': parcel_id}
        path = find_parcel_data_path(self.data_path, parcel_id)
        if path:
            results['success'] = 'Y'
        else:
            results['success'] = 'N'
            path = find_parcel_data_path(self.incomplete_path, parcel_id)

        if path:
            results['bytes'] = str(fsutil.total_size_bytes(path))
            results['files'] = str(fsutil.total_file_count(path))

            cfg = self.read_config()
            for name in cfg.get('metrics', {}):
                cmd = cfg['metrics'][name]['cmd']
                env = {'PARCEL_PATH': str(path),
                       'DATASET_PATH': str(self.path)}
                try:
                    out = subprocess.check_output(cmd, shell=True, env=env)
                    results[name] = str(out, 'utf-8').strip()
                except Exception as e:
                    results[name] = 'ERROR'
                    print(f'metric {name} failed for {path}', file=sys.stderr)
                    print(e, file=sys.stderr)

        return results

    def run_export(self):
        cfg = self.read_config()
        cmd = cfg.get('cmd', None)
        if not cmd:
            return

        self.incomplete_path.mkdir(exist_ok=True)
        self.log_path.mkdir(exist_ok=True)

        parcel_id = datetime.utcnow().strftime('%Y-%m-%dT%H%M%SZ')
        dest = self.incomplete_path.joinpath(parcel_id)
        outpath = self.log_path.joinpath(f'{parcel_id}.out')
        errpath = self.log_path.joinpath(f'{parcel_id}.err')
        env = {'PARCEL_DEST': str(dest),
               'DATASET_PATH': str(self.path)}

        with open(outpath, 'w') as out:
            with open(errpath, 'w') as err:
                subprocess.check_call(cmd, shell=True, env=env,
                                      stdout=out, stderr=err)

        oldpath = find_parcel_data_path(self.incomplete_path, parcel_id)
        if not oldpath:
            raise Exception(f'export did not produce data in {dest}')
        newpath = self.data_path.joinpath(oldpath.name)
        self.data_path.mkdir(exist_ok=True)
        oldpath.rename(newpath)

        self.commit(f'[export_manager] add parcel data for {parcel_id}',
                    [str(newpath)])

        return parcel_id

    def find_parcel_ids(self):
        ids = set()
        ids.update(p.stem for p in self.data_path.glob('*'))
        ids.update(p.stem for p in self.incomplete_path.glob('*'))
        ids.update(p.stem for p in self.log_path.glob('*.*'))
        ids = (i for i in ids if PARCEL_ID_FORMAT.match(i))
        return sorted(ids)

    def is_due(self, margin = timedelta(minutes=5)):
        cfg = self.read_config()
        delta_str = cfg.get('interval', None)
        if not delta_str:
            return False
        delta = interval.parse_delta(delta_str) - margin

        ids = self.find_parcel_ids()
        if not ids:
            return True
        last = datetime.strptime(ids[-1], '%Y-%m-%dT%H%M%S%z')
        now = datetime.now(last.tzinfo)
        return (now - last) >= delta

    def clean(self):
        cfg = self.read_config()
        keep = cfg.get('keep', None)
        if not keep:
            return

        git_rm = []

        ids = self.find_parcel_ids()
        while len(ids) > keep:
            for path in self.log_path.glob(f'{ids[0]}.*'):
                path.unlink()

            # TODO: it would probably be best to ensure we keep a certain
            #   number of complete parcels regardless of how many incomplete
            #   parcels there are
            incomplete = find_parcel_data_path(self.incomplete_path, ids[0])
            if incomplete:
                if incomplete.is_file():
                    incomplete.unlink()
                if incomplete.is_dir():
                    shutil.rmtree(incomplete)

            complete = find_parcel_data_path(self.data_path, ids[0])
            if complete:
                git_rm.append(str(complete))
                if complete.is_file():
                    complete.unlink()
                if complete.is_dir():
                    shutil.rmtree(complete)

            ids.pop(0)

        if git_rm:
            self.commit('[export_manager] clean', rm=git_rm)
