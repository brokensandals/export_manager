"""The main API for accessing datasets managed by export_manager.

Terminology:
- a "dataset" groups all the data exported from a particular source,
  and includes configuration and a set of parcels; each dataset has
  a directory
- a "parcel" is a particular data dump made at a point in time; it is
  identified by a date-time string; it includes data files, stdout/stderr
  logs, and metrics, stored within the dataset's directory

The DatasetAccessor class is the main entry point for working with datasets.
"""


import csv
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from git import Repo
import glob
from operator import itemgetter
from pathlib import Path
import re
import shutil
import subprocess
import sys
import toml
import traceback
from export_manager import _fsutil
from export_manager import _interval


_DEFAULT_GITIGNORE = """.DS_Store
incomplete/
log/
/secrets*
"""

_DEFAULT_CONFIG_TOML = """# cmd = "echo example > $PARCEL_PATH.txt"
# keep = 5
# interval = "1 day"
"""

_INITIAL_METRICS_CSV = "parcel_id,success,files,bytes"

_PARCEL_ID_FORMAT = re.compile('\\A\\d{4}-\\d{2}-\\d{2}T\\d{6}Z\\Z')

_VALID_INGEST_TIME_SOURCES = ['mtime', 'now']


class ParcelIDFormatException(Exception):
    """Indicates an invalid parcel_id was used."""
    pass


class ParcelExistsException(Exception):
    """Indicates an attempt to use a parcel_id that already exists."""
    pass


def new_parcel_id():
    """Generates a parcel ID corresponding to the current time."""
    return datetime.utcnow().strftime('%Y-%m-%dT%H%M%SZ')


def _find_parcel_data_path(parent, parcel_id):
    matches = list(parent.glob(f'{parcel_id}*'))
    if not matches:
        return None
    if len(matches) > 1:
        raise Exception(
            f'multiple data files or dirs exist for {parcel_id} in {parent}')
    return matches[0]


def parse_parcel_id(parcel_id):
    """Returns the datetime the parcel_id represents.

    Raises ValueError if it is invalid.
    """
    if not _PARCEL_ID_FORMAT.match(parcel_id):
        raise ParcelIDFormatException(parcel_id)
    return datetime.strptime(parcel_id, '%Y-%m-%dT%H%M%S%z')


class ParcelAccessor:
    """Helps access a parcel's data/metadata.

    These should be retrieved from a DatasetAccessor, not created directly.

    Attributes:
    - dataset_accessor
    - parcel_id
    - datetime - the parsed parcel_id
    """
    def __init__(self, dataset_accessor, parcel_id):
        self.dataset_accessor = dataset_accessor
        self.parcel_id = parcel_id
        self.datetime = parse_parcel_id(parcel_id)

    def find_data(self):
        """Returns the pathlib.Path to the complete data, or None.

        "Complete" data means the export process completed without errors.
        """
        return _find_parcel_data_path(
            self.dataset_accessor.data_path, self.parcel_id)

    def find_incomplete(self):
        """Returns the pathlib.Path to the incomplete data, or None.

        "Incomplete" data means the export process may have been interrupted
        or failed, so the data should be regarded with suspicion.
        """
        return _find_parcel_data_path(
            self.dataset_accessor.incomplete_path, self.parcel_id)

    def is_complete(self):
        """Returns True or False to indicate whether this parcel is complete.

        See find_data and find_incomplete for more info.
        """
        return bool(self.find_data())

    def find_stdout(self):
        """Returns the pathlib.Path to the stdout log, or None.

        The log contains the stdout of the export command that produced
        the parcel.
        """
        path = self.dataset_accessor.log_path.joinpath(f'{self.parcel_id}.out')
        if path.is_file():
            return path
        return None

    def find_stderr(self):
        """Returns the pathlib.Path to the stderr log, or None.

        The log contains the stderr of the export command that produced
        the parcel.
        """
        path = self.dataset_accessor.log_path.joinpath(f'{self.parcel_id}.err')
        if path.is_file():
            return path
        return None

    def is_known(self):
        """Returns True if there is any record of this parcel existing.

        The presence of a complete or incomplete data file, log files, or
        an entry in metrics.csv counts.
        """
        return bool(self.find_data()
                    or self.find_incomplete()
                    or self.find_stdout()
                    or self.find_stderr()
                    or self.parcel_id in self.dataset_accessor.read_metrics())


class DatasetAccessor:
    """Helps access a dataset stored in a given directory.

    The class is named 'Accessor' to emphasize that no information about
    the dataset is stored in memory, beyond the path to its directory.
    All information is retrieved from the filesystem upon request.

    Attributes:
    - path - the pathlib.Path to the directory containing the dataset
    """
    def __init__(self, path):
        """Creates an instance for accessing a dataset at the given path.

        The path does not need to exist, as the initialize method can be
        used to set up a new dataset. However, if the path exists, it
        should be a path to a directory, not a file.
        """
        self.path = Path(path)
        self.config_path = self.path.joinpath('config.toml')
        self.data_path = self.path.joinpath('data')
        self.incomplete_path = self.path.joinpath('incomplete')
        self.log_path = self.path.joinpath('log')
        self.metrics_path = self.path.joinpath('metrics.csv')

    def initialize(self, *, git=False):
        """Ensures the dataset exists and initializes common files inside it.

        If git=True, a git repo and .gitignore file will also be set up.

        This method will only create dirs/files that do not already exist,
        so it's safe to call on an already-existing dataset.
        """
        self.path.mkdir(exist_ok=True, parents=True)
        self.data_path.mkdir(exist_ok=True)
        self.incomplete_path.mkdir(exist_ok=True)
        self.log_path.mkdir(exist_ok=True)

        if not self.config_path.exists():
            config = _DEFAULT_CONFIG_TOML
            if git:
                config += 'git = true\n'
            self.config_path.write_text(config)

        if not self.metrics_path.exists():
            self.metrics_path.write_text(_INITIAL_METRICS_CSV)

        if git:
            if not self.path.joinpath('.git').exists():
                Repo.init(str(self.path))
            gitignore_path = self.path.joinpath('.gitignore')
            if not gitignore_path.exists():
                gitignore_path.write_text(_DEFAULT_GITIGNORE)
            self._commit('[export_manager] initialize',
                         ['.gitignore', 'config.toml', 'metrics.csv'])

    def read_config(self):
        """Returns a dict of the dataset's config, read from config.toml."""
        if not self.config_path.is_file():
            raise Exception(f'{self.config_path} is not a file')
        return toml.load(self.config_path)

    def write_config(self, cfg):
        """Saves the given dict as the dataset's config in config.toml."""
        with open(self.config_path, 'w') as file:
            toml.dump(cfg, file)

    def is_git(self):
        """Returns True if the dataset's config enables git commits."""
        return self.read_config().get('git', False)

    def _commit(self, message, add=[], *, rm=[]):
        if self.is_git():
            repo = Repo(str(self.path))
            if add:
                repo.index.add(add)
            if rm:
                repo.index.remove(rm, r=True)
            if repo.is_dirty():
                repo.index.commit(message)

    def read_metrics(self):
        """Returns a dict of parcel_ids to parcel metrics.

        Each entry is a dict of string column names to string values.
        The data is read from metrics.csv; an empty dict is returned
        if metrics.csv is missing.
        """
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

    def _update_metrics(self, updates):
        """Creates or updates metrics for specified parcels.

        The input is a dict mapping parcel_ids to a dict of metrics.
        The metrics dict should map string column names to string values.

        Existing parcels that are not included in the updates map will
        remain in the metrics file.
        Any parcels that are included in the updates map and already
        exist in the metrics file will be completely overridden:
        if the update is missing metrics that are present in the existing
        row, those metrics will be replaced with empty strings.

        metrics.csv is created or updated by this method.
        """
        if not updates:
            return

        metrics = self.read_metrics()
        field_order = []
        fields = set()
        if metrics:
            field_order = list(next(iter(metrics.values())).keys())
            fields.update(field_order)
        for row in updates.values():
            for key in row.keys():
                if key not in fields:
                    field_order.append(key)
                    fields.add(key)
            metrics[row['parcel_id']] = row
        rows = sorted(metrics.values(), key=itemgetter('parcel_id'))

        with open(self.metrics_path, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=field_order)
            writer.writeheader()
            writer.writerows(rows)

    def _collect_metrics(self, parcel_id):
        """Calculates metrics for the parcel and returns them as a dict.

        The result is a map of string keys to string values.

        The following keys are always populated:
        - parcel_id
        - success: "Y" if complete data exists for the parcel, else "N"

        The following keys are populated if there is any data for the parcel:
        - files: total number of the parcel's data files
        - bytes: total number of bytes for the parcel's data files

        If there is data, additional metrics will be calculated based on
        config.toml. For example, if the config.toml contains:

        metrics.lines.cmd = "wc -l < $PARCEL_PATH"

        then the result dict will contain a key "lines" whose value is
        the output of executing that command. If the command fails,
        the value will be "ERROR". Note that leading/trailing whitespace
        is stripped from the command's output.

        The following env vars are set when executing commands:
        - DATASET_PATH: the directory this DatasetAccessor was created with
        - PARCEL_PATH: the path to the file or directory where the
                       parcel's data was written. Note that unlike
                       when the export command is run, here the
                       variable will include the file extension, if any.
        """
        results = {'parcel_id': parcel_id}
        pa = self.parcel_accessor(parcel_id)
        path = pa.find_data()
        if path:
            results['success'] = 'Y'
        else:
            results['success'] = 'N'
            path = _find_parcel_data_path(self.incomplete_path, parcel_id)

        if path:
            results['files'] = str(_fsutil.total_file_count(path))
            results['bytes'] = str(_fsutil.total_size_bytes(path))

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

    def _process_metrics(self, parcel_ids):
        updates = {p: self._collect_metrics(p) for p in parcel_ids}
        self._update_metrics(updates)

    def reprocess_metrics(self, parcel_ids):
        """Updates metrics.csv with recalculated values for the given parcels.

        See the _collect_metrics method for details on how metrics are built.

        If git=true in config.toml, this method commits the metrics.csv file.
        """
        self._process_metrics(parcel_ids)
        message = ('[export_manager] reprocess metrics for: '
                   + ', '.join(parcel_ids))
        self._commit(message, ['metrics.csv'])

    def _run_export(self, parcel_id):
        """Runs the dataset's export command, without committing.

        Returns the parcel_id, or None if no export command is configured.

        The given parcel_id must be in the YYYY-MM-DDTHHMMSSZ format.
        The parcel_id must not already be in use.

        This runs the shell command specified by "cmd" in config.toml.
        If none is specified, this method does nothing.

        The following env vars are set when executing the command:
        - DATASET_PATH: the directory this DatasetAccessor was created with
        - PARCEL_PATH: a path and file prefix where the parcel should be
                       written. This will be a location inside the dataset
                       directory, and includes the parcel_id. The command
                       may append a file extension to this, or may call
                       mkdir on it and add files inside that directory.

        The command's stdout and stderr are saved to files associated with
        the parcel.

        If the command's exit code is nonzero, an error will be raised.
        Also, the parcel will be considered 'incomplete' - the data
        will reside in a different location than complete data.
        """
        pa = self.parcel_accessor(parcel_id)
        if pa.is_known():
            raise ParcelExistsException(parcel_id)

        cfg = self.read_config()
        cmd = cfg.get('cmd', None)
        if not cmd:
            return None

        self.incomplete_path.mkdir(exist_ok=True)
        self.log_path.mkdir(exist_ok=True)

        dest = self.incomplete_path.joinpath(parcel_id)
        outpath = self.log_path.joinpath(f'{parcel_id}.out')
        errpath = self.log_path.joinpath(f'{parcel_id}.err')
        env = {'PARCEL_PATH': str(dest),
               'DATASET_PATH': str(self.path)}

        with open(outpath, 'w') as out:
            with open(errpath, 'w') as err:
                subprocess.check_call(cmd, shell=True, env=env,
                                      stdout=out, stderr=err)

        oldpath = _find_parcel_data_path(self.incomplete_path, parcel_id)
        if not oldpath:
            raise Exception(f'export did not produce data in {dest}')
        newpath = self.data_path.joinpath(oldpath.name)
        self.data_path.mkdir(exist_ok=True)
        oldpath.rename(newpath)

        return parcel_id

    def perform_export(self, parcel_id=None):
        """Runs the dataset's export command to produce a new parcel.

        Returns the parcel_id used, or None if there is no export command
        configured for the dataset.

        See _run_export for details on how export commands are configured.
        By default new_parcel_id() is used for the parcel_id.

        Updates metrics.csv.

        If git=true in config.toml, commits the metrics and data file.
        Incomplete data files are not committed.
        """
        parcel_id = self._run_export(parcel_id or new_parcel_id())
        added = []
        if parcel_id:
            self._process_metrics([parcel_id])
            added.append('metrics.csv')
            pa = self.parcel_accessor(parcel_id)
            if pa:
                dp = pa.find_data()
                if dp:
                    added.append(str(dp.relative_to(self.path)))
        message = f'[export_manager] add new export {parcel_id}'
        self._commit(message, added)
        return parcel_id

    def _run_ingest(self):
        """Moves any files matching the configured ingest.paths

        A dict of parcel_ids that were created, to their original path,
        is returned.

        This looks at the array property "ingest.paths" in config.toml,
        which should be an array of path globs. If the property is not
        set, this method does nothing.

        The paths may be absolute, or relative to the dataset dir.

        All files/dirs matching the globs will be ingested - see
        the ingest method.

        The parcel_ids will be chosen based on the property
        "ingest.time_source" in config.toml, which may have the following
        values:
        - "mtime" - modification time of the file
        - "now" (default) - create a new parcel_id based on current time
        """
        cfg = self.read_config()
        pathglobs = cfg.get('ingest', {}).get('paths', [])
        if not pathglobs:
            return {}
        if isinstance(pathglobs, str):
            pathglobs = [pathglobs]
        found = []
        for pathglob in pathglobs:
            # Checking specifically for '~' is super hacky, but I haven't
            # found a better way to do this.
            if Path(pathglob).is_absolute() or pathglob.startswith('~'):
                found += glob.glob(pathglob, recursive=True)
            else:
                found += list(self.path.glob(pathglob))

        time_source = cfg.get('ingest').get('time_source', 'now')
        if time_source not in _VALID_INGEST_TIME_SOURCES:
            raise Exception('invalid ingest.time_source '
                            f'(expected one of {_VALID_INGEST_TIME_SOURCES}) '
                            f': {time_source}')

        parcel_ids = {}
        for path in found:
            parcel_id = None
            if time_source == 'mtime':
                dt = datetime.fromtimestamp(
                    path.stat().st_mtime, timezone.utc)
                parcel_id = dt.strftime('%Y-%m-%dT%H%M%SZ')
            elif time_source == 'now':
                parcel_id = new_parcel_id()
            self._ingest_path(path, parcel_id)
            parcel_ids[parcel_id] = path
        return parcel_ids

    def _ingest_path(self, path, parcel_id):
        """Ingests path without processing metrics or committing."""
        pa = self.parcel_accessor(parcel_id)
        if pa.is_known():
            raise ParcelExistsException(parcel_id)
        path = Path(path)
        newpath = self.data_path.joinpath(parcel_id).with_suffix(path.suffix)
        path.rename(newpath)

    def ingest_path(self, path, parcel_id=None):
        """Ingests the specified file or directory into the dataset directory.

        The file/dir will be saved as a (completed) parcel with the given id.

        The given parcel_id must be in the YYYY-MM-DDTHHMMSSZ format;
        by default new_parcel_id() is used.
        The parcel_id must not already be in use.

        If git=true in config.toml, the data and metrics will be committed.
        """
        if not parcel_id:
            parcel_id = new_parcel_id()
        self._ingest_path(path, parcel_id)
        self.reprocess_metrics([parcel_id])
        message = f'[export_manager] ingest {path} as {parcel_id}'
        pa = self.parcel_accessor(parcel_id)
        self._commit(message, ['metrics.csv',
                               str(pa.find_data().relative_to(self.path))])

    def process(self):
        """Runs ingestion, export (if due), updates metrics, then commits.

        Returns a tuple containing a list of new parcel IDs and a list of
        exceptions that were caught. Exceptions running the export or
        updating metrics are caught, other errors are propagated.

        For details on how each step is handled see:
        - _run_ingest
        - _run_export
        - _collect_metrics
        """
        parcel_ids = []
        errors = []

        # TODO: handle errors here
        ingested_paths = self._run_ingest()
        parcel_ids += ingested_paths.keys()

        if self.is_due():
            export_id = new_parcel_id()
            parcel_ids.append(export_id)
            try:
                self._run_export(export_id)
            except Exception as e:
                print(f'export failed for {self.path}', file=sys.stderr)
                traceback.print_exc()
                errors.append(e)

        try:
            self._process_metrics(parcel_ids)
        except Exception as e:
            print(f'metrics update failed for {self.path}', file=sys.stderr)
            traceback.print_exc()
            errors.append(e)

        message = '[export_manager] process new parcels: ' + ', '.join(parcel_ids)
        for pid in ingested_paths:
            message += f'\n{pid} was ingested from {ingested_paths[pid]}'
        added = ['metrics.csv']
        for pid in parcel_ids:
            pa = self.parcel_accessor(pid)
            dp = pa.find_data()
            if dp:
                added.append(str(dp.relative_to(self.path)))

        self._commit(message, added)

        return parcel_ids, errors

    def find_parcel_ids(self):
        """Returns the ids of extant parcels, as a list of strings.

        This only includes parcels for which data (complete or incomplete)
        or logs exist, not historical parcels recorded in metrics.csv.
        """
        ids = set()
        ids.update(p.stem for p in self.data_path.glob('*'))
        ids.update(p.stem for p in self.incomplete_path.glob('*'))
        ids.update(p.stem for p in self.log_path.glob('*.*'))
        ids = (i for i in ids if _PARCEL_ID_FORMAT.match(i))
        return sorted(ids)

    def parcel_accessor(self, parcel_id):
        """Returns a ParcelAccessor for the specified parcel_id.

        As long as the parcel_id follows the correct datetime format,
        this currently always returns an instance, even if the parcel
        does not actually exist.
        """
        return ParcelAccessor(self, parcel_id)

    def parcel_accessors(self):
        """Returns a list of ParcelAccessors for all extant parcels.

        This only includes parcels for which data (complete or incomplete)
        or logs exist, not historical parcels recorded in metrics.csv.
        """
        return [ParcelAccessor(self, pid) for pid in self.find_parcel_ids()]

    def is_due(self, margin=timedelta(minutes=5)):
        """Returns True if the dataset's schedule demands a new parcel.

        The margin parameter will be subtracted from the scheduled interval,
        making the dataset due sooner than it otherwise would be. This is so
        that if you're running this tool via cron on, say, a daily schedule,
        and your exports are set to "1 day" intervals, they'll still be "due"
        even if the previous export actually happened slightly less than a day
        ago.

        Due date is determined using the date of the most recent extant parcel
        (complete or incomplete) and the "interval" property in config.toml.
        If interval is not configured, this method returns False.

        Interval should be a string such as "1 day", "3 hours", etc.
        """
        cfg = self.read_config()
        delta_str = cfg.get('interval', None)
        if not delta_str:
            return False
        delta = _interval.parse_delta(delta_str) - margin

        ids = self.find_parcel_ids()
        if not ids:
            return True
        last = parse_parcel_id(ids[-1])
        now = datetime.now(last.tzinfo)
        return (now - last) >= delta

    def _clean(self):
        """Removes old parcels without committing.

        Returns paths of removed complete data files.

        The number of parcels to keep at once is determined by the "keep"
        property in config.toml. If that property is missing, this method
        does nothing.

        All but the most recent N parcels (where N = value of "keep") will
        be deleted. Complete and incomplete data files, and log files, are
        deleted. Currently, the most recent parcels are kept regardless of
        their completeness status, so it is possible that only incomplete
        parcels remain after this method runs.

        Nothing is removed from metrics.csv.
        """
        cfg = self.read_config()
        keep = cfg.get('keep', None)
        if not keep:
            return []

        rm = []

        parcels = self.parcel_accessors()
        while len(parcels) > keep:
            out_path = parcels[0].find_stdout()
            if out_path:
                out_path.unlink()

            err_path = parcels[0].find_stderr()
            if err_path:
                err_path.unlink()

            # TODO: it would probably be best to ensure we keep a certain
            #   number of complete parcels regardless of how many incomplete
            #   parcels there are
            incomplete = parcels[0].find_incomplete()
            if incomplete:
                if incomplete.is_file():
                    incomplete.unlink()
                if incomplete.is_dir():
                    shutil.rmtree(incomplete)

            complete = parcels[0].find_data()
            if complete:
                rm.append(complete)
                if complete.is_file():
                    complete.unlink()
                if complete.is_dir():
                    shutil.rmtree(complete)

            parcels.pop(0)

        return rm

    def clean(self):
        """Removes old parcels.

        See _clean for details on how this is done.

        If git=True in config.toml, a commit is made to remove complete
        data files.
        """
        rm = self._clean()
        git_rm = [str(p.relative_to(self.path)) for p in rm]

        if git_rm:
            self._commit('[export_manager] clean', rm=git_rm)
