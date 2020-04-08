from git import Repo
from pathlib import Path


DEFAULT_GITIGNORE = """.DS_Store
incomplete/
log/
/secrets*
"""

DEFAULT_CONFIG_TOML = """# cmd = "echo example > $PARCEL_DEST.txt"
# keep = 5
# interval = "1 day"
"""

INITIAL_METRICS_CSV = "timestamp,files,bytes"


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
                gitignore_path.write_bytes(DEFAULT_GITIGNORE)
            repo = Repo(str(self.path))
            index = repo.index
            index.add(['.gitignore', 'config.toml', 'metrics.csv'])
            if repo.is_dirty():
                index.commit('[export_manager] initialize')
