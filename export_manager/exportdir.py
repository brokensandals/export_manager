from pathlib import Path

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
        self.metrics_path = self.path.joinpath('metrics.csv')

    def is_valid(self):
        return self.path.is_dir() and self.config_path.is_file()

    def initialize(self):
        """Creates the directory and config.toml file if they don't exist."""
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)
        if not self.config_path.exists():
            self.config_path.touch()
