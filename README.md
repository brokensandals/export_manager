# export\_manager

This tool helps manage automatic backups of data from cloud services.

## Getting Started

1. Install python3 and pip
2. `pip install export_manager`
3. Create a directory to hold all your exports, for example:
    ```bash
    mkdir ~/exports
    ```
4. Use the tool to set up subdirectories for each data source:
    ```bash
   export_manager --base ~/exports new todoist
   export_manager --base ~/exports new goodreads
   # etc. The names can be anything you want, those are just examples.
    ```
    This will create a directory structure like this:
    ```
   ~/exports/
       todoist/
           config.toml
           metrics.csv
           data/
       goodreads/
           config.toml
           metrics.csv
           data/
   ```
5. Edit the `config.toml` file in each subdirectory to specify the schedule and the command to run.
   For example, if you install [middling\_export\_todoist][middling_export_todoist] you could use the following config to export data from Todoist:
    ```toml
    # Command to invoke. $EXPORT_DEST will be set to ~/exports/todoist/data/DATETIME
    exportcmd = "TODOIST_API_TOKEN=your_token middling_export_todoist full_sync > $EXPORT_DEST.json"
    # Only get a new export if the last one is at least 1 day old.
    interval = "1 day"
    # Only keep the most recent 5 exports.
    # If you don't specify this, export_manager will not delete old exports.
    keep = 5
    ```
6. Run `export_manager -b ~/exports process` to run all the exports and cleanups that are due.
   You should have a cron or launchd job run this periodically.

## Development

Setup:

1. Install python3 and pip
2. Clone the repo
3. I recommend creating a venv:
    ```bash
    cd export_manager
    python3 -m venv venv
    source venv/bin/activate
    ```
4. Install dependencies:
    ```bash
   pip install .
   pip install -r requirements-dev.txt
    ```

To run unit tests:

```bash
pytest
```

To run the CLI:

```bash
PYTHONPATH=src python -m export_manager ...
```

[middling_export_todoist]: https://github.com/brokensandals/middling_export_todoist
