# export\_manager

This tool helps manage automatic exports/backups of personal data (for example, from cloud services like Todoist or Goodreads).

It handles:

- Running exports according to a schedule (note: export\_manager itself must be run via a scheduler like cron/launchd/etc)
- Keeping data and logs organized
- Cleaning up old data
- Gathering metrics and generating reports to help you see whether your backups are working

Terminology:

- **dataset**: a collection of parcels from a particular source, and related config and metadata
- **parcel**: data exported on a single occasion (e.g. your Todoist data retrieved on 2020-0215T01:02:03Z) and related metadata

## Getting Started

1. Install python3 and pip
2. `pip install export_manager`
3. Use the tool to set up directories for each dataset:
    ```bash
   mkdir ~/exports
   export_manager init ~/exports/todoist
   export_manager init ~/exports/goodreads
   # etc. The names can be anything you want, those are just examples.
    ```
    This will create a directory structure like this:
    ```
   ~/exports/
       todoist/
           config.toml
           metrics.csv
           data/
           incomplete/
           log/
       goodreads/
           config.toml
           metrics.csv
           data/
           incomplete/
           log/
   ```
4. Edit the `config.toml` file in each subdirectory to specify the schedule and the command to run.
   For example, if you install [exporteer\_todoist][exporteer_todoist] you could use the following config to export data from Todoist:
    ```toml
    # Command to invoke. $PARCEL_PATH will be set to ~/exports/todoist/data/DATETIME
    # $DATASET_PATH is also available and will be set to ~/exports/todoist
    cmd = "TODOIST_API_TOKEN=your_token exporteer_todoist full_sync > $PARCEL_PATH.json"
    # Only get a new export if the last one is at least 1 day old.
    interval = "1 day"
    # Only keep the most recent 5 exports.
    # If you don't specify this, export_manager will not delete old exports.
    keep = 5
    ```
5. Run `export_manager process ~/exports/*` to run all the exports and cleanups that are due.
   Set up a cron or launchd job to run this periodically.

## Metrics and Reports

You shouldn't trust your backups unless you're testing them.
Testing has to involve some manual action - if it were fully automated, you'd never know if the automation broke.
But ideally, you'd automate the process of collecting all the evidence to prove that a backup is working, so that all you have to do is periodically look at it and say "yep, looks good."

export\_manager helps with this by providing a report you can generate:

```bash
export_manager report ~/exports/*
```

This will:

- Warn you of any obvious problems, such as datasets that are overdue or failing.
- Tell you when each dataset was most recently exported successfully.
- Show you metrics about each dataset's most recent successful parcel, and compare them with metrics from 7 days and 180 days ago.

By default, the metrics include the number of bytes and number of files in the parcel when it was produced.

You can also define custom metrics for each dataset in the `config.toml` file.
For example, for json data, you might use [jq](https://stedolan.github.io/jq/) to count some elements of the json.
The following config creates a metric named "tasks" to track the number of tasks in the todoist exports configured above:

```toml
metrics.tasks.cmd = "jq '.items | length' $PARCEL_PATH"
```

The `process` command gathers all the configured metrics every time a new parcel is produced and stores them in `metrics.csv`.

## Tracking Parcels in Git

If you make your dataset directory a git repo, and set `git = true` in `config.toml`, then the data files of successful exports, as well as the metrics.csv file, will be committed after each change.

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
PYTHONPATH=src pytest
```

(Overriding PYTHONPATH as shown ensures the tests run against the code in the src/ directory rather than the installed copy of the package.)

To run the CLI:

```bash
PYTHONPATH=src python -m export_manager ...
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/brokensandals/export_manager.

## License

This is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

[exporteer_todoist]: https://github.com/brokensandals/exporteer_todoist
