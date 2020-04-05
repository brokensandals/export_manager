# export\_manager

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
    ```

To run unit tests:

```bash
PYTHONPATH=src python -m unittest discover tests
```

To run the CLI:

```bash
PYTHONPATH=src python -m export_manager ...
```
