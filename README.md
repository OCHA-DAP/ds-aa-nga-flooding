# Nigeria Anticipatory Action: flooding
<!-- markdownlint-disable MD013 -->
[![Generic badge](https://img.shields.io/badge/STATUS-ENDORSED-%231EBFB3)](https://shields.io/)

This repository contains work in progress to support the OCHA Anticipatory Action Framework.

Initial outputs include:

- [Flood Exposure Return Periods](https://nga-floodexposure-marim-development-guc4dqhrguaxf3d9.eastus2-01.azurewebsites.net/)
- [Monitoring Overview](https://nga-monitoring-marimo-development-d6fhc2fud0hqa3e6.eastus2-01.azurewebsites.net/)

## Reproducing this analysis

Create a directory where you would like the data to be stored,
and point to it using an environment variable called
`AA_DATA_DIR`.

Next create a new virtual environment and install the requirements with:

```shell
pip install -r requirements.txt
```

Finally, install any code in `src` using the command:

```shell
pip install -e .
```

If you would like to instead receive the processed data from our team, please
[contact us](mailto:centrehumdata@un.org).

## Development

All code is formatted according to black and flake8 guidelines.
The repo is set-up to use pre-commit.
Before you start developing in this repository, you will need to run

```shell
pre-commit install
```

The `markdownlint` hook will require
[Ruby](https://www.ruby-lang.org/en/documentation/installation/)
to be installed on your computer.

You can run all hooks against all your files using

```shell
pre-commit run --all-files
```

It is also **strongly** recommended to use `jupytext`
to convert all Jupyter notebooks (`.ipynb`) to Markdown files (`.md`)
before committing them into version control. This will make for
cleaner diffs (and thus easier code reviews) and will ensure that cell outputs
aren't
committed to the repo (which might be problematic if working with sensitive
data).

## Framework monitoring

This repo also includes code for monitoring Google and
GloFAS forecasts per the CERF AA framework thresholds.
This monitoring includes:

- Retrieving data from forecast sources and saving to a database
- Saving output summary plots in Azure blob storage
- Sending regular email updates. The recipients of these
emails are configured in `.csv` files saved to Azure blob storage.

The setup for this monitoring can be found in `.github/workflows/monitoring.yml`.

### Configuration

Configure monitoring runs using the following environment variables:

- `STAGE`: (`dev` or `prod`) Determines whether the monitoring emails are sent to the test distribution list, or to the 'production' distribution list. Note that when running on the schedule, `STAGE` will fall back to the value in the GitHub Actions `STAGE` variable, and if this is not set, it will default to `dev`.
- `MONITORING_DATE`: (`yyyy-mm-dd`) The date for which to check forecast sources. Defaults to today if not set.
Due to different data storage procedures for older data, the monitoring is currently only set up to run for 2024 onwards.
