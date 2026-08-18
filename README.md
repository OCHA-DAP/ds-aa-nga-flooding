# Nigeria Anticipatory Action: flooding
<!-- markdownlint-disable MD013 -->
[![Generic badge](https://img.shields.io/badge/STATUS-ENDORSED-%231EBFB3)](https://shields.io/)

This repository contains the analysis and operational monitoring for the OCHA Anticipatory Action framework for flooding in Nigeria (2026 framework: Adamawa riverine + BAY-states flash flood).

**Operational monitoring** (daily, from `main` — see [CLAUDE.md](CLAUDE.md) for the full architecture, trigger definitions, and ops runbook):

- `Monitor flooding` — Adamawa riverine: GloFAS readiness (forecast ≤12 d OR latest reanalysis vs 3,132 m³/s at Wuroboki) + multi-gauge action trigger (≥6 of 10 Google gauges over their 4-yr RP thresholds). Daily 20:00 UTC.
- `Monitor flash flooding` — 4 BAY-states LGAs (Mobbar, Maiduguri, Jere, Geidam): FloodScan×WorldPop exposure vs per-LGA thresholds, with an approaching-threshold advisory tier. Daily 01:30 UTC.

Both send email updates via the team Listmonk instance (weekly informational + immediate trigger/advisory alerts); the `STAGE` repo variable switches between the test and production audiences.

Other outputs:

- [Niger/Benue multi-state monitoring app](https://ocha-dap.github.io/ds-aa-nga-flooding/app/) (GH Pages)
- [2026 CERF analysis notes](https://ocha-dap.github.io/ds-aa-nga-flooding/exploration/2026/cerf/notes/)

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
