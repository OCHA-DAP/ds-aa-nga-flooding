# monitoring-status

Not a code branch. `monitoring.yml` and `flash-monitoring.yml` (on `main`)
push `exploration/2026/cerf/monitoring/status.json` and the latest chart
PNGs here after each daily run, via `pipelines/export_monitoring_status.py`.

Pushed directly (no PR) because `main` requires pull requests and this
branch updates twice a day with no review step. The static page that reads
this data — `exploration/2026/cerf/monitoring/index.html` — lives on `main`
and goes through normal review; only the generated data lives here.

See the commit that introduced this for the full design writeup.
