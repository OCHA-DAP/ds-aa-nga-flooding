# ds-aa-nga-flooding

Analysis and **operational monitoring** for the OCHA Anticipatory Action
framework for flooding in Nigeria. The 2026 framework (endorsed July 2026,
document pending publication) has two independent components, each with its
own daily monitoring pipeline and its own email audience.

## Operational monitoring (runs from `main`)

| workflow | schedule (UTC) | pipeline | component |
|---|---|---|---|
| `Monitor flooding` (`monitoring.yml`) | daily 20:00 | `check_forecasts.py` → `save_plots.py` → `send_emails.py` | Adamawa riverine (readiness + action) |
| `Monitor flash flooding` (`flash-monitoring.yml`) | daily 01:30 | `monitor_flash_flood.py` | BAY-states flash flood (observational) |

The flash schedule is timed to the upstream `floodexposure-monitoring` chain
(its 23:15 UTC cron normally lands the day's exposure in the DB by ~23:52,
worst observed ~01:10); the pipeline fails loudly if the data is stale
rather than emailing an outdated situation.

## Triggers (2026 framework — all values in `src/constants.py`)

- **Riverine action**: ≥6 of the 10 endorsed Google GRRR gauges
  (`ACTION_GAUGE_THRESHOLDS`) simultaneously exceed their individual 4-yr
  RP thresholds on the same forecast valid day. Thresholds derived in
  `exploration/2026/cerf/workflow/06_trigger_definition.ipynb`.
- **Riverine readiness**: GloFAS at Wuroboki > 3,132 m³/s — in the
  ensemble-mean forecast at lead ≤13 days OR in the latest intermediate
  reanalysis (the reanalysis OR-branch is in the endorsed framework
  document; it supersedes the "not recommended" verdict in notebook 07 and
  `notes/trigger_development.md`).
- **Flash flood (observational)**: FloodScan×WorldPop population exposure
  (strict 3-day rolling mean) in any of 4 LGAs (Mobbar, Maiduguri, Jere,
  Geidam — NiHSA composite retargeting) exceeds its per-LGA threshold
  (`FLASH_LGAS`). Thresholds come from the framework document and were
  independently validated: each reproduces as the empirical ~7.75-yr RP of
  calendar-year annual maxima (1998–2025) computed on **exactly the data
  the monitoring reads** (`app.floodscan_exposure` 3-day rolling), i.e.
  each LGA exceeds in exactly 3 of 28 historical years; combined any-LGA
  RP ~3.5 yr. Do not "correct" the odd-looking RP — the equal-count design
  is the method. An advisory tier fires at `FLASH_WARNING_FRACTION` (0.8)
  of any threshold.

## Email dispatch (Listmonk / ocha-relay)

Campaigns use the instance's default `base_campaign` template; charts are
uploaded to the Listmonk media library. Lists are resolved by tag at
runtime (`LISTMONK_LISTS` / `LISTMONK_FLASH_LISTS`); created by
`pipelines/setup_nga_listmonk_lists.py` (`--flash` for the flash stream,
which has a **separate audience**).

- Riverine: informational 113 (`nga:info`), trigger 114 (`nga:trigger`),
  test 115 (`nga:test`).
- Flash: informational 118 (`nga-flash:info`), trigger 119
  (`nga-flash:trigger`), test 120 (`nga-flash:test`).

**The `STAGE` repo variable is the prod/test switch**: anything but `prod`
routes every send to the stream's test list, adds a `[TEST]` subject
prefix, and (via `[test]` in the campaign name) the template's test
banner. Cadence: weekly Monday informational; immediate send on trigger
(both streams) or approaching-threshold (flash). The legacy blob CSVs
under `ds-aa-nga-flooding/email/` are superseded; the setup script can
migrate them into the riverine lists when the full audience comes aboard.

**Testing emails**: run with `STAGE=dev` (or unset) so sends go to the
test lists only. Never test against the production lists, and don't use
the legacy blob test CSV (it contains real team addresses).

## Data plumbing & gotchas

- Riverine forecasts land in `projects.ds_aa_nga_flooding_monitoring`
  (dev-stage DB; unique key `monitoring_date, valid_date, src`). Flash
  reads `app.floodscan_exposure` (**prod**-stage DB; `adm_level` is TEXT —
  quote it) written by the `floodexposure-monitoring` pipeline.
- **CDS GloFAS**: `cems-glofas-historical` was restructured during the
  GloFAS v5 rollout (Aug 2026): `year/month/day` params, renamed variable,
  required `timespan`. We stay pinned to `version_4_0` (the 3,132 m³/s
  threshold is calibrated on v4 climatology). The intermediate product
  lags ~2–8 days; `check_forecasts.py` walks back up to 7 days and
  continues without reanalysis if none is available.
- **When GloFAS v5 becomes the operational forecast** (`cems-glofas-forecast`
  `system_version=operational` cannot be pinned), the readiness threshold
  needs re-derivation/sanity-check against v5 climatology.
- **eccodes ≥2.47.0 required** (`requirements.txt`): post-v5 GRIBs carry an
  ECMWF local-section template older libeccodes can't parse. The GRIB data
  variable is picked generically in `etl.process_glofas` (name differs by
  product). On linux, eccodes/cfgrib segfaults at interpreter teardown
  after successful runs — hence `os._exit(0)` at the end of
  `check_forecasts.py`; real failures still exit non-zero.
- GitHub disables scheduled workflows after 60 days without repo activity
  (`disabled_inactivity`) — this silently killed monitoring from Dec 2025
  to Aug 2026. If crons stop, check `gh workflow list --all` first.

## Apps

The GH Pages site (https://ocha-dap.github.io/ds-aa-nga-flooding/) hosts
the multi-state Niger/Benue exploration app, deployed from
`feat/niger-benue-multistate-monitoring` by `deploy-app-cron.yml` every
6 h — a separate work stream from the operational monitoring above.

## KB

Team knowledge-base pages: `frameworks/nga-flooding/2026-06-18.md`
(trigger design + provenance) and `pipelines/nga-flooding-monitoring.md`
(ops runbook) in `OCHA-DAP/ds-knowledge-base`.
