import marimo

__generated_with = "0.13.6"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.image(
        src="2025/assets/UNOCHA_logo_horizontal_blue_CMYK.png", height=100
    ).center()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        rf"""
    # Flood Exposure Observational Trigger

    This notebook is intended to demonstrate potential observational trigger options for flooding across LGAs in BAY states in Nigeria. This trigger is based on flood exposure estimates, which combine daily estimates of flood extent with population distribution. See [this application](https://chd-ds-floodexposure-monitoring.azurewebsites.net/) for more details.
    """
    )
    return


@app.cell
def _(mo):
    mo.callout(
        "This notebook is still a work in progress, and meant for internal use. Please contact hannah.ker@un.org at the OCHA Centre for Humanitarian Data for any questions.",
        kind="warn",
    )
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _():
    import matplotlib.pyplot as plt
    import ocha_stratus as stratus
    import pandas as pd

    import src.constants as constants
    from src.datasources import codab
    from src.utils import blob, rp_calc

    # Colors to be used in plots, etc.
    SAPPHIRE = "#007ce0"
    TOMATO = "#f2645a"
    TOMATO_DARK = "#c25048"
    TOMATO_LIGHT = "#f7a29c"
    MINT = "#1ebfb3"
    GREY_DARK = "#888888"
    GREY_LIGHT = "#cccccc"
    return GREY_DARK, MINT, SAPPHIRE, TOMATO, codab, pd, plt, rp_calc, stratus


@app.cell
def _(codab, stratus):
    # Define database engine and load in adm2 boundaries
    engine = stratus.get_engine(stage="prod")
    adm2 = codab.load_codab_from_blob(aoi_only=True, admin_level=2)
    return adm2, engine


@app.cell
def _(adm2, mo):
    # Select LGAs for prioritization
    options = dict(zip(adm2["ADM2_EN"], adm2["ADM2_PCODE"]))
    lgas = mo.ui.multiselect(options=options, value=["Bama", "Dikwa", "Ngala"])
    return (lgas,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Select LGAs from the dropdown below for analysis""")
    return


@app.cell(hide_code=True)
def _(lgas):
    lgas
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Selected LGAs within the BAY states""")
    return


@app.cell
def _(GREY_DARK, SAPPHIRE, adm2, lgas, plt):
    adm2_pri = adm2[adm2.ADM2_PCODE.isin(lgas.value)]
    _fig, _ax = plt.subplots()
    adm2.boundary.plot(ax=_ax, edgecolor=GREY_DARK, linewidth=0.5)
    adm2_pri.plot(ax=_ax, color=SAPPHIRE)
    _ax.set_axis_off()
    _ax
    return


@app.cell
def _(engine, lgas, pd):
    # Make query to the database
    priority_values = "', '".join(lgas.value)
    priority_clause = f"('{priority_values}')"

    query = f"SELECT * FROM app.floodscan_exposure WHERE iso3='NGA' and pcode IN {priority_clause}"
    with engine.connect() as con:
        df_fe = pd.read_sql(query, con=con, parse_dates=["valid_date"])
    return (df_fe,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        f"""
    ## Flood exposure over time

    For each LGA, we can pull in daily average flood exposure estimates from 1998 to present. We'll calculate the 3-year, 4-year, and 5-year return period thresholds (in total population exposed to flooding) for each LGA. See the graph below for highlighted years that exceed each threshold.
    """
    )
    return


@app.cell
def _(df_fe):
    df_fe["year"] = df_fe.valid_date.dt.year
    df_fe_peaks = (
        df_fe.groupby(["year", "pcode"])
        .agg(
            fe_max=("sum", "max"),
            fe_max_date=(
                "sum",
                lambda x: df_fe.loc[x.idxmax(), "valid_date"],
            ),
        )
        .reset_index()
    )

    rp_vals = {}
    rps = [3, 3.5, 4, 4.5, 5]

    for _rp in rps:
        rp_vals[_rp] = {}
        for _pcode in df_fe_peaks.pcode.unique():
            df_sel = df_fe_peaks[df_fe_peaks.pcode == _pcode]
            rp_val = df_sel["fe_max"].quantile(1 - 1 / _rp)
            rp_vals[_rp][_pcode] = rp_val
    return df_fe_peaks, rp_vals


@app.cell
def _(GREY_DARK, MINT, SAPPHIRE, TOMATO, df_fe, df_fe_peaks, plt, rp_vals):
    pcodes = df_fe["pcode"].unique()
    n_pcodes = len(pcodes)

    n_cols = 1
    n_rows = n_pcodes

    rp_display = {3: SAPPHIRE, 4: MINT, 5: TOMATO}

    _fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(15, 3 * n_rows), sharex=True
    )
    axes = axes.flatten()

    for i, _pcode in enumerate(pcodes):
        dff = df_fe[df_fe["pcode"] == _pcode].sort_values("valid_date")
        dff_fe_peaks = df_fe_peaks[df_fe_peaks["pcode"] == _pcode]
        axes[i].plot(dff["valid_date"], dff["sum"], c=GREY_DARK)
        axes[i].set_title(_pcode)
        axes[i].set_ylabel("Flood exposed population")
        axes[i].tick_params(axis="x", rotation=45)
        axes[i].grid(True, linestyle="--", alpha=0.7)

        mask_rp3 = df_fe_peaks["fe_max"] >= rp_vals[3][_pcode]
        mask_rp4 = df_fe_peaks["fe_max"] >= rp_vals[4][_pcode]
        mask_rp5 = df_fe_peaks["fe_max"] >= rp_vals[5][_pcode]

        for rp, c in rp_display.items():
            axes[i].axhline(rp_vals[rp][_pcode], c=c, label=f"1 in {rp} rp")
            mask = dff_fe_peaks["fe_max"] >= rp_vals[rp][_pcode]
            axes[i].scatter(
                dff_fe_peaks.loc[mask, "fe_max_date"],
                dff_fe_peaks.loc[mask, "fe_max"],
                c=c,
                s=60,
                label=f"Exceeds 1 in {rp} rp",
            )

            for idx, row in dff_fe_peaks.loc[mask].iterrows():
                year = row["fe_max_date"].year
                axes[i].annotate(
                    f"{year}",
                    (row["fe_max_date"], row["fe_max"]),
                    xytext=(
                        5,
                        0,
                    ),  # Offset text slightly above and to the right
                    textcoords="offset points",
                    ha="left",
                    va="bottom",
                    color="black",
                    fontsize=10,
                )

    _fig.text(0.5, 0.04, "Date", ha="center", va="center")
    plt.legend()
    plt.tight_layout()
    # plt.savefig("fe_exceedance.png", dpi=300)
    _fig
    return


@app.cell
def _(df_fe_peaks, rp_calc):
    df_combined = rp_calc.calculate_groups_rp(
        df_fe_peaks, by=["pcode"], col_name="fe_max", ascending=False
    )
    return (df_combined,)


@app.cell
def _(df_combined, np, pd):
    total_years = df_combined["year"].nunique()

    dicts = []
    for _rp in df_combined["fe_max_rp"].unique():
        _dff = df_combined[df_combined["fe_max_rp"] >= _rp]
        dicts.append(
            {
                "rp_ind": round(_rp, 4),
                "rp_combined": (
                    round((total_years + 1) / _dff["year"].nunique(), 4)
                    if _dff["year"].nunique() > 0
                    else np.inf
                ),
            }
        )

    df_rps = pd.DataFrame(dicts)
    return (df_rps,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Combined return periods

    Now we'll look more closely at the return periods. Assuming that we have symmetrical return periods within LGAs (ie. they are all the same frequency of occurrence), the plot below will tell us what the resulting overall return period will be.
    """
    )
    return


@app.cell
def _(df_rps, plt):
    _fig, _ax = plt.subplots(dpi=200, figsize=(12, 6))

    df_rps.sort_values("rp_ind").plot(
        x="rp_ind", y="rp_combined", legend=False, ax=_ax
    )

    _ax.set_xlabel("Individual LGA RP (years)")
    _ax.set_ylabel("Combined  RP (years)")

    _ax.spines["top"].set_visible(False)
    _ax.spines["right"].set_visible(False)
    _ax.grid(True, linestyle="--", alpha=0.7)
    _fig
    return


if __name__ == "__main__":
    app.run()
