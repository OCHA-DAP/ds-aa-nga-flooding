import marimo

__generated_with = "0.13.6"
app = marimo.App(app_title="NGA Monitoring")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    img1 = mo.image(
        src="exploration/2025/assets/UNOCHA_logo_horizontal_blue_CMYK.png",
        height=100,
    ).center()

    img2 = mo.image(
        src="exploration/2025/assets/centre_logo.png",
        height=100,
    ).center()

    mo.hstack([img1, img2])
    return


@app.cell
def _(mo):
    mo.center(mo.md("# 2025 Framework Monitoring"))
    return


@app.cell
def _(mo):
    mo.Html("<hr></hr>")
    return


@app.cell
def _(mo):
    mo.callout(
        "This notebook is still a work in progress, and meant for internal use. Please contact hannah.ker@un.org at the OCHA Centre for Humanitarian Data for any questions.",
        kind="warn",
    )
    return


@app.cell
def _(mo):
    date = mo.ui.date(label="**Select a monitoring date:**")
    return (date,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        rf"""This notebook supports monitoring for the 2025 [OCHA Anticipatory Action](https://www.unocha.org/anticipatory-action) framework in Nigeria. It has been developed by the [Centre for Humanitarian Data](https://centre.humdata.org/). Select a monitoring date (defaults to today) and see below for daily updates from data sources to support the riverine trigger and observational flash flooding trigger."""
    )
    return


@app.cell
def _(date, mo):
    mo.center(date)
    return


@app.cell
def _():
    import io
    from datetime import timedelta

    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import ocha_stratus as stratus
    import pandas as pd
    from PIL import Image
    from sqlalchemy import text

    return Image, io, mdates, pd, plt, stratus, text, ticker, timedelta


@app.cell
def _():
    STAGE = "prod"
    return (STAGE,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Riverine Trigger

    See the plot below for an overview of streamflow forecasts along the Benue river from both GloFAS and Google. The data behind these plots can be verified at Google's [Flood Hub](https://sites.research.google/floods/l/9.394871245232991/12.36785888671875/10/g/hybas_1120842550) and GloFAS's [flood monitoring interface](https://global-flood.emergency.copernicus.eu/glofas-forecasting/)
    """
    )
    return


@app.cell
def _(date):
    monitoring_date = date.value.strftime("%Y-%m-%d")
    return (monitoring_date,)


@app.cell
def _(date, mo, monitoring_date, stratus):
    files = stratus.list_container_blobs(
        name_starts_with=f"ds-aa-nga-flooding/monitoring/{monitoring_date}"
    )

    mo.stop(
        len(files) == 0,
        mo.md(f"**No monitoring data available for {date.value}**"),
    )
    mo.stop(len(files) > 1, mo.md("**Unexpected number of files saved**"))

    file_name = files[0]
    return (file_name,)


@app.cell
def _(Image, file_name, io, stratus):
    plot_data = stratus.load_blob_data(blob_name=file_name)
    image = Image.open(io.BytesIO(plot_data))
    return (image,)


@app.cell
def _(image, mo):
    mo.image(image)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Flash Flooding Trigger

    This trigger is based on an analysis of Floodscan data -- the results of which can be see on [this application](https://chd-ds-floodexposure-monitoring.azurewebsites.net/). Note that flood exposure estimates have an update lag of one day. Today's data will be available after 11pm UTC tomorrow.
    """
    )
    return


@app.cell
def _():
    lga_config = {
        "Bade": {"pcode": "NG036001", "threshold": 32124},
        "Bama": {"pcode": "NG008003", "threshold": 12334},
        "Karasuwa": {"pcode": "NG036010", "threshold": 4296},
        "Madagali": {"pcode": "NG002010", "threshold": 6856},
        "Maiduguri": {"pcode": "NG008021", "threshold": 27424},
        "Ngala": {"pcode": "NG008025", "threshold": 126086},
    }

    ROLLING_WINDOW = 3
    return ROLLING_WINDOW, lga_config


@app.cell
def _(STAGE, date, lga_config, pd, stratus, text, timedelta):
    pcode_list = [value["pcode"] for _, value in lga_config.items()]

    # Convert list to tuple for SQL IN operator
    query = text(
        """
    SELECT *
    FROM app.floodscan_exposure
    WHERE pcode IN :pcode_list
    AND valid_date BETWEEN :start_date AND :input_date
    """
    )

    params = {
        "pcode_list": tuple(pcode_list),
        "input_date": date.value,
        "start_date": date.value - timedelta(days=21),
    }

    # Rest of your code remains the same
    engine = stratus.get_engine(STAGE)
    with engine.connect() as con:
        df_exposure = pd.read_sql_query(
            query,
            con,
            params=params,
        )
    return (df_exposure,)


@app.cell
def _(ROLLING_WINDOW, date, df_exposure, mo):
    mo.stop(
        df_exposure.valid_date.max() != date.value,
        mo.md(f"**No monitoring data available for {date.value}**"),
    )

    df_exposure_rolling = df_exposure.sort_values(["pcode", "valid_date"])
    df_exposure_rolling["rolling_avg"] = df_exposure_rolling.groupby("pcode")[
        "sum"
    ].transform(lambda x: x.rolling(ROLLING_WINDOW).mean())
    return (df_exposure_rolling,)


@app.cell
def _(
    df_exposure_rolling,
    lga_config,
    mdates,
    monitoring_date,
    plt,
    ticker,
    today,
):
    fig, axes = plt.subplots(3, 2, figsize=(15, 10))
    fig.suptitle(
        "Flood Exposure Monitoring: 3-Day Rolling Averages", fontsize=16
    )
    axes = axes.flatten()

    for i, (lga_name, config) in enumerate(lga_config.items()):
        if i >= len(axes):  # Safety check
            break
        ax = axes[i]

        # Filter data for this LGA
        pcode = config["pcode"]
        threshold = config["threshold"]
        lga_data = df_exposure_rolling[
            df_exposure_rolling["pcode"] == pcode
        ].copy()

        if len(lga_data) > 0:
            # Get today's data point
            today_data = lga_data[lga_data["valid_date"] == monitoring_date]

            # Plot rolling average (muted)
            ax.plot(
                lga_data["valid_date"],
                lga_data["rolling_avg"],
                "o-",
                color="blue",
                alpha=0.6,
                linewidth=2,
                markersize=4,
                label="3-Day Rolling Avg",
            )

            # Highlight today's points
            if len(today_data) > 0:
                today_rolling = today_data["rolling_avg"].iloc[0]

                ax.plot(
                    today,
                    today_rolling,
                    "o",
                    color="blue",
                    markersize=12,
                    markeredgewidth=2,
                    markeredgecolor="white",
                    label=f"Today's Rolling Avg ({today_rolling:,.1f})",
                    zorder=10,
                )

                # Check if today is above threshold
                if today_rolling > threshold:
                    ax.plot(
                        today,
                        today_rolling,
                        "s",
                        color="red",
                        markersize=16,
                        markeredgewidth=3,
                        markeredgecolor="yellow",
                        alpha=0.8,
                        zorder=11,
                    )

            ax.axhline(
                y=threshold,
                color="black",
                linestyle="--",
                linewidth=1.5,
                alpha=0.7,
                label=f"Threshold ({threshold:,})",
            )

        ax.set_title(f"{lga_name}", fontweight="bold")
        if i % 3 == 0:
            ax.set_ylabel("Population exposed to flooding")
        else:
            ax.set_ylabel("")
        if i >= 3:
            ax.set_xlabel("Date")
        else:
            ax.set_xlabel("")

        ax.legend(fontsize=7, loc="upper left")
        ax.grid(True, alpha=0.3)

        # Format y-axis with comma separators
        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda x, p: f"{x:,.0f}")
        )

        # Format x-axis dates as "July 7"
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%B %d"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    # Hide any unused subplots
    for j in range(len(lga_config), len(axes)):
        axes[j].set_visible(False)
    plt.tight_layout()
    fig
    return


if __name__ == "__main__":
    app.run()
