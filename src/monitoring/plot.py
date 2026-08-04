import io

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import ocha_stratus as stratus
from matplotlib.ticker import FuncFormatter

from src.constants import (
    ACTION_GAUGE_THRESHOLDS,
    ACTION_MIN_GAUGES,
    PROJECT_PREFIX,
    READINESS_GLOFAS_THRESH,
)
from src.monitoring import etl


def combined_plots(df, save_output=True):
    assert df.monitoring_date.nunique() == 1
    update_date = df.monitoring_date.unique()[0].strftime("%Y-%m-%d")
    status = etl.evaluate_trigger(df)

    df_glofas = df[df.src.str.contains("glofas_forecast")].reset_index()
    df_reanalysis = df[df.src.str.contains("glofas_reanalysis")].reset_index()
    df_google = df[df.src.str.startswith("grrr_")].copy()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    readiness_subplot(ax1, df_glofas, df_reanalysis, status)
    action_subplot(ax2, df_google, status)

    if save_output:
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", bbox_inches="tight", dpi=300)
        buffer.seek(0)
        container_client = stratus.get_container_client(
            "projects", "dev", write=True
        )
        blob_name = (
            f"{PROJECT_PREFIX}/monitoring/{update_date}_"
            f"{status['action']}.png"
        )

        container_client.upload_blob(
            name=blob_name, data=buffer.getvalue(), overwrite=True
        )
        print(f"File saved on blob to {blob_name}!")
        buffer.close()


def _format_dates(ax):
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%B %-d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.tick_params(axis="x", rotation=45)


def readiness_subplot(ax, df_glofas, df_reanalysis, status):
    issue_date = df_glofas.issued_date[0].strftime("%Y-%m-%d")
    ax.plot(
        df_glofas["valid_date"],
        df_glofas["value"],
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=4,
        label="GloFAS forecast (ensemble mean)",
        color="blue",
        alpha=0.8,
    )
    for _, row in df_glofas.iterrows():
        ax.annotate(
            f'{row["value"]:.0f}',  # noqa
            (row["valid_date"], row["value"]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
            color="blue",
        )
    if len(df_reanalysis):
        ax.plot(
            df_reanalysis["valid_date"],
            df_reanalysis["value"],
            marker="s",
            linestyle="-",
            linewidth=2,
            markersize=6,
            label="GloFAS reanalysis (latest available)",
            color="red",
            alpha=0.8,
        )
        for _, row in df_reanalysis.iterrows():
            ax.annotate(
                f'{row["value"]:.0f}',  # noqa
                (row["valid_date"], row["value"]),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=8,
                color="red",
            )
    ax.axhline(
        y=READINESS_GLOFAS_THRESH,
        color="black",
        linestyle="--",
        linewidth=2,
        label=f"Readiness threshold ({READINESS_GLOFAS_THRESH:,.0f})",
        alpha=0.8,
    )

    def format_thousands(x, pos):
        return f"{x:,.0f}"  # noqa

    ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))
    ax.set_ylim(0, None)
    ax.set_ylabel("Discharge, daily average (m$^3$ / s)", fontsize=12)
    ax.set_title(
        f"Readiness trigger — GloFAS at Wuroboki, issued {issue_date} | "
        f"Triggered = {status['readiness']}",
        fontsize=12,
        fontweight="bold",
    )
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    _format_dates(ax)


def action_subplot(ax, df_google, status):
    issue_date = df_google.issued_time.iloc[0].strftime("%Y-%m-%d")
    df_google = df_google.copy()
    df_google["gauge_id"] = df_google.src.str.removeprefix("grrr_")
    df_google["threshold"] = df_google.gauge_id.map(ACTION_GAUGE_THRESHOLDS)
    df_google["pct_of_threshold"] = (
        df_google.value / df_google.threshold * 100
    )

    for gauge_id, df_gauge in df_google.groupby("gauge_id"):
        ax.plot(
            df_gauge["valid_date"],
            df_gauge["pct_of_threshold"],
            marker="o",
            linestyle="-",
            linewidth=1.5,
            markersize=3,
            label=gauge_id,
            alpha=0.7,
        )
    ax.axhline(
        y=100,
        color="black",
        linestyle="--",
        linewidth=2,
        label="Gauge 4-yr RP threshold",
        alpha=0.8,
    )
    n_gauges = len(ACTION_GAUGE_THRESHOLDS)
    ax.set_ylim(0, None)
    ax.set_ylabel("Forecast, % of gauge threshold", fontsize=12)
    ax.set_title(
        f"Action trigger — Google gauges, issued {issue_date} | "
        f"max {status['max_gauges_exceeding']}/{n_gauges} gauges over "
        f"threshold on one day (needs ≥{ACTION_MIN_GAUGES}) | "
        f"Triggered = {status['action']}",
        fontsize=12,
        fontweight="bold",
    )
    ax.legend(fontsize=8, ncols=2)
    ax.grid(True, alpha=0.3)
    _format_dates(ax)
