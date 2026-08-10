import io

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import ocha_stratus as stratus
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

from src.constants import (
    ACTION_GAUGE_THRESHOLDS,
    ACTION_MIN_GAUGES,
    PROJECT_PREFIX,
    READINESS_GLOFAS_THRESH,
)
from src.monitoring import etl

# HDX design tokens (assets/tokens.css in the HDX style guide)
HDX_PRIMARY = "#1862d8"  # primary-5: GloFAS forecast
HDX_PRIMARY_MUTED = "#a3c0ef"  # primary-2: individual gauge lines
HDX_BRAND = "#269777"  # brand-5: GloFAS reanalysis
HDX_ERROR = "#c44536"  # error-5: thresholds / exceedance / triggered
HDX_SUCCESS = "#2f9e6f"  # success-5: not-triggered status
HDX_TEXT = "#1f2324"  # neutral-9
HDX_SUBTEXT = "#5e6a6b"  # neutral-7
HDX_SPINE = "#d8e0e1"  # neutral-2
HDX_GRID = "#ebeff0"  # neutral-1

FONT_STACK = ["Roboto", "Helvetica Neue", "Arial", "DejaVu Sans"]

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": FONT_STACK,
        "text.color": HDX_TEXT,
        "axes.edgecolor": HDX_SPINE,
        "axes.labelcolor": HDX_SUBTEXT,
        "xtick.color": HDX_SUBTEXT,
        "ytick.color": HDX_SUBTEXT,
        "axes.titlelocation": "left",
        "axes.titleweight": "semibold",
        "axes.titlesize": 12,
        "axes.titlecolor": HDX_TEXT,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
    }
)


def _style_axes(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color=HDX_GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(labelsize=9, length=3, color=HDX_SPINE)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%-d %b"))


def combined_plots(df, save_output=True):
    assert df.monitoring_date.nunique() == 1
    update_date = df.monitoring_date.unique()[0].strftime("%Y-%m-%d")
    status = etl.evaluate_trigger(df)

    df_glofas = df[df.src.str.contains("glofas_forecast")].reset_index()
    df_reanalysis = df[df.src.str.contains("glofas_reanalysis")].reset_index()
    df_google = df[df.src.str.startswith("grrr_")].copy()

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 8.5), sharex=True, dpi=200
    )
    fig.subplots_adjust(top=0.86, hspace=0.35)

    if status["action"]:
        status_text, status_color = "ACTION TRIGGER REACHED", HDX_ERROR
    elif status["readiness"]:
        status_text, status_color = "READINESS TRIGGER REACHED", HDX_ERROR
    else:
        status_text, status_color = "Not activated", HDX_SUCCESS

    fig.text(
        0.07,
        0.965,
        "Nigeria Anticipatory Action — Adamawa riverine flooding",
        fontsize=15,
        fontweight="bold",
        color=HDX_TEXT,
    )
    fig.text(
        0.07,
        0.925,
        f"Forecasts retrieved {update_date}   ·   Status: ",
        fontsize=11,
        color=HDX_SUBTEXT,
    )
    fig.text(
        0.365,
        0.925,
        status_text,
        fontsize=11,
        fontweight="bold",
        color=status_color,
    )

    readiness_subplot(ax1, df_glofas, df_reanalysis, status)
    action_subplot(ax2, df_google, status)

    if save_output:
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", bbox_inches="tight", dpi=200)
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


def readiness_subplot(ax, df_glofas, df_reanalysis, status):
    ax.plot(
        df_glofas["valid_date"],
        df_glofas["value"],
        marker="o",
        linewidth=2,
        markersize=3.5,
        color=HDX_PRIMARY,
        label="GloFAS forecast (ensemble mean)",
    )
    # annotate the peak forecast value only
    peak = df_glofas.loc[df_glofas["value"].idxmax()]
    ax.annotate(
        f'{peak["value"]:,.0f}',  # noqa
        (peak["valid_date"], peak["value"]),
        textcoords="offset points",
        xytext=(0, 8),
        ha="center",
        fontsize=9,
        fontweight="bold",
        color=HDX_PRIMARY,
    )
    if len(df_reanalysis):
        ax.plot(
            df_reanalysis["valid_date"],
            df_reanalysis["value"],
            marker="s",
            linewidth=2,
            markersize=5,
            color=HDX_BRAND,
            label="GloFAS reanalysis (latest available)",
        )
        for _, row in df_reanalysis.iterrows():
            ax.annotate(
                f'{row["value"]:,.0f}',  # noqa
                (row["valid_date"], row["value"]),
                textcoords="offset points",
                xytext=(0, 8),
                ha="center",
                fontsize=9,
                fontweight="bold",
                color=HDX_BRAND,
            )
    ax.axhline(
        y=READINESS_GLOFAS_THRESH,
        color=HDX_ERROR,
        linestyle=(0, (4, 3)),
        linewidth=1.5,
    )
    ax.annotate(
        f"Readiness threshold — {READINESS_GLOFAS_THRESH:,.0f} m³/s",
        (0.99, READINESS_GLOFAS_THRESH),
        xycoords=("axes fraction", "data"),
        textcoords="offset points",
        xytext=(0, 5),
        ha="right",
        fontsize=9,
        color=HDX_ERROR,
    )

    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x:,.0f}"))
    ax.set_ylim(0, max(READINESS_GLOFAS_THRESH * 1.15, 1))
    ax.set_ylabel("Discharge (m³/s, daily average)", fontsize=10)
    ax.set_title(
        "Readiness trigger — GloFAS at Wuroboki\n",
        pad=10,
    )
    ax.legend(
        loc="upper left",
        fontsize=9,
        frameon=True,
        facecolor="white",
        edgecolor="none",
        framealpha=0.9,
        labelcolor=HDX_SUBTEXT,
    )
    _style_axes(ax)


def action_subplot(ax, df_google, status):
    df_google = df_google.copy()
    df_google["gauge_id"] = df_google.src.str.removeprefix("grrr_")
    df_google["threshold"] = df_google.gauge_id.map(ACTION_GAUGE_THRESHOLDS)
    df_google["pct_of_threshold"] = (
        df_google.value / df_google.threshold * 100
    )

    any_exceeding = False
    for gauge_id, df_gauge in df_google.groupby("gauge_id"):
        exceeds = bool((df_gauge["pct_of_threshold"] >= 100).any())
        any_exceeding |= exceeds
        ax.plot(
            df_gauge["valid_date"],
            df_gauge["pct_of_threshold"],
            marker="o",
            linewidth=1.8 if exceeds else 1.2,
            markersize=2.5,
            color=HDX_ERROR if exceeds else HDX_PRIMARY_MUTED,
            zorder=3 if exceeds else 2,
        )
    ax.axhline(
        y=100,
        color=HDX_ERROR,
        linestyle=(0, (4, 3)),
        linewidth=1.5,
    )
    ax.annotate(
        "Gauge 4-yr return period threshold",
        (0.99, 100),
        xycoords=("axes fraction", "data"),
        textcoords="offset points",
        xytext=(0, 5),
        ha="right",
        fontsize=9,
        color=HDX_ERROR,
    )

    n_gauges = len(ACTION_GAUGE_THRESHOLDS)
    handles = [
        Line2D([], [], color=HDX_PRIMARY_MUTED, linewidth=1.2),
    ]
    labels = [f"Individual Google gauges (n={n_gauges}), % of own threshold"]
    if any_exceeding:
        handles.append(Line2D([], [], color=HDX_ERROR, linewidth=1.8))
        labels.append("Gauge exceeding its threshold")
    ax.legend(
        handles,
        labels,
        loc="upper left",
        fontsize=9,
        frameon=True,
        facecolor="white",
        edgecolor="none",
        framealpha=0.9,
        labelcolor=HDX_SUBTEXT,
    )

    ax.set_ylim(0, max(115, df_google["pct_of_threshold"].max() * 1.1))
    ax.set_ylabel("Forecast (% of gauge threshold)", fontsize=10)
    ax.set_title(
        f"Action trigger — Google Flood Hub gauges  "
        f"(max {status['max_gauges_exceeding']}/{n_gauges} over threshold "
        f"on a single day; ≥{ACTION_MIN_GAUGES} activates)\n",
        pad=10,
    )
    _style_axes(ax)
