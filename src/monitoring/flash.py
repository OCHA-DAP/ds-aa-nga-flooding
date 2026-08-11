"""NHF flash-flood observational trigger: data access, evaluation, plot.

Indicator: daily FloodScan SFED x WorldPop population exposure per target
LGA, produced by the live floodexposure-monitoring pipeline
(DB app.floodscan_exposure, prod stage), smoothed with a 3-day rolling
mean. The trigger fires when any LGA's rolling exposure exceeds its
endorsed threshold. LGAs whose threshold is still None are monitored but
cannot fire.
"""

import io

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import ocha_stratus as stratus
import pandas as pd
from sqlalchemy import text

from src.constants import (
    FLASH_LGAS,
    FLASH_ROLLING_DAYS,
    PROJECT_PREFIX,
)
from src.monitoring.plot import (
    HDX_ERROR,
    HDX_GRID,
    HDX_PRIMARY,
    HDX_SPINE,
    HDX_SUBTEXT,
    HDX_SUCCESS,
    HDX_TEXT,
)


def load_exposure(days=120):
    """Daily exposure per target LGA from the floodexposure-monitoring
    pipeline's DB table, with the rolling mean used by the trigger."""
    engine = stratus.get_engine(stage="prod")
    with engine.connect() as con:
        df = pd.read_sql(
            text(
                """
            select valid_date, pcode, sum as exposure
            from app.floodscan_exposure
            where pcode = any(:pcodes)
            and adm_level = '2'
            and valid_date >= current_date - :days
            order by pcode, valid_date
            """
            ),
            con=con,
            params={"pcodes": list(FLASH_LGAS), "days": days},
        )
    if df.empty:
        raise Exception("No exposure data returned for flash-flood LGAs")
    df["valid_date"] = pd.to_datetime(df["valid_date"])
    # Strict window (no min_periods) — matches the threshold derivation and
    # the 2025 monitoring convention
    df["rolling"] = df.groupby("pcode")["exposure"].transform(
        lambda x: x.rolling(FLASH_ROLLING_DAYS).mean()
    )
    return df


def evaluate_flash(df):
    """Evaluate the flash trigger per LGA on the latest available date."""
    latest_date = df["valid_date"].max()
    lgas = {}
    for pcode, cfg in FLASH_LGAS.items():
        df_lga = df[df.pcode == pcode]
        latest = df_lga[df_lga.valid_date == latest_date]
        rolling = float(latest["rolling"].iloc[0]) if len(latest) else None
        threshold = cfg["threshold"]
        exceeds = (
            rolling is not None
            and threshold is not None
            and rolling >= threshold
        )
        lgas[pcode] = {
            "name": cfg["name"],
            "rolling": rolling,
            "threshold": threshold,
            "exceeds": exceeds,
        }
    return {
        "triggered": any(lga["exceeds"] for lga in lgas.values()),
        "thresholds_pending": any(
            lga["threshold"] is None for lga in lgas.values()
        ),
        "latest_date": latest_date,
        "lgas": lgas,
    }


def flash_plot(df, status, save_output=True):
    """2x2 small multiples: rolling exposure per LGA vs its threshold,
    HDX-styled to match the riverine chart."""
    latest_date = status["latest_date"].strftime("%Y-%m-%d")
    fig, axs = plt.subplots(
        2, 2, figsize=(11, 7.5), sharex=True, dpi=200
    )
    fig.subplots_adjust(top=0.82, hspace=0.4, wspace=0.25)

    if status["triggered"]:
        status_text, status_color = "FLASH FLOOD TRIGGER REACHED", HDX_ERROR
    else:
        status_text, status_color = "Not activated", HDX_SUCCESS

    fig.text(
        0.07,
        0.955,
        "Nigeria Anticipatory Action — flash flooding (BAY states)",
        fontsize=15,
        fontweight="bold",
        color=HDX_TEXT,
    )
    fig.text(
        0.07,
        0.905,
        f"FloodScan exposure to {latest_date}   ·   Status: ",
        fontsize=11,
        color=HDX_SUBTEXT,
    )
    fig.text(
        0.375,
        0.905,
        status_text,
        fontsize=11,
        fontweight="bold",
        color=status_color,
    )

    for ax, (pcode, lga) in zip(axs.flat, status["lgas"].items()):
        df_lga = df[df.pcode == pcode]
        color = HDX_ERROR if lga["exceeds"] else HDX_PRIMARY
        ax.plot(
            df_lga["valid_date"],
            df_lga["rolling"],
            linewidth=1.8,
            color=color,
        )
        if lga["threshold"] is not None:
            ax.axhline(
                y=lga["threshold"],
                color=HDX_ERROR,
                linestyle=(0, (4, 3)),
                linewidth=1.3,
            )
            thresh_note = f"threshold {lga['threshold']:,.0f}"
        else:
            thresh_note = "threshold pending"
        latest_txt = (
            f"{lga['rolling']:,.0f}" if lga["rolling"] is not None else "n/a"
        )
        ax.set_title(
            f"{lga['name']} — {latest_txt} exposed ({thresh_note})",
            fontsize=10.5,
        )
        ax.set_ylim(0, None)
        top = ax.get_ylim()[1]
        if lga["threshold"] is not None:
            ax.set_ylim(0, max(top, lga["threshold"] * 1.15))
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", color=HDX_GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        ax.tick_params(labelsize=8.5, length=3, color=HDX_SPINE)
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-d %b"))
    fig.supylabel(
        f"People exposed ({FLASH_ROLLING_DAYS}-day rolling mean)",
        fontsize=10,
        color=HDX_SUBTEXT,
    )

    if save_output:
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", bbox_inches="tight", dpi=200)
        buffer.seek(0)
        container_client = stratus.get_container_client(
            "projects", "dev", write=True
        )
        blob_name = get_flash_plot_blob_name(
            latest_date, status["triggered"]
        )
        container_client.upload_blob(
            name=blob_name, data=buffer.getvalue(), overwrite=True
        )
        print(f"File saved on blob to {blob_name}!")
        buffer.close()


def get_flash_plot_blob_name(latest_date, triggered):
    return (
        f"{PROJECT_PREFIX}/monitoring/flash_{latest_date}_{triggered}.png"
    )
