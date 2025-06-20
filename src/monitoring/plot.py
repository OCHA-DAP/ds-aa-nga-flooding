import io

import matplotlib.pyplot as plt
import ocha_stratus as stratus
import pandas as pd

from src.utils.blob import PROJECT_PREFIX


def combined_plots(df, glofas_thresh, google_thresh, save_output=True):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    assert df.updated.nunique() == 1
    update_date = df.updated.unique()[0].strftime("%Y-%m-%d %H:%M:%S")

    df_forecast = df[df.src.str.contains("forecast")]
    df_reanalysis = df[df.src.str.contains("reanalysis")]
    df_google = df[df.src.str.contains("hybas")]

    # We're taking the forecast issue date for GloFAS (not the reanalysis)
    glofas_update = df_forecast.issued_date[0].strftime("%Y-%m-%d %H:%M:%S")
    google_update = df_google.issued_date[0].strftime("%Y-%m-%d %H:%M:%S")

    glofas_exceeds = (df_reanalysis.value.any() > glofas_thresh) | (
        df_forecast.value.any() > glofas_thresh
    )
    google_exceeds = df_google.value.any() > google_thresh
    overall_exceeds = glofas_exceeds | google_exceeds

    forecast_subplot(
        ax1,
        df_forecast,
        df_reanalysis,
        glofas_exceeds,
        glofas_thresh,
        "GloFAS",
        glofas_update,
    )
    forecast_subplot(
        ax2,
        df_google,
        None,
        google_exceeds,
        google_thresh,
        "Google",
        google_update,
    )

    if save_output:
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", bbox_inches="tight", dpi=300)
        buffer.seek(0)
        container_client = stratus.get_container_client(
            "projects", "dev", write=True
        )
        blob_name = (
            f"{PROJECT_PREFIX}/monitoring/{update_date}_{overall_exceeds}.png"
        )

        container_client.upload_blob(
            name=blob_name, data=buffer.getvalue(), overwrite=True
        )
        print(f"File saved on blob to {blob_name}!")


def forecast_subplot(
    ax, df_forecast, df_reanalysis, exceeds, thresh, dataset, date
):
    ax.plot(
        df_forecast["valid_time"],
        df_forecast["value"],
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=4,
        label="Forecast",
        color="blue",
        alpha=0.8,
    )

    for _, row in df_forecast.iterrows():
        ax.annotate(
            f'{row["value"]:.1f}',  # noqa
            (row["valid_time"], row["value"]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
            color="blue",
        )

    # Add horizontal threshold line
    ax.axhline(
        y=thresh,
        color="black",
        linestyle="--",
        linewidth=2,
        label=f"Trigger Threshold ({thresh})",
        alpha=0.8,  # noqa
    )

    if isinstance(df_reanalysis, pd.DataFrame):
        ax.plot(
            df_reanalysis["valid_time"],
            df_reanalysis["value"],
            marker="s",
            linestyle="-",
            linewidth=2,
            markersize=6,
            label="Reanalysis",
            color="red",
            alpha=0.8,
        )

        # Add labels for reanalysis points
        for _, row in df_reanalysis.iterrows():
            ax.annotate(
                f'{row["value"]:.1f}',  # noqa
                (row["valid_time"], row["value"]),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=8,
                color="red",
            )

    title = f"{dataset} Monitoring: {date} | Triggers = {exceeds}"

    ax.set_ylabel("Discharge, daily average (m$^3$ / s)", fontsize=12)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
