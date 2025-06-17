import io

import matplotlib.pyplot as plt
import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

GLOFAS_THRESH = 3132
GOOGLE_THRESH = 1195


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

    ax.set_ylabel("Streamflow", fontsize=12)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)


if __name__ == "__main__":
    engine = stratus.get_engine(stage="dev")
    with engine.connect() as con:
        df = pd.read_sql(
            """
            select * from monitoring.nga_cerf_flooding
            where updated = (select max(updated) from
            monitoring.nga_cerf_flooding)
            """,
            con=con,
        )

    assert df.updated.nunique() == 1
    update_date = df.updated.unique()[0].strftime("%Y-%m-%d %H:%M:%S")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    df_forecast = df[df.src.str.contains("forecast")]
    df_reanalysis = df[df.src.str.contains("reanalysis")]
    df_google = df[df.src.str.contains("hybas")]

    glofas_exceeds = (df_reanalysis.value.any() > GLOFAS_THRESH) | (
        df_forecast.value.any() > GLOFAS_THRESH
    )
    google_exceeds = df_google.value.any() > GOOGLE_THRESH
    overall_exceeds = glofas_exceeds | google_exceeds

    forecast_subplot(
        ax1,
        df_forecast,
        df_reanalysis,
        glofas_exceeds,
        GLOFAS_THRESH,
        "GloFAS",
        update_date,
    )
    forecast_subplot(
        ax2,
        df_google,
        None,
        google_exceeds,
        GOOGLE_THRESH,
        "Google",
        update_date,
    )
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", bbox_inches="tight", dpi=300)
    buffer.seek(0)
    container_client = stratus.get_container_client(
        "projects", "dev", write=True
    )
    blob_name = (
        f"ds-aa-nga-flooding/monitoring/{update_date}_{overall_exceeds}.png"
    )

    container_client.upload_blob(
        name=blob_name, data=buffer.getvalue(), overwrite=True
    )
    print(f"File saved on blob to {blob_name}!")

    buffer.close()
