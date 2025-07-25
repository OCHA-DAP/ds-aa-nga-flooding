import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.image(
        src="exploration/2025/assets/UNOCHA_logo_horizontal_blue_CMYK.png",
        height=100,
    ).center()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # 2025 Framework Monitoring

    This notebook can be used internally for monitoring the 2025 riverine trigger (from Google and GloFAS models) and the flash flooding observational trigger (from Floodscan flood exposure).
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.callout(
        "This notebook is still a work in progress, and meant for internal use. Please contact hannah.ker@un.org at the OCHA Centre for Humanitarian Data for any questions.",
        kind="warn",
    )
    return


@app.cell
def _():
    import io

    import ocha_stratus as stratus
    from PIL import Image

    return Image, io, stratus


@app.cell
def _(mo):
    date = mo.ui.date(label="Select a monitoring date")
    return (date,)


@app.cell
def _(date):
    date
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Riverine Trigger

    See the plot below for an overview of streamflow forecasts along the Benue river from both GloFAS and Google. The data behind these plots can be verified at:

    - Google's [Flood Hub](https://sites.research.google/floods/l/9.394871245232991/12.36785888671875/10/g/hybas_1120842550)
    - GloFAS's [flood monitoring interface](https://global-flood.emergency.copernicus.eu/glofas-forecasting/)
    """
    )
    return


@app.cell
def _(date):
    monitoring_date = date.value.strftime("%Y-%m-%d")
    return (monitoring_date,)


@app.cell
def _(monitoring_date, stratus):
    files = stratus.list_container_blobs(
        name_starts_with=f"ds-aa-nga-flooding/monitoring/{monitoring_date}"
    )
    assert len(files) == 1
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
    mo.md(r"""## Flash Flooding Trigger (Observational)""")
    return


if __name__ == "__main__":
    app.run()
