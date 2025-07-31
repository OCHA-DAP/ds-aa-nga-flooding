import numpy as np
import xarray as xr


def compute_density_from_grid(da, lat_name="lat", lon_name="lon"):
    dlat = np.abs(da[lat_name].diff(lat_name).mean().item())  # degrees
    dlon = np.abs(da[lon_name].diff(lon_name).mean().item())  # degrees

    km_per_deg = 111.32  # average km per degree of latitude
    lat_km = dlat * km_per_deg  # north-south length in km

    # Compute east-west length as function of latitude
    lat_radians = np.deg2rad(da[lat_name])
    lon_km = dlon * km_per_deg * np.cos(lat_radians)

    area_km2 = xr.DataArray(
        lat_km * lon_km, coords={lat_name: da[lat_name]}, dims=lat_name
    )
    area_2d = area_km2.broadcast_like(da)

    da_density = da / area_2d
    da_density.name = "population_density"
    da_density.attrs["units"] = "people per km²"
    return da_density


def compute_pop_from_density(da_density, lat_name="lat", lon_name="lon"):
    dlat = np.abs(da_density[lat_name].diff(lat_name).mean().item())  # degrees
    dlon = np.abs(da_density[lon_name].diff(lon_name).mean().item())  # degrees

    km_per_deg = 111.32
    lat_km = dlat * km_per_deg

    lat_radians = np.deg2rad(da_density[lat_name])
    lon_km = dlon * km_per_deg * np.cos(lat_radians)

    area_km2 = xr.DataArray(
        lat_km * lon_km, coords={lat_name: da_density[lat_name]}, dims=lat_name
    )
    area_2d = area_km2.broadcast_like(da_density)

    da_pop = da_density * area_2d
    da_pop.name = "population_per_pixel"
    da_pop.attrs["units"] = "people per pixel"
    return da_pop
