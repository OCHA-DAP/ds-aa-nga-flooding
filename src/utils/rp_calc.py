from typing import List, Optional, Union

import pandas as pd
from scipy import stats


def calculate_one_group_rp(group, col_name: str = "q", ascending: bool = True):
    """Calculate the empirical RP for a single group.

    Parameters
    ----------
    group : pd.DataFrame
        The group for which to calculate the RP.
    col_name : str, optional
        The name of the column for which to calculate the RP, by default "q".
    ascending : bool, optional
        Whether to rank the column in ascending order, by default True.
        Should be False for cases where a high number is severe
        (e.g. precipitation for flooding), and True for cases where a low
        number is severe (e.g. precipitation for drought).

    Returns
    -------
    pd.DataFrame
        The input group with the RP columns added.
    """
    group[f"{col_name}_rank"] = group[col_name].rank(ascending=ascending)
    group[f"{col_name}_rp"] = (len(group) + 1) / group[f"{col_name}_rank"]
    return group


def calculate_groups_rp(
    df: pd.DataFrame, by: List, col_name: str = "mean", ascending: bool = True
):
    """Calculate the empirical RP for each group in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame for which to calculate the RP.
    by : List
        The columns by which to group the DataFrame.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with the RP columns added.
    """
    return (
        df.groupby(by)
        .apply(
            calculate_one_group_rp,
            include_groups=False,
            col_name=col_name,
            ascending=ascending,
        )
        .reset_index()
        .drop(columns=f"level_{len(by)}")
    )


def estimate_return_periods(
    df: pd.DataFrame,
    date_col: str,
    val_col: str,
    target_rps: Optional[List[Union[int, float]]] = None,
) -> pd.DataFrame:
    """
    Estimate return periods for extreme values using a Gumbel distribution.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataframe containing time series data
    date_col : str
        Name of the column containing dates
    val_col : str
        Name of the column containing values (e.g., streamflow)
    target_rps : array_like, optional
        List or array of target return periods in years
        Default is [2, 3, 5, 7, 10]

    Returns
    -------
    pandas.DataFrame
        DataFrame with two columns:
        - 'return_period': The requested return periods
        - 'value': The estimated values corresponding to each return period
    """
    if target_rps is None:
        target_rps = [2, 3, 5, 7, 10]

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["year"] = df[date_col].dt.year
    df_annual_max = df.groupby("year")[val_col].max().reset_index()
    loc, scale = stats.gumbel_r.fit(df_annual_max[val_col])

    target_exceedance_probs = 1 / pd.Series(target_rps)

    values = stats.gumbel_r.ppf(
        1 - target_exceedance_probs, loc=loc, scale=scale
    )
    df_rp_calculated = pd.DataFrame(
        {"return_period": target_rps, "value": values}
    )

    return df_rp_calculated


def get_rp_val(df, rp_val, rp_col="return_period", val_col="value"):
    return float(df.loc[df[rp_col] == rp_val, val_col].iloc[0])
