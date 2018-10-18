"""A collection of useful functions to be used for AQMesh data analysis."""

import datetime as dt
import pandas as pd
import plotly.graph_objs as go

from query import Query  # custom
from pandas.io import gbq  # for running queries

from typing import Tuple, Optional, Union  # for typing support


def read_ts(data: Union[list, str],
            stationID: Union[int, str, None] = None,
            begin: Optional[dt.datetime] = None,
            end: Optional[dt.datetime] = None,
            query: Union[str, Query, None] = None,
            resample_rule: str = "12H") -> Tuple[dict]:
    """Read timeseries for given data/sensor labels and return in raw/resampled
    form.

    data should contain all the values to either be queried automagically or
    the key name of the resulting data in the output dict for a given query.
    If query is given, use the given query instead of the prebuilt one.
    """
    # sanitizing
    data = data if isinstance(data, list) else [data]

    if not isinstance(begin, dt.datetime) and not query:
        raise TypeError(f"Expected 'begin' to be datetime.datetime object, but"
                        f" received {type(begin)}.")

    # setup of output dicts
    dfs = dict()
    dfs_resampled = dict()

    # iterate over all elements in data
    for sl in data:  # sl = SensorLabel
        print(f"Working on {sl}-dataset...")
        # create query object
        if not query:
            end = dt.datetime.now(tz=dt.timezone.utc)
            q = Query(SELECT=f"TBTimestamp AS ts, {sl}_Scaled AS {sl}_ts",
                      FROM="`exeter-science-unit.airmonitor.airmonitor`",
                      WHERE=f"UniqueID = {stationID} AND {sl}_Status = 'Valid'"
                            f" AND {sl}_Scaled >= 0"
                            f" AND TBTimestamp >= '{begin}'"
                            f" AND TBTimestamp <= '{end}'",
                      ORDERBY="ts")
        else:
            q = query

        # read data
        dfs[sl] = gbq.read_gbq(str(q), dialect='standard')

        # transform timestamps to datetime and set index to datetime
        dfs[sl].ts = pd.to_datetime(dfs[sl].ts)
        dfs[sl].index = dfs[sl].ts

        # resample data
        dfs_resampled[sl] = dfs[sl].resample(resample_rule).mean()

    return (dfs, dfs_resampled)


def bounded_graph(fbforecast: pd.DataFrame, bounds_args: Optional[dict] = None,
                  forecast_args: Optional[dict] = None) -> Tuple[go.Scatter]:
    """Wrapper for plotly graph objects for bounded graphs.

    Intended to use with fbprophet, because the output DataFrame has all
    columns named accordingly.

    Returns a tuple of go.Scatter objects.
    """
    if not (hasattr(fbforecast, "ds") or hasattr(fbforecast, "yhat_upper") or
            hasattr(fbforecast, "yhat_lower") or hasattr(fbforecast, "yhat")):
        raise RuntimeError("Could not resolve DataFrame, expected column names"
                           " 'yhat', 'yhat_upper', 'yhat_lower', 'ds'.")

    else:
        ds = fbforecast.ds  # prevent repeated call
        if not bounds_args:  # set default values
            bounds_args = {"marker": {"color": "#444"},
                           "line": {"width": 0},
                           "showlegend": False}

        if not forecast_args:  # more default values
            forecast_args = {"name": "Model+Forecast",
                             "marker": {"color": "#1F77B4"},
                             "line": {"width": 3}}

        upper_trace = go.Scatter(x=ds,  # time intervals
                                 y=fbforecast.yhat_upper,
                                 name="Upper bound",
                                 **bounds_args)

        lower_trace = go.Scatter(x=ds,
                                 y=fbforecast.yhat_lower,
                                 name="Lower bound",
                                 fill="tonexty",
                                 fillcolor="rgba(68, 68, 68, 0.3)",  # "rgba(173, 216, 230, 1)",
                                 **bounds_args)
        # NOTE: fillcolor needs to be rgba, go.Scatter doesn't understand
        # 8-digit hex codes

        trace = go.Scatter(x=ds,
                           y=fbforecast.yhat,
                           **forecast_args)

    return (upper_trace, lower_trace, trace)
