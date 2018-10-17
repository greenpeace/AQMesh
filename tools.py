"""A collection of useful functions to be used for AQMesh data analysis."""

import datetime as dt
import pandas as pd

from query import Query  # custom
from pandas.io import gbq  # for running queries

from typing import Optional, Union  # for typing support


def read_ts(stationID: Union[int, str],
            data: Union[list, str],
            begin: Optional[dt.datetime] = None,
            end: Optional[dt.datetime] = None,
            query: Union[str, Query, None] = None,
            resample_rule: str = "12H") -> tuple:
    """Read timeseries for given data/sensor labels and return in raw/resampled
    form.

    data should contain all the values to either be queried automagically or
    the key name of the resulting data in the output dict for a given query.
    If query is given, use the given query instead of the prebuilt one.
    """
    # sanitizing
    if data:
        data = data if isinstance(data, list) else [data]

    if not isinstance(begin, dt.datetime) and not query:
        raise TypeError(f"Expected 'begin' to be datetime.datetime object, but"
                        f" received {type(begin)}.")

    # setup of output dicts
    dfs = dict()
    dfs_resampled = dict()

    # iterate over all elements in data
    for sl in data:
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

    return dfs, dfs_resampled


# TODO: wrapper for plotly to create timeseries with upper and lower bound
