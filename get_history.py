#!/usr/bin/env python
import json
import logging

from typing import Union
from google.cloud import bigquery
from google.cloud.bigquery import Dataset, Table

from schema import airmonitorSchema  # custom
from query import Query  # custom

import requests as req
import datetime as dt  # needed for blocked requests of data

# logger setup ---------------------------------------------------------------
# create logger instance
logger = logging.getLogger('airmonitorHistory')
logger.setLevel(logging.DEBUG)
# create file handler
fh = logging.FileHandler(f"{logger.name}.log")
fh.setLevel(logging.INFO)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t"
                              "%(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add handlers to logger
logger.addHandler(fh)
logger.addHandler(ch)

# setting up airmonitor credentials -------------------------------------------
with open('airmonitor_credentials.json', 'r') as ac:
    credentials = json.load(ac)

accountID = credentials["accountID"]
licenceKey = credentials["licenceKey"]
baseURL = f"https://api.airmonitors.net/3.5/GET/{accountID}/{licenceKey}/"
stations = req.get(baseURL + 'stations').json()
manualHistoryEnd = dt.datetime.now(dt.timezone.utc)
timestepDays = 3

# get the client --------------------------------------------------------------
# make sure right environment variable is set for google account credentials
client = bigquery.Client()
dataset_id = "airmonitor"
table_id = "airmonitor"

# bool to see if check for duplicates should be done
checkForDuplicates = False

# list to store newly generated IdStrings to check against for duplicates
genIdStrings = []

# list to store IdStrings queried from an existing table
queriedIds = []


# create dataset and table reference
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

# try and see if dataset already exists - if not, create it -------------------
try:
    dataset = client.get_dataset(dataset_ref)
    logger.info("Found Dataset %s.", repr(dataset_id))
    try:
        table = client.get_table(table_ref)
        logger.info("Found Table %s.", repr(table_id))
        checkForDuplicates = True

    except:  # TODO find teh right exception for this
        logger.info("Creating Table %s.", repr(table_id))
        table = Table(table_ref, schema=airmonitorSchema)
        table = client.create_table(table)

except:  # TODO find the right exception for this
    # create the dataset
    logger.info("Creating Dataset %s.", repr(dataset_id))
    dataset = Dataset(dataset_ref)
    dataset.location = "EU"
    dataset = client.create_dataset(dataset)
    # create a table
    logger.info("Creating Table %s.", repr(table_id))
    table = Table(table_ref, schema=airmonitorSchema)
    table = client.create_table(table)


# functions -------------------------------------------------------------------
def queryThis(query: Query) -> list:
    """Query the given query object and return the resulting list."""
    q = str(query)  # extra line for clarity. calls the __str__ magic function

    return list(client.query(q).result())


def stringifyID(point: dict, uid: Union[int, str]) -> str:
    """Take a measurement dictionary and return a hopefully unique string id.

    More precisely, it's a concat of the begin and end timestamps, station uid
    and the sensor values.

    Returns the concat of the above mentioned.
    """
    # ordinal time for begin (b) and end (e)
    b = dt.datetime.fromisoformat(point['TBTimestamp']).strftime('%s')
    e = dt.datetime.fromisoformat(point['TETimestamp']).strftime('%s')
    # string concat of all sensor labels
    values = "-".join([str(sens["Scaled"]) for sens in point["Channels"]])

    idString = f"{uid}-{b}-{e}_{values}"  # actual id string
    return idString


# function to break down the json data
def rowify(url: str, additional_info: list = []) -> list:
    """Request given url and create list of row-tuples containing the data.

    The fields of the tuple correspond to the ones in the airmonitorSchema.
    Filled with None if no measurement data is available.

    Returns a list of row tuples.
    """
    try:
        rawdata = req.get(url).json()  # does exactly what you think
    except json.decoder.JSONDecodeError as err:
        splits = url.split('/')  # get the parts of the URL
        intvl = f"[{splits[-3]}, {splits[-2]}]"  # create a string of interval
        logger.warning("[rowify] No data found for interval %s. "
                       "Msg: %s.", intvl, err)  # use exc_info=1 for traceback
        return []

    fulldata = []
    for point in rawdata:  # iterating over all measured datapoints
        uid = additional_info[0]
        idstring = stringifyID(point, uid)  # create unique IdString

        # check for duplicates
        if idstring not in genIdStrings and idstring not in queriedIds:
            genIdStrings.append(idstring)

            # first part of data
            row = [point[i] for i in ["TBTimestamp", "TETimestamp", "Latitude",
                                      "Longitude", "Altitude"]]

            # Channel part of data
            channels = point["Channels"]
            tableChannels = ["AIRPRES", "CO", "HUM", "NO", "NO2", "O3",
                             "SO2", "PM1", "PM10", "PM2.5", "PARTICLE_COUNT",
                             "TEMP", "TSP", "VOLTAGE"]

            # dict comprehension to get sensorlabel: sensorchannel pairs
            channelDict = {ch["SensorLabel"]: ch for ch in channels}
            channelDictKeys = list(channelDict.keys())

            # log a warning if unrecognized channel labels appear
            for cdk in channelDictKeys:
                if cdk not in tableChannels:
                    logger.warning("Unrecognized channel label %s for IdString"
                                   " %s.", cdk, idstring)

            # kind of diagnostics so see, whether all the data fits in nicely
            if len(tableChannels) != len(channels):
                logger.debug("Number of expected channels [%s] and received "
                             "channels [%s] do not match. Received: %s.",
                             len(tableChannels), len(channels), channelDictKeys)

            # create the actual row
            for tch in tableChannels:
                if tch in channelDictKeys:
                    ch = channelDict[tch]
                    row = [*row, *[ch["PreScaled"], ch["Slope"], ch["Offset"],
                                   ch["Scaled"], ch["UnitName"], ch["Status"]]]
                else:
                    filler = [None] * 6  # no data -> fill with None
                    row = [*row, *filler]

            row = tuple([*row, *additional_info, idstring])
            fulldata.append(row)

        else:
            logger.warning("Encountered duplicate IdString %s.", idstring)

    del rawdata[:]  # freeing memory
    del genIdStrings[:]  # if API not broken this shouldn't do harm+free memory

    return fulldata


# fill data into the table ----------------------------------------------------
for num, s in enumerate(stations):  # iterating over all stations
    # setup
    UniqueId = s["UniqueId"]
    stationName = s["StationName"]
    logger.info("Working on: %s [%s/%s]", stationName, num+1, len(stations)+1)

    # get list of IdStrings for current station if necessary
    if checkForDuplicates:
        logger.info("Getting IdStrings for Station %s.", UniqueId)

        # get all TBTimestamps and IdStrings for UniqueId (see above) sorted
        timestampIdStrings = Query("TBTimestamp, IdString",
                                   f"`{project}.{dataset_id}.{table_id}`",
                                   WHERE=f"UniqueId = {UniqueId}",
                                   ORDERBY="TBTimestamp DESC")

        # use previous query to get all IdStrings (sorted by date)
        latestIdStrings = Query(WITHAS=('q', str(timestampIdStrings)),
                                SELECT="IdString", FROM="q")

        # create list of actual IdStrings
        queriedIds = [r.get('IdString') for r in queryThis(latestIdStrings)]
        logger.info("Queried IdStrings to check for duplicates.")

    # get period of measurements
    periodURL = f"{baseURL}stationdata/period/{UniqueId}"
    period = req.get(periodURL).json()[0]

    # get begin and end of data
    begin = dt.datetime.fromisoformat(period["FirstTBTimestamp"])

    # set fixed end for history -- start daily scraping from there (TODO)
    end = manualHistoryEnd  # setting the end as current time as of runtime
    delta = dt.timedelta(days=timestepDays)  # create timedelta as stepsize

    # create list of timesteps
    timesteps = [begin]
    while(end - timesteps[-1] > delta):
        timesteps.append(timesteps[-1] + delta)
    timesteps.append(end)

    # create string intervals for the airmonitor api
    intervals = []
    for i, ts in enumerate(timesteps[:-1]):
        if i > 0:
            ts += dt.timedelta(seconds=1)  # small deviation from original val
        intervals.append(f"{ts.isoformat()}/{timesteps[i+1].isoformat()}")

    for i, iv in enumerate(intervals):
        # terminal output updates in percentage
        print(f"::: Processing data chunks.. [{round(i/len(intervals)*100)}%]",
              end='')
        print('\r', end='')

        # actual magic happens in here
        rows = rowify(f"{baseURL}stationdata/{iv}/{UniqueId}", [UniqueId,
                                                                stationName])
        if len(rows) > 0:  # if data is returned
            client.insert_rows(table, rows)
            del rows[:]  # freeing memory

    del queriedIds[:]  # freeing memory
    del genIdStrings[:]
    print("\n")
    logger.info("Finished %s.", stationName)
