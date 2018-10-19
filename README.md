# AQMesh

Two scripts to pipe airmonitor API's data into BigQuery. 

### File description
```
AQMesh
|    get_history.py
|    schema.py
|    query.py
|    scraper.py
|    tools.py
|
└────visu 
     |    BigQueryInlineQuery.ipynb
     |    BigQueryPandasPlotly.ipynb
     |    global_air_quality.ipynb
  
```

- `get_history.py`: requests all historic data (up to today) of the airmonitor API, reformats it and pipes it into BigQuery. If a new table/dataset needs to be created in the process (as specified in the file in the top section), the currently used table schema is read from `schema.py`. Logs are written to a file, per default `airmonitorHistory.log` and to stdout. 
- `query.py`: contains a class, `Query` that is used to organise and build a string that can be used to query BigQuery. (helper class)
- `scraper.py`: is in principal almost identical to `get_history.py`; this script should be run by e.g. a __cronjob__, to scrape the latest data off the API. It checks the timestamp of the latest entry in BigQuery for every available station and starts scraping from there. Has logging to `Stackdriver.Logging` enabled, so all logging messages are available in GCP. Also logs to stdout, but not to a file (can still be enabled if wanted though).
- `tools.py`: contains two functions that are needed for the visualisations to unclutter the code. The first one (`read_ts`) makes reading data from the BigQuery table easier, the second one (`bounded_graph`) helps to draw a bounded graph with `plotly`. Both are used in the visualisations, described below.

#### └ visu

- `BigQueryInlineQuery.ipynb`: example of how to use jupyter magic commands to query BigQuery and use the data (here with `matplotlib`).
- `BigQueryPandasPlotly.ipynb`: example of how to use `pandas.io.gbq` to query BigQuery. Visualising time series using `pandas` and `plotly`. Also trying to forecast timeseries (temperature and carbon monoxide time series in this example) using the package `fbprophet`. 
- `global_air_quality.ipynb`: uses the historical open data of EPA to do the same as in `BigQueryPandasPlotly.ipynb`, but with a longer historical record to train the model from, resulting in  better forecasts. In this example, a site in St. Louis, Missouri, was used with a hourly temperature record going back to 2013. 

### Dependencies
It is recommended to use `Python` >= 3.7 to avoid problems caused by new syntax features (string interpolation, type hinting, .. ). All dependencies can be installed via `pip` (version numbers are of this writing):

| package 		  | version   |
|:-----------------------:| ---------:|
| `google-cloud-bigquery` | 1.5.1     |
| `google-cloud-logging`  | 1.8.0     |
| `numpy`		  | 1.15.2    |
| `fbprophet` 		  | 0.3.post2 |
| `jupyter notebook`      | 5.5.0     |
| `matplotlib` 	 	  | 3.0.0     |
| `pandas` 		  | 0.23.4    |
| `pandas-gbq` 	          | 0.6.1     |

`fbprophet` depends on `pyStan`, which needs quite a lot of RAM during the installation. If you run into problems, consider using a swapfile.
