# Spectrader ETL

Fetch financial timeseries and provide means to serve in R.

Consists of periodic aggregates (bars in M1, ..., MN1) and ticks,
each series is connected to a data source (flat file or API) and serves
streaming and REST API.

Series can be stocks, options, futures, indices, forex, and crypto.
