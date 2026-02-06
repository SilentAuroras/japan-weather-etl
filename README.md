# Japan Weather ETL

This repository creates a data pipeline Japan weather or events centered around train stations.

Two versions exist for learning different pipelines
- GCP - [GCP](gcp)
- Databricks - [Databricks](databricks)

Readme and installation instructions exist within each sub folder.

For both deployment scenarios, the following data is integrated:
-   Station data:
    -   API: http://overpass-api.de/
    -   Requested data
        -   Station name
        -   Latitude
        -   Longitude
-   Earthquake data
    -   API: https://www.jma.go.jp/bosai/quake/data/list.json
    -   Requested data
        -   Earthquake event data
-   Weather data:
    -   API: https://open-meteo.com/en/docs/jma-api
    -   Requested data
        -   Temperature
        -   Day or night
        -   Precipitation
        -   Wind speed
        -   Wind direction