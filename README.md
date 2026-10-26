# Japan Weather ETL

## Deployment Summary

This repository creates a data pipeline Japan weather or events centered around train stations.

Two versions exist for learning different pipelines. Readme and installation instructions exist within each sub folder.
- GCP - [GCP](gcp/README.md)
- Databricks - [Databricks](databricks/README.md)

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

## Limitations
The Overpass API has limitations on throughput and query size, so several workarounds were made for the projects to work:
 
1. GCP - Self-hosted Overpass API in GCP on Compute

2. Databricks - Parquet file of stations exported from OpenStreetMap