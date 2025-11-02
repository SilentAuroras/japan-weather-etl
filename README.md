# Japan Weather ETL

This repository creates a data pipeline Japan weather or events centered around train stations.

The following data sources are integrated into the pipeline:

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

Steps

1. Build docker image

```bash
docker build -t japan-weather-etl .
```

2. Test run the image

```bash
docker run --rm -it japan-weather-etl
```

3. Check raw files written (optional)

```bash
docker run --rm -it japan-weather-etl ls -l /app/data/raw/
```
