# Japan Weather ETL

### Deployment Summary
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

# Limitations
The Overpass API has limitations on throughput and query size, so several workarounds were made for the projects to work:
    1. GCP
        Limit OverPass query to just query Shinkansen stations
    2. Databricks
        Read a parquet file of stations exported from OpenStreetMap uploaded to a volume

# Data flow
The following shows images of the Databricks pipeline, however the GCP setup is relatively similar.

The data flow of the pipeline can be seen in the figure below.

<img src="screenshots/Data%20Flow.png" alt="Data Flow" width="900" />

The pipeline begins by parsing the parquet file for a list of train stations. This list is created from an OpenStreetMap (OSM) dataset, and uploaded to a volume. This parses the dataset, and creates a stations table.

<img src="screenshots/Stations.png" alt="Stations" width="400" />

Once parsed, the stations are grouped using DBSCAN to find clusters of train stations. This helps limit the API calls to OpenMeteo weather API, by only grabbing weather for a selected representative within that cluster. For each of the clusters, weather such as temperature, wind speed, and precipitation are obtained, and reassigned to each station within the cluster.

<img src="screenshots/Weather.png" alt="Weather" width="700" />

Seperately, earthquake events are pulled from the JMA quake API by parsing JSON files on the website, once parsed, these files are parsed into a earthquake table.

<img src="screenshots/Earthquake.png" alt="Earthquake" width="700" />

Finally, once all initial tables are created, silver and gold tables are created from the base tables by filtering out troublesome rows, nulls, etc.

The dashboards were created using tableau and imported into Databricks and GCP.

Temperature:

<img src="screenshots/Temperatures.png" alt="Temperatures" width="550" />

Weather Summary:

<img src="screenshots/Summary.png" alt="Weather Summary" width="700" />


1. Databricks DABs are used to deploy the pipeline, a YAML file is created to deploy this pipeline [dab](databricks/databricks.yaml)
2. Run the SQL commands to create catalog, schema, volumes [create-env.sql](databricks/create-env.sql)
3. Due to newer OSM API requirements, a parquet file ![stations-2025.parquet](databricks/data/stations-list-2025.parquet) should be uploaded to a volume, then referenced in ![stations.py](databricks/pipeline/stations.py)
4. Deployment steps as follows:
   ```bash
   databricks bundle validate
   ```
   ```bash
   databricks bundle deploy
   ```

# Deployment Steps (GCP with Terraform)

1. Enable the following Google Cloud APIs:
- Artifact Registry (artifactregistry.googleapis.com)
- Cloud Run (run.googleapis.com)

2. Authenticate to GCP for Terraform:

```bash
gcloud auth application-default login
```

3. Test the Docker image locally (optional):

```bash
docker build -t japan-weather-etl ./gcp
docker run --rm -it japan-weather-etl
```

4. Configure `gcp/terraform/terraform.tfvars` with the following values:

```
project = ""
region = ""
location = ""
overpass_referer = ""
overpass_user_agent = ""
```

5. Deploy with Terraform. Terraform creates the Artifact Registry repository, builds and pushes the image with Cloud Build, and provisions Cloud Run and its supporting resources:

```bash
cd gcp/terraform
terraform init
terraform apply
```