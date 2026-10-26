## Databricks Data flow
The following shows images of the Databricks pipeline.

The data flow of the pipeline can be seen in the figure below.

<img src="../screenshots/Data%20Flow.png" alt="Data Flow" width="900" />

The pipeline begins by parsing the parquet file for a list of train stations. This list is created from an OpenStreetMap (OSM) dataset, and uploaded to a volume. This parses the dataset, and creates a stations table.

<img src="../screenshots/Stations.png" alt="Stations" width="400" />

Once parsed, the stations are grouped using DBSCAN to find clusters of train stations. This helps limit the API calls to OpenMeteo weather API, by only grabbing weather for a selected representative within that cluster. For each of the clusters, weather such as temperature, wind speed, and precipitation are obtained, and reassigned to each station within the cluster.

<img src="../screenshots/Weather.png" alt="Weather" width="700" />

Separately, earthquake events are pulled from the JMA quake API by parsing JSON files on the website, once parsed, these files are parsed into a earthquake table.

<img src="../screenshots/Earthquake.png" alt="Earthquake" width="700" />

Finally, once all initial tables are created, silver and gold tables are created from the base tables by filtering out troublesome rows, nulls, etc.

The dashboards were created using tableau and imported into Databricks and GCP.

Temperature:

<img src="../screenshots/Temperatures.png" alt="Temperatures" width="550" />

Weather Summary:

<img src="../screenshots/Summary.png" alt="Weather Summary" width="700" />

## Databricks Deployment Steps

1. Databricks DABs are used to deploy the pipeline, a YAML file is created to deploy this pipeline [dab](databricks.yaml)
2. Run the SQL commands to create catalog, schema, volumes [create-env.sql](create-env.sql)
3. Due to newer OSM API requirements, a parquet file ![stations-2025.parquet](data/stations-list-2025.parquet) should be uploaded to a volume, then referenced in ![stations.py](pipeline/stations.py)
4. Deployment steps as follows:
   ```bash
   databricks bundle validate
   ```
   ```bash
   databricks bundle deploy
   ```
