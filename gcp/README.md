## GCP Data Flow

```mermaid
flowchart TD
    TF[Terraform] -->|provisions| AR[Artifact Registry]
    TF -->|gcloud builds submit| CB[Cloud Build]
    CB -->|push image| AR
    AR -->|image| CR

    CS[Cloud Scheduler<br/>daily 02:00 UTC] -->|POST + OIDC token| CR[Cloud Run<br/>japan-weather-etl-docker]

    subgraph Sources[External APIs]
        OP[Overpass API<br/>shinkansen stations]
        OM[Open-Meteo API<br/>current weather]
        JMA[JMA<br/>earthquake events]
    end

    CR -->|stations query| OP
    CR -->|weather per station cluster| OM
    CR -->|recent quake reports| JMA

    CR -->|write parquet<br/>/app/data mount| GCS[(Cloud Storage bucket<br/>raw/*.parquet)]

    GCS -->|BigQuery load job<br/>WRITE_TRUNCATE| T1[(japan-stations-table-001)]
    GCS -->|BigQuery load job<br/>WRITE_APPEND| T2[(japan-weather-table-001)]
    GCS -->|BigQuery load job<br/>WRITE_APPEND| T3[(japan-earthquake-table-001)]

    subgraph BQ[BigQuery dataset: japan_weather_dataset_001]
        T1
        T2
        T3
    end

    SA([Service account<br/>sa-weather]) -.->|invoker, GCS objectAdmin,<br/>BigQuery editor + jobs| CR
```

## Screenshots

The cloud run function when called creates parquet files in GCS, as well as loads the data directly into BigQuery upon execution. These parquet files can be seen below:

<img src="screenshots/gcs.png" alt="gcs" width="300" />

This data is then loaded into BigQuery datasets created by terraform. First the 

<img src="screenshots/stations_table.png" alt="gcs" width="500" />

## GCP Deployment Steps

1. Enable the following Google Cloud APIs:
- Artifact Registry (artifactregistry.googleapis.com)
- Cloud Run (run.googleapis.com)

2. Authenticate to GCP for Terraform:

```bash
gcloud auth login
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

5. Deploy with Terraform. 
```bash
cd gcp/terraform
terraform init
terraform apply
```

6. Terraform creates the Artifact Registry repository, builds and pushes the image with Cloud Build, and provisions Cloud Run and its supporting resources:

7. Trigger the cloud function