# Japan Weather ETL

This repository creates a data pipeline Japan weather or events centered around train stations.

Requirements
1. Google Cloud SDK
2. Docker

Steps

1. Build docker image

```bash
docker build -t japan-weather-etl .
```

2. Test run the image locally (optional)

```bash
docker run --rm -it japan-weather-etl
```

3. Check raw files written (optional)

```bash
docker run --rm -it japan-weather-etl ls -l /app/data/raw/
```

4. Create docker repository in Artifact Registry
```bash
gcloud artifacts repositories create weather-etl-repo --repository-format=docker --location=<REGION> --description="Weather ETL repository"
    
gcloud artifacts repositories list
```

5. Submit docker image for Cloud Build
```bash
gcloud builds submit --region=<REGION> --tag <REGION>-docker.pkg.dev/PROJECT_ID/weather-etl-repo/japan-weather-etl:tag1 

```

6. Setup terraform variables file (_terraform.tfvars_) with the following set:
```
project = ""
region = ""
location = ""
```