// BigQuery dataset for pipeline
resource "google_bigquery_dataset" "japan-weather-dataset" {
  dataset_id = "japan_weather_dataset_001"
  friendly_name = "japan-weather-dataset"
  description = "Dataset for stations weather and earthquake data read from GCS"
  location = var.location
  delete_contents_on_destroy = true
}

// BigQuery table for station list
resource "google_bigquery_table" "japan-stations-table" {
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  table_id   = "japan-stations-table-001"
  deletion_protection = false
  external_data_configuration {
    autodetect = true
    source_format = "PARQUET"
    source_uris = [
      "${google_storage_bucket.docker-mount-gcs.url}/raw/station-coordinates.parquet"
    ]
    parquet_options {
      enum_as_string = true
    }
  }
}

// BigQuery table for weather data
resource "google_bigquery_table" "japan-weather-table" {
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  table_id   = "japan-weather-table-001"
  deletion_protection = false
  external_data_configuration {
    autodetect = true
    source_format = "PARQUET"
    source_uris = [
      "${google_storage_bucket.docker-mount-gcs.url}/raw/weather*.parquet"
    ]
    parquet_options {
      enum_as_string = true
    }
  }
}

// BigQuery table for earthquake data
resource "google_bigquery_table" "japan-earthquake-table" {
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  table_id   = "japan-earthquake-table-001"
  deletion_protection = false
  external_data_configuration {
    autodetect = true
    source_format = "PARQUET"
    source_uris = [
      "${google_storage_bucket.docker-mount-gcs.url}/raw/quake*.parquet"
    ]
    parquet_options {
      enum_as_string = true
    }
  }
}