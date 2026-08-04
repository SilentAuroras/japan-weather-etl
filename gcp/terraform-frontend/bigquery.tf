// BigQuery dataset for pipeline
resource "google_bigquery_dataset" "japan-weather-dataset" {
  friendly_name = "japan-weather-dataset"
  description = "Dataset for stations weather and earthquake data read from GCS"
  location = var.location
  dataset_id = "japan_weather_dataset_001"
  delete_contents_on_destroy = true
}

// BigQuery table for station list
resource "google_bigquery_table" "japan-stations-table" {
  table_id   = "japan-stations-table-001"
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  deletion_protection = false

  # pull parquet files from gcs mounted by cloudrun
  external_data_configuration {

    # Autodetect schema
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
  table_id   = "japan-weather-table-001"
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  deletion_protection = false

  # Define schema
  external_data_configuration {
    
    # Manually define schema
    autodetect = false

    # Schema from parquet DDL
    schema = jsonencode([
       { name = "name", type = "STRING", mode = "NULLABLE" },
       { name = "latitude", type = "NUMERIC", mode = "NULLABLE", precision = 9, scale = 7 },
       { name = "longitude", type = "NUMERIC", mode = "NULLABLE", precision = 10, scale = 7 },
       { name = "geography", type = "GEOGRAPHY", mode = "NULLABLE" },
       { name = "cluster", type = "INT64", mode = "NULLABLE" },
       { name = "temperature_2m", type = "FLOAT64", mode = "NULLABLE" },
       { name = "is_day", type = "FLOAT64", mode = "NULLABLE" },
       { name = "precipitation", type = "FLOAT64", mode = "NULLABLE" },
       { name = "wind_speed_10m", type = "FLOAT64", mode = "NULLABLE" },
       { name = "wind_direction_10m", type = "FLOAT64", mode = "NULLABLE" },
       { name = "timestamp", type = "TIMESTAMP", mode = "NULLABLE" }
     ])
    
    // Read in parquet files - default to string
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
  table_id   = "japan-earthquake-table-001"
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  deletion_protection = false

  // Read earthquake parquet files from cloudrun gcs mount
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