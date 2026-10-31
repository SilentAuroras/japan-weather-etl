// BigQuery Dataset
resource "google_bigquery_dataset" "japan-weather-dataset" {
  dataset_id = "japan_weather_dataset_001"
  friendly_name = "japan-weather-dataset"
  description = "Dataset for station weather and earthquake data"
  location = var.location

  // Allow tf to destroy dataset
  delete_contents_on_destroy = true
}

// BigQuery table - station list
resource "google_bigquery_table" "japan-stations-table" {
  table_id = "japan-stations-table-001"
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  deletion_protection = false

  // Predefine schema so not reliant on preexisting data
  schema = jsonencode([
    { name = "name", type = "STRING", mode = "NULLABLE" },
    { name = "latitude", type = "FLOAT64", mode = "NULLABLE" },
    { name = "longitude", type = "FLOAT64", mode = "NULLABLE" },
    { name = "geography", type = "GEOGRAPHY", mode = "NULLABLE" }
  ])
}

// BigQuery table - weather data
resource "google_bigquery_table" "japan-weather-table" {
  table_id = "japan-weather-table-001"
  dataset_id = google_bigquery_dataset.japan-weather-dataset.dataset_id
  deletion_protection = false

  // Predefine schema
  schema = jsonencode([
    { name = "name", type = "STRING", mode = "NULLABLE" },
    { name = "latitude", type = "FLOAT64", mode = "NULLABLE" },
    { name = "longitude", type = "FLOAT64", mode = "NULLABLE" },
    { name = "geography", type = "GEOGRAPHY", mode = "NULLABLE" },
    { name = "cluster", type = "INT64", mode = "NULLABLE" },
    { name = "temperature_2m", type = "FLOAT64", mode = "NULLABLE" },
    { name = "is_day", type = "FLOAT64", mode = "NULLABLE" },
    { name = "precipitation", type = "FLOAT64", mode = "NULLABLE" },
    { name = "wind_speed_10m", type = "FLOAT64", mode = "NULLABLE" },
    { name = "wind_direction_10m", type = "FLOAT64", mode = "NULLABLE" },
    { name = "timestamp", type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}

// BigQuery table - earthquake events
resource "google_bigquery_table" "japan-earthquake-table" {
  table_id            = "japan-earthquake-table-001"
  dataset_id          = google_bigquery_dataset.japan-weather-dataset.dataset_id
  deletion_protection = false

  // Predefine schema
  schema = jsonencode([
    { name = "control_title", type = "STRING", mode = "NULLABLE" },
    { name = "control_datetime", type = "STRING", mode = "NULLABLE" },
    { name = "control_status", type = "STRING", mode = "NULLABLE" },
    { name = "control_editorialoffice", type = "STRING", mode = "NULLABLE" },
    { name = "control_publishingoffice", type = "STRING", mode = "NULLABLE" },
    { name = "head_title", type = "STRING", mode = "NULLABLE" },
    { name = "head_reportdatetime", type = "STRING", mode = "NULLABLE" },
    { name = "head_targetdatetime", type = "STRING", mode = "NULLABLE" },
    { name = "head_eventid", type = "STRING", mode = "NULLABLE" },
    { name = "head_infotype", type = "STRING", mode = "NULLABLE" },
    { name = "head_serial", type = "STRING", mode = "NULLABLE" },
    { name = "head_infokind", type = "STRING", mode = "NULLABLE" },
    { name = "head_infokindversion", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_text", type = "STRING", mode = "NULLABLE" },
    { name = "head_entitle", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_origintime", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_arrivaltime", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_name", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_coordinate", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_enname", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_magnitude", type = "STRING", mode = "NULLABLE" },
    { name = "body_intensity_observation_maxint", type = "STRING", mode = "NULLABLE" },
    { name = "body_intensity_observation_pref", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_forecastcomment_text", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_forecastcomment_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_forecastcomment_entext", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_varcomment_text", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_varcomment_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_varcomment_entext", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_kind_name", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_areas_area", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_detailedname", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_detailedcode", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_source", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_freeformcomment", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_areas_area_name", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_areas_area_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquakeinfo_infokind", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquakeinfo_infoserial_name", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquakeinfo_infoserial_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquakeinfo_text", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquakeinfo_appendix", type = "STRING", mode = "NULLABLE" }
  ])
}