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
    { name = "longitude", type = "FLOAT64", mode = "NULLABLE" }
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
    { name = "control_editorial_office", type = "STRING", mode = "NULLABLE" },
    { name = "control_publishing_office", type = "STRING", mode = "NULLABLE" },
    { name = "head_title", type = "STRING", mode = "NULLABLE" },
    { name = "head_report_datetime", type = "STRING", mode = "NULLABLE" },
    { name = "head_target_datetime", type = "STRING", mode = "NULLABLE" },
    { name = "head_event_id", type = "STRING", mode = "NULLABLE" },
    { name = "head_info_type", type = "STRING", mode = "NULLABLE" },
    { name = "head_serial", type = "STRING", mode = "NULLABLE" },
    { name = "head_info_kind", type = "STRING", mode = "NULLABLE" },
    { name = "head_info_kind_version", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_text", type = "STRING", mode = "NULLABLE" },
    { name = "head_entitle", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_origin_time", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_arrival_time", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_name", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_coordinate", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_entitle", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_magnitude", type = "STRING", mode = "NULLABLE" },
    { name = "body_intensity_observation_max_int", type = "STRING", mode = "NULLABLE" },
    { name = "body_intensity_observation_pref", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_forecast_comment_text", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_forecast_comment_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_forecast_comment_entext", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_var_comment_text", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_var_comment_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_var_comment_entext", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_kind_name", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_areas_area", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_detailed_name", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_area_detailed_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_hypocenter_source", type = "STRING", mode = "NULLABLE" },
    { name = "body_comments_free_form_comment", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_areas_area_name", type = "STRING", mode = "NULLABLE" },
    { name = "head_headline_information_item_areas_area_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_info_info_kind", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_info_info_serial_name", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_info_info_serial_code", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_info_text", type = "STRING", mode = "NULLABLE" },
    { name = "body_earthquake_info_appendix", type = "STRING", mode = "NULLABLE" }
  ])
}