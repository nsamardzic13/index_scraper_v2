resource "google_bigquery_table" "cars_parquet_external_table" {
  dataset_id          = google_bigquery_dataset.bigquery.dataset_id
  table_id            = "external_table_cars"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    # Use wildcards to match all parquet files in cars folder
    source_uris = [
      "gs://${google_storage_bucket.bucket.name}/data/car/*.parquet"
    ]

    # Skip header rows and ignore unknown values to handle empty buckets
    ignore_unknown_values = true
    max_bad_records       = 0
  }

  depends_on = [google_storage_bucket.bucket]
}