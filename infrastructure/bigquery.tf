resource "google_bigquery_dataset" "bigquery" {
  dataset_id    = var.gcp_dataset
  friendly_name = var.gcp_dataset
  description   = "This is a dataset used as a passion project for playing with Index Ads data"
  location      = var.region

  labels = {
    source  = "terraform-github"
    project = var.project_name
  }
}

resource "google_bigquery_table" "parquet_external_table" {
  dataset_id = google_bigquery_dataset.bigquery.dataset_id
  table_id   = "external_table_apartments"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    # Use wildcards to match all parquet files in any date folder
    source_uris = [
      "gs://${google_storage_bucket.bucket.name}/*.parquet"
    ]
  }

  depends_on = [google_storage_bucket.bucket]
}

resource "google_bigquery_table" "price_changes_view" {
  dataset_id          = google_bigquery_dataset.bigquery.dataset_id
  table_id            = "${var.project_name}_price_changes_view"
  deletion_protection = false
  view {
    query = <<EOF
      WITH previous_price_cte AS (
        SELECT
          url,
          neighborhood,
          price,
          priceM2,
          area,
          LAG(price) OVER (PARTITION BY url, neighborhood ORDER BY extractDate) AS previous_price,
          extractDate,
          LAG(extractDate) OVER (PARTITION BY url, neighborhood ORDER BY extractDate) AS previous_extractDate
        FROM `${var.gcp_project}.${var.gcp_dataset}.${google_bigquery_table.parquet_external_table.table_id}`
      )
      SELECT *
      FROM previous_price_cte
      WHERE 
        previous_price > price
        AND area >= 60
        AND previous_extractDate <> extractDate
        AND DATE(extractDate) >= CURRENT_DATE() - INTERVAL 1 DAY
      ORDER BY extractDate DESC
    EOF

    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "count_per_day_view" {
  dataset_id          = google_bigquery_dataset.bigquery.dataset_id
  table_id            = "${var.project_name}_count_per_day_view"
  deletion_protection = false
  view {
    query = <<EOF
      select 
        date(substring(renewalTime, 1, 10)) as dt,
        count(*)
      from `${var.gcp_project}.${var.gcp_dataset}.${google_bigquery_table.parquet_external_table.table_id}`
      group by 1
      order by 1 desc
      limit 100
    EOF

    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "price_per_neighborhood_view" {
  dataset_id          = google_bigquery_dataset.bigquery.dataset_id
  table_id            = "${var.project_name}_price_per_neighborhood_view"
  deletion_protection = false
  view {
    query = <<EOF
      with distinct_data as (
        select
          *,
          row_number() over (partition by url order by extractDate) rn
        from `${var.gcp_project}.${var.gcp_dataset}.${google_bigquery_table.parquet_external_table.table_id}`
        where postedTime > '2025-01-01'
      )
      select 
        neighborhood,
        round(avg(priceM2), 2) as avg_price_m2m,
        round(avg(area), 2) as avg_listing_m2,
        count(1) as no_of_listings
      from distinct_data
      where rn = 1
      group by 1
      order by 2 desc
    EOF

    use_legacy_sql = false
  }
}
