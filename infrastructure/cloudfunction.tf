data "archive_file" "function_zip" {
  type        = "zip"
  output_path = local.function_zip_path
  source_dir  = local.function_folder
}

resource "google_storage_bucket_object" "archive" {
  name   = "cloudfunction.zip"
  bucket = google_storage_bucket.bucket.name
  source = data.archive_file.function_zip.output_path

  metadata = {
    md5hash = data.archive_file.function_zip.output_md5
  }
}


# Single Cloud Function with multiple triggers
resource "google_cloudfunctions2_function" "function" {
  name     = "${var.project_name}-function2"
  location = var.region
  build_config {
    runtime     = "python312"
    entry_point = "main"
    source {
      storage_source {
        bucket     = google_storage_bucket.bucket.name
        object     = google_storage_bucket_object.archive.name
        generation = google_storage_bucket_object.archive.generation
      }
    }
  }

  service_config {
    available_memory      = "512Mi"
    timeout_seconds       = 513
    max_instance_count    = 1
    service_account_email = local.service_account_email

    environment_variables = {
      BUILD_CONFIG_HASH = data.archive_file.function_zip.output_base64sha256 # trigger redeploy on code change
      BUCKET_NAME       = google_storage_bucket.bucket.name
    }
  }
}

# Apartments trigger
resource "google_cloud_scheduler_job" "daily_trigger_apartments" {
  name        = "${var.project_name}-apartments-daily-trigger"
  description = "Daily trigger for Cloud Function at 2AM UTC"
  schedule    = "0 2 * * *"
  time_zone   = "UTC"

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.function.service_config[0].uri
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode(jsonencode({
      category = "flats-for-sale"
    }))

    oidc_token {
      service_account_email = local.service_account_email
    }
  }
}

# Cars trigger
resource "google_cloud_scheduler_job" "daily_trigger_cars" {
  name        = "${var.project_name}-cars-daily-trigger"
  description = "Daily trigger for Cloud Function at 2AM UTC"
  schedule    = "30 2 * * *"
  time_zone   = "UTC"

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.function.service_config[0].uri
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode(jsonencode({
      category = "car"
    }))

    oidc_token {
      service_account_email = local.service_account_email
    }
  }
}
