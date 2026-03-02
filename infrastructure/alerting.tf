resource "google_monitoring_notification_channel" "email" {
  display_name = "${var.project_name}-email-notifications"
  type         = "email"
  labels = {
    email_address = var.alerting_email
  }
  force_delete = true
}

resource "google_monitoring_alert_policy" "function_failure" {
  display_name = "${var.project_name}-function-failure-alert"
  combiner     = "OR"
  conditions {
    display_name = "Cloud Function execution failures"
    condition_threshold {
      filter          = "resource.type = \"cloud_function\" AND resource.labels.function_name = \"${google_cloudfunctions2_function.function.name}\" AND metric.type = \"cloudfunctions.googleapis.com/function/execution_count\" AND metric.labels.status != \"ok\""
      duration        = "60s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.id]

  alert_strategy {
    auto_close = "1800s"
  }

  depends_on = [google_monitoring_notification_channel.email]
}
