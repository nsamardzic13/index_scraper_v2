resource "google_storage_bucket" "bucket" {
  name          = "${var.project_name}-bucket"
  location      = "EU"
  force_destroy = true
}