terraform {
  backend "gcs" {
    bucket      = "tf-my-backend-bucket-new"
    prefix      = "tf-index-ads/"
    credentials = "service_account.json"
  }
}