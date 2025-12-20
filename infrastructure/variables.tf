variable "region" {
  type    = string
  default = "europe-west3"
}

variable "gcp_project" {
  type    = string
  default = "cws-poc-st"
}

variable "project_name" {
  description = "Default project_name"
  type        = string
  default     = "tf-index-ads-st"
}

variable "credentials" {
  description = "Path to service account file"
  type        = string
  default     = "./service_account.json"
}

variable "gcp_dataset" {
  type    = string
  default = "IndexAds"
}

variable "alerting_email" {
  type    = string
  default = "nikola.samardzic1997+GCP@gmail.com"
}