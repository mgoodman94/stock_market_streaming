locals {
  data_lake_bucket = "stock_data_lake"
}

variable "project" {
  description = "GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources"
  default = "europe-west6"
  type = string
}

variable "bucket_name" {
  description = "Name of Google Cloud Storage bucket (globally unique)"
  default = ""
}

variable "storage_class" {
  description = "Storage class type for the bucket"
  default = "STANDARD"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that raw data will be written to"
  type = string
  default = "alpha_vantage_api"
}

variable "table_name" {
  description = "BigQuery Table"
  type = string
  default = "intraday_stock_data"
}
