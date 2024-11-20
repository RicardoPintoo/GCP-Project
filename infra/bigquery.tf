resource "google_bigquery_dataset" "example_dataset" {
  dataset_id = "dataset_teste"
  project    = var.project_id
  location   = "EU"
}

resource "google_bigquery_table" "event_data" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  table_id   = "event_data_table"
  project    = var.project_id
  
  # Disable deletion protection
  deletion_protection = false

  schema = <<EOF
[
  {"name": "user_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
  {"name": "date", "type": "DATE", "mode": "REQUIRED"},
  {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "page_url", "type": "STRING", "mode": "NULLABLE"},
  {"name": "device_type", "type": "STRING", "mode": "NULLABLE"},
  {"name": "location", "type": "STRING", "mode": "NULLABLE"},
  {"name": "duration_seconds", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "comment_text", "type": "STRING", "mode": "NULLABLE"},
  {"name": "subscription_plan", "type": "STRING", "mode": "NULLABLE"},
  {"name": "form_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "login_method", "type": "STRING", "mode": "NULLABLE"},
  {"name": "referral_source", "type": "STRING", "mode": "NULLABLE"},
  {"name": "streaming_quality", "type": "STRING", "mode": "NULLABLE"},
  {"name": "cart_items_count", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "error_message", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}
