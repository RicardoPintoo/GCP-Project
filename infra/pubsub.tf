resource "google_pubsub_topic" "user_events" {
  name = "${var.environment}-user-events-topic"
}

resource "google_pubsub_subscription" "user_events_subscription" {
  name  = "${var.environment}-user-events-subscription"
  topic = google_pubsub_topic.user_events.id

  retain_acked_messages     = true
  message_retention_duration = "604800s"
}
