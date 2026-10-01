"""Stable rejection reasons produced by the validated quality/dedup contract."""

REJECTION_REASONS = (
    "corrupt_json", "missing_event_id", "missing_event_type", "missing_repo_name",
    "missing_actor_login", "invalid_timestamp", "unsupported_event_type",
    "outside_date", "duplicate_event_id", "conflicting_event_id",
)
