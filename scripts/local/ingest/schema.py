"""
PyArrow schema definition for GitHub Archive events.

This schema flattens the nested JSON structure from GitHub Archive
into a columnar format optimized for the StarScout queries.
"""

import pyarrow as pa

# Core event schema matching StarScout query requirements
EVENTS_SCHEMA = pa.schema([
    # Event identification
    ("id", pa.string()),  # Event ID (string in GitHub Archive)
    ("type", pa.string()),  # Event type: WatchEvent, PushEvent, etc.
    ("created_at", pa.timestamp("us", tz="UTC")),  # Event timestamp

    # Actor (user) fields
    ("actor_id", pa.int64()),
    ("actor_login", pa.string()),
    ("actor_avatar_url", pa.string()),

    # Repository fields
    ("repo_id", pa.int64()),
    ("repo_name", pa.string()),  # Format: owner/repo

    # Organization fields (nullable - not all events have org)
    ("org_id", pa.int64()),
    ("org_login", pa.string()),

    # Derived field for efficient star queries
    ("is_star", pa.bool_()),  # True if WatchEvent with action=started

    # Partition columns (duplicated for Hive partitioning)
    ("year", pa.int16()),
    ("month", pa.int8()),
    ("day", pa.int8()),
    ("hour", pa.int8()),
])

# Event types we care about for StarScout
# WatchEvent = star, others used for activity analysis
RELEVANT_EVENT_TYPES = {
    "WatchEvent",      # Star/unstar
    "PushEvent",       # Code push
    "IssuesEvent",     # Issue actions
    "PullRequestEvent",  # PR actions
    "CreateEvent",     # Branch/tag/repo creation
    "DeleteEvent",     # Branch/tag deletion
    "ForkEvent",       # Repository fork
    "IssueCommentEvent",  # Issue comments
    "CommitCommentEvent",  # Commit comments
    "PullRequestReviewEvent",  # PR reviews
    "PullRequestReviewCommentEvent",  # PR review comments
    "ReleaseEvent",    # Release actions
    "MemberEvent",     # Collaborator changes
    "PublicEvent",     # Repo made public
    "GollumEvent",     # Wiki edits
}

# Minimal schema for memory-efficient processing
# Only includes fields actually used in queries
MINIMAL_SCHEMA = pa.schema([
    ("type", pa.string()),
    ("created_at", pa.timestamp("us", tz="UTC")),
    ("actor_id", pa.int64()),
    ("actor_login", pa.string()),
    ("repo_id", pa.int64()),
    ("repo_name", pa.string()),
    ("org_login", pa.string()),
    ("is_star", pa.bool_()),
    ("year", pa.int16()),
    ("month", pa.int8()),
    ("day", pa.int8()),
    ("hour", pa.int8()),
])


def get_schema(minimal: bool = False) -> pa.Schema:
    """Get the appropriate schema for Parquet files."""
    return MINIMAL_SCHEMA if minimal else EVENTS_SCHEMA
