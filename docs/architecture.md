# Architecture and failure semantics

Profiles and contracts are content-addressed, so retrying the same payload is safe. Lineage event IDs are idempotent. Comparisons calculate statistical evidence and persist an incident with the exact findings and downstream graph snapshot.

The local reference uses SQLite WAL. Large profiles should be stored in object storage and analyzed by background workers; the synchronous endpoint intentionally accepts profile summaries rather than raw datasets.
