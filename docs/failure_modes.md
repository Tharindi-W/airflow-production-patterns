# Failure Modes

A cross pattern table of failure scenarios and how this repo handles each. It grows as patterns land. Each pattern's folder README has a fuller "Failure modes" section.

| Scenario | Pattern(s) | How it is handled |
|----------|-----------|-------------------|
| Same load runs twice (retry, manual re-run, catchup) | 01 | Primary key upsert plus execution date scoping, so the second run is a no-op on row counts and content hash. |
| Partial write interrupted mid load | 01 | The load for a logical date is delete-then-insert within the date partition, so a re-run cleanly reestablishes the correct state. |

Scenarios for patterns 02 through 10 will be added as those patterns are implemented.
