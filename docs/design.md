# Design

Each persister has one serialized operation chain and one replaceable pending snapshot. Slow writes therefore do not create an unbounded queue of snapshots. Incoming snapshots copy row metadata while treating data objects as immutable host-owned values.

Held state describes acknowledged rows only. Weak object references plus serialized metadata detect unchanged rows without retaining large data objects. A failed write discards bookkeeping and reloads storage on the next fresh snapshot. Absence from memory preserves durable rows; explicit deletion, filtering and budget eviction remove them.

SQLite updates use BEGIN IMMEDIATE and compare the namespace generation inside the transaction. Clears increment that generation. This prevents an older transaction from resurrecting cleared rows but does not elect a writer or authenticate accounts. A host must stop stale producers and provide one writer per namespace.

The buster and timestamp belong to the persisted snapshot. Unchanged snapshots perform no write, including no timestamp refresh. Consumers applying age policies should account for that behavior. Restore reconstructs the supported persisted-client fields; arbitrary extra top-level client properties are not retained.

Budgets measure UTF-8 row payloads. Protected rows may cause atomic rejection rather than relaxing the budget. SQLite pages, indexes, WAL and runtime snapshot memory are outside the payload limit.
