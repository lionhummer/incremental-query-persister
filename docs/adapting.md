# Adapting storage

`RowStore.read()` returns one consistent metadata/row snapshot. `write(change)` returns false for a stale generation and otherwise commits the complete change atomically. A rejected or thrown write must not partially commit. `clear()` advances the generation even when no rows exist.

A native bridge can send `RowChange` to a worker owning SQLite. Keep a separate namespace per account. Enforce authorization on the native side rather than trusting a client-supplied account identifier. Compare generations inside the same transaction as the row update; checking before sending a request is insufficient.

An IndexedDB adapter should store generation and rows in one readwrite transaction. A remote adapter needs equivalent transactional compare-and-write behavior, bounded calls, cancellation and a defined response-loss policy. Do not assume an HTTP retry is harmless: a committed operation whose reply was lost needs reconciliation. There is no built-in remote adapter or distributed writer election.

For multiple windows, elect one writer in the host and pass `isWriter`. Stop the producer before sign-out, await `removeClient`, then create the next account's namespace and producer. Await `close` before shutting down the storage worker.

Store only JSON-compatible query data. Dates become strings, nonfinite numbers become null, and custom serialization follows JSON rules. The library does not encrypt or redact query payloads. Exclude confidential data using dehydration and `shouldPersist`, and protect the storage directory using the host's security model.
