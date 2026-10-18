# eventd

eventd stores Peios events, logs, and metrics and serves authorized queries.
It consumes kernel event rings, accepts brokered service logs, and checks
metric publication against the sender's token.

The workspace contains four crates:

- `eventd-core`: bounded queues, receipt accounting, and SQLite stores.
- `eventd`: kernel integration, ingestion, authorization, and service
  lifecycle.
- `eventd-client`: query transport, client lifecycle, and shared access
  constants.
- `evctl`: the command-line query client.

Read [the contributor guide](docs/building.md) to build, test, and package
the workspace. Start with [Stored Records](docs/learn/peios.antho/200--events-and-logs.shelf/100--reading-records.topic/100--stored-records.md)
for the reader documentation, or the [eventd technical reference manual](docs/learn/peios.antho/200--events-and-logs.shelf/400--eventd.book/1--introduction/1--overview.md)
for implementation details.

Command manuals live in [docs/man](docs/man). Registry and event references
live in [eventd.regman](docs/eventd.regman) and [eventd.evman](docs/eventd.evman).
The original documentation inventory is recorded in the
[migration source map](docs/migration-source-map.json).

Queries run through eventd's socket. Clients do not open its databases or
substitute their own authorization decisions. Native kernel integration
requires the Peios environment; host tests exercise the portable logic.
