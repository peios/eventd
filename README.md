# eventd

`eventd` is Peios's durable observability daemon. It drains one lock-free KMES
ring per logical CPU into receipt-backed SQLite event shards, accepts service
logs and metrics over bounded Unix datagram sockets, and serves authorized
observability queries over a Unix stream socket.

The workspace is split deliberately:

- `eventd-core` contains the bounded handoff, recovery model and SQLite stores.
  It has no Peios-kernel linkage, so its tests and hot-path benchmarks run on
  an ordinary development host.
- `eventd` owns KMES attachment, ingestion, retention, adaptive indexing,
  access control, querying, configuration reload and lifecycle supervision. It
  is the only crate linked to libpeios.

The implementation follows the eventd TRM in the Peios `learn` repository.

Run `tools/check` before committing. Run `tools/bench` on deployment-class
hardware before changing queue, batch or SQLite parameters. `pekit build`
produces the release tree and `pekit package --version <version>` produces the
Peios package, including the inert registry seeds and the standard
`/var/state/eventd/{events,logs,metrics}` directory skeleton.
