# eventd

`eventd` is Peios's durable observability daemon. It drains one lock-free KMES
ring per logical CPU into receipt-backed SQLite event shards, accepts service
logs and metrics over bounded Unix datagram sockets, and serves authorized
observability queries over a Unix stream socket.

Log origins are broker-attested: the log socket denies the Service logon group
before allowing SYSTEM, admitting peinit but not phase-2 services. Metric
producers convey their effective KACS token on every datagram and eventd checks
`EVENTD_PUBLISH` for each metric name through a bounded generation-aware cache.

The workspace is split deliberately:

- `eventd-core` contains the bounded handoff, recovery model and SQLite stores.
  It has no Peios-kernel linkage, so its tests and hot-path benchmarks run on
  an ordinary development host.
- `eventd` owns KMES attachment, ingestion, retention, adaptive indexing,
  access control, querying, configuration reload and lifecycle supervision. It
  is the only long-running process in the workspace and the only one that
  attaches to kernel event state.
- `evctl` is the native query client. It sends one PSPU query to eventd and
  renders the authorized response without ever opening the stores directly.

The common interface is deliberately just the query:

```sh
evctl 'EVENTS kacs.* SINCE 1h ago TAKE 50'
evctl --format jsonl 'LOGS FROM authd ERROR ONLY STREAM'
```

Run `evctl help` for socket, file, stdin and output-format options. Initial
results are transactionally spooled until eventd sends `end` or `watch`, so a
late query error can never leak an invalid partial result to a pipeline.

The implementation follows the eventd TRM in the Peios `learn` repository.

Run `tools/check` before committing. Run `tools/bench` on deployment-class
hardware before changing queue, batch or SQLite parameters. `pekit build`
produces the release tree and `pekit package --version <version>` produces the
Peios package, including `evctl`, the inert registry seeds and the standard
`/var/state/eventd/{events,logs,metrics}` directory skeleton.

The check also runs the queue's Loom models in a separate target directory.
They explore publication to an idle consumer, capacity release to a blocked
producer, and reservation racing with close, using the actual queue code and
instrumented atomics, payload cells, mutexes and condition variables. To run
only these models:

```sh
CARGO_TARGET_DIR=target/loom RUSTFLAGS="--cfg loom" \
  cargo test --locked -p eventd-core --lib queue::loom_tests -- --test-threads=1
```

The production handshake uses explicit sequentially consistent fences.
Loom 0.7.2 models these fences; its documented treatment of `SeqCst` accesses
as weaker orderings cannot validate an accesses-only implementation. These
bounded models supplement the host stress and timeout tests; they are not
hardware qualification or an exhaustive proof of the entire daemon.
