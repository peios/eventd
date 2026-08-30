# eventd

`eventd` is Peios's durable observability daemon. Its first implementation
milestone is the performance-critical path from one lock-free KMES ring per
logical CPU to receipt-backed, WAL-mode SQLite event shards.

The workspace is split deliberately:

- `eventd-core` contains the bounded handoff, event model, recovery coverage,
  boot-ID conversion and SQLite shard writer. It has no Peios-kernel linkage,
  so its tests and benchmarks run on an ordinary development host.
- `eventd` owns KMES attachment and drain threads. It is the only crate linked
  to libpeios.

The implementation follows the eventd TRM in the Peios `learn` repository.
Run `tools/check` before committing. Run `tools/bench` on the deployment-class
hardware before changing queue, batch or SQLite parameters.
