# eventd integration suite

This bundle owns 64 conformance test files and its exported fixtures.
`suite.json` records the imported source commit and maps files into the
qualification run layout. The original MIT notice is in `LICENSE`.

Run fast unit and contract checks through this repository's existing Bazel
checks. Run native conformance against explicitly supplied boot artifacts:

```sh
PEIOS_QUALIFY=/path/to/integration-tests/peios-qualify \
  ./tests/integration/check-integration \
  --lock /path/to/integration-tests/suites.lock.json \
  --composition /path/to/composition.json \
  --output /path/to/fresh-run
```

The same entry point is available as
`bazel run //tests/integration:check -- ...`.
Use absolute input and output paths with Bazel.

The lock selects committed suite revisions, including helper dependencies.
To test a different committed suite, pass `--candidate eventd=/path/to/repo`.
That explicit override requires a clean candidate checkout. It changes the
test source, not the supplied image; record image composition separately.

The central runner assembles shared helpers from their recorded owners. Do
not copy a helper into another component. Generic host utilities belong to
Provium; component helpers belong beside their component tests.

Images must contain the fixtures and services the selected tests expect.
Supplied signed tools must be signed for the test image's trust policy.
The runner resolves supplied tools; it does not compile or sign them.
Missing inputs fail the run, and skipped cases make the result incomplete.

Read [the qualification guide](https://learn.peios.org/peios/developing-for-peios/qualification/test-ownership)
for ownership, composition, and result contracts. Imported assertions retain
their historical specification references; the migration does not certify
those references or establish runtime qualification.
