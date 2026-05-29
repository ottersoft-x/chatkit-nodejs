# ChatKit Parity Foundation Design

## Purpose

This milestone turns ChatKit Bun parity from a collection of successful slices
into a tracked, testable process. The repository already pins the upstream
Python reference in `docs/parity/upstream.json` and has strong Bun coverage for
Agents, server processing, widgets, and core schemas. What is missing is a
single parity inventory that says which upstream behaviors are covered, which
tests prove them, and which gaps are intentional or still open.

The goal is to create a parity foundation that supports full parity over time:
a machine-readable matrix, a short sync procedure, a Bun smoke harness for the
matrix and the most important ChatKit JS event paths, and an opt-in parity
verification command for deeper upstream checks.

## Scope

In scope:

- Add tracked parity matrix data under `docs/parity`.
- Document how to update the upstream pin and matrix when Python ChatKit
  changes.
- Add Bun tests that validate the matrix structure and referenced local files.
- Add end-to-end smoke coverage for representative ChatKit JS event paths that
  cross the Agents bridge, server processing, and stream event wire shape.
- Add `verify:parity` as an opt-in package script, separate from the default
  `bun run verify`, so deeper parity checks can evolve without slowing the
  normal development gate.

Out of scope:

- Implementing missing behavior rows such as `accumulate_text`,
  `previous_response_id`, entity annotation mapping, input-side annotation
  replay, or attachment content conversion.
- Running networked OpenAI calls or live ChatKit JS browser tests.
- Importing the Python submodule from production TypeScript code.
- Changing public ChatKit stream event schemas, Agents converter method names,
  or default server persistence behavior.

## Architecture

The design has three layers.

`docs/parity/matrix.json` is the source of truth for parity status. Each row
names an upstream area, points at the Python reference file or test when one
exists, lists the Bun tests or docs that currently cover the behavior, and marks
the status as one of `covered`, `partial`, `intentional-difference`, `deferred`,
or `not-applicable`.

`docs/parity/sync.md` explains the sync process: update the Python submodule,
record the package version and commit in `upstream.json`, review upstream
release notes and test diffs, update matrix rows, then port code and tests.
This keeps the existing port design promise concrete without forcing every
developer to rediscover the process.

`tests/parity-smoke.test.ts` validates the matrix and exercises a small number
of full-path behaviors. The smoke tests should stay thin: they prove that the
bridge/server/wire path works for critical ChatKit JS scenarios, while detailed
behavior coverage remains in the focused suites such as `tests/agents.test.ts`,
`tests/agents-converter.test.ts`, and `tests/server.test.ts`.

## Matrix Shape

The matrix should be JSON so Bun tests can validate it without a custom parser.
Rows should be explicit and stable enough to survive upstream churn.

Required top-level fields:

- `schemaVersion`: integer, starting at `1`.
- `upstream`: object that mirrors the package name, version, submodule path,
  and commit from `docs/parity/upstream.json`.
- `rows`: array of parity rows.

Required row fields:

- `id`: stable kebab-case identifier.
- `area`: broad surface such as `agents-output`, `agents-input`, `server`,
  `widgets`, `types`, or `http`.
- `status`: `covered`, `partial`, `intentional-difference`, `deferred`, or
  `not-applicable`.
- `upstream`: object with Python file and test names when available.
- `bun`: object with local test files, source files, and optional docs that
  prove or explain the row.
- `notes`: concise explanation of coverage, gaps, or intentional differences.

Initial rows should cover the existing high-value surfaces:

- Agents output streaming through `streamAgentResponse`.
- Agents input conversion through `ThreadItemConverter` and
  `simpleToAgentInput`.
- Annotation conversion and server annotation merge behavior.
- Workflows, generated images, guardrail rollback, client tool deferral, and
  context-injected events.
- Server request processing and ChatKit wire event schemas.
- Widgets and copied upstream widget fixtures.
- Upstream metadata pinning.
- Known deferred gaps: `accumulate_text`, `previous_response_id`, entity
  annotation sources, input-side annotation replay, attachment content
  conversion, and non-text assistant content parts not yet represented on the
  ChatKit wire.

## Smoke Harness

The first smoke harness should avoid duplicating the thousands of assertions in
the focused suites. It should test integration boundaries that are otherwise
spread across separate files.

Matrix validation smoke:

- Load `docs/parity/upstream.json` and `docs/parity/matrix.json`.
- Assert the matrix upstream fields match the pinned upstream metadata.
- Assert every row has a unique id, valid status, non-empty notes, and at least
  one Bun test/doc/source reference unless the status is `deferred` or
  `not-applicable`.
- Assert every local file reference exists.

ChatKit JS event-path smoke:

- Build a minimal Agents stream fixture that produces assistant text with
  annotations.
- Pass it through `streamAgentResponse`.
- Feed emitted events through `ChatKitServer.processEvents` using a local
  in-memory test store that preserves the real persistence merge logic.
- Assert emitted events parse with `ThreadStreamEventSchema`.
- Assert stored assistant content keeps streamed annotations even when final
  done events carry empty annotation arrays.

HTTP/SSE smoke:

- Exercise `createChatKitHandler` with a deterministic streaming responder.
- Parse the `text/event-stream` response into JSON events.
- Assert the response contains ChatKit wire events for a user-visible assistant
  item and a terminal done update.

These smoke tests should use local fixtures only. They should not call OpenAI,
start a browser, or require the Python submodule to be installed.

## Opt-In Parity Verification

Add a package script named `verify:parity` that runs Bun's normal verification
plus the parity smoke test. Add `scripts/verify-parity.ts` as the helper entry
point for future upstream checks. In this milestone, the helper prints the
Python upstream command to run when the submodule environment is available and
does not make pytest mandatory.

The first version should not make Python pytest a mandatory part of
`bun run verify`. The Python submodule is a reference dependency, and requiring
its environment for every Bun verification would make the main development loop
more fragile. A future milestone can make the upstream pytest runner stricter
once the parity matrix and local smoke harness are stable.

## Error Handling

Malformed matrix rows should fail loudly in Bun tests with messages that name
the row id and missing field. Missing local file references should fail with the
exact referenced path.

If the Python submodule is absent or not initialized, default Bun verification
should still pass as long as `docs/parity/upstream.json` and
`docs/parity/matrix.json` are valid. Optional parity tooling may report the
missing submodule as a skipped upstream check, but it should not mask matrix or
local smoke failures.

## Testing

Focused verification for this milestone:

- `bun test tests/parity-metadata.test.ts tests/parity-smoke.test.ts`
- `bun run typecheck`
- `bun run verify`

`bun run verify:parity` should be tested directly as part of this milestone.

## Success Criteria

- The repository has a tracked parity matrix tied to the pinned upstream Python
  package metadata.
- The matrix makes existing coverage and known deferred parity gaps visible.
- Bun tests validate the matrix structure and local references.
- At least one full-path smoke test covers ChatKit JS-compatible stream events
  across Agents conversion and server persistence.
- Default verification remains fast and local.
- The next behavior parity slices can be chosen from matrix rows rather than
  from memory.
