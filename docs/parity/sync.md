# Parity Sync Procedure

This repository tracks parity against the Python `openai-chatkit` package pinned
in `docs/parity/upstream.json`.

## When Upstream Changes

1. Update the `packages/chatkit-python` submodule to the target upstream commit.
2. Record the package name, version, submodule path, and commit in
   `docs/parity/upstream.json`.
3. Review upstream release notes and the Python test diff since the previous
   pinned commit.
4. Update `docs/parity/matrix.json` for changed public models, request types,
   stream events, widget behavior, store contracts, Agents behavior, and tests.
5. Port corresponding TypeScript code and Bun tests.
6. Run local verification:

```bash
bun run verify:parity
```

The opt-in `bun run verify:parity` wrapper runs the normal Bun verification and
prints the optional Python upstream command.

7. When the Python submodule environment is available, run:

```bash
cd packages/chatkit-python
make test
```

## Matrix Status Values

- `covered`: Bun has local tests for the behavior and no known parity gap.
- `partial`: Bun covers the main behavior, but the row still has known limits or
  related sub-gaps.
- `intentional-difference`: Bun deliberately differs from Python, and the row
  notes why.
- `deferred`: The behavior is known but not implemented in Bun yet.
- `not-applicable`: The upstream behavior does not apply to this Bun port.

## Rules

- Keep `docs/parity/upstream.json` and `docs/parity/matrix.json` in sync.
- Every non-deferred matrix row must cite local tests, source files, or docs.
- Do not add networked OpenAI calls to parity tests.
- Do not make Python pytest part of the default `bun run verify` command.
- Choose future parity implementation slices from `deferred` and `partial` rows.
