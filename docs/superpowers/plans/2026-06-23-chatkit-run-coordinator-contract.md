# ChatKit RunCoordinator Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Keep task commits small and run the verification listed in each task.

**Goal:** Replace package-owned in-memory run management with a required app-owned `RunCoordinator` contract, an explicit cancel helper, detach-only SSE disconnect behavior, and Vercel-safe documentation.

**Architecture:** `src/http.ts` depends on a narrow public `RunCoordinator` contract. `ChatKitServer.processRequest(...)` remains the canonical request parsing boundary and exposes streaming descriptor metadata to HTTP results. The HTTP handler passes the parsed descriptor, raw request bytes, context, and source stream to `runCoordinator.startRun(...)`. Response cancellation detaches only the SSE subscription. Explicit cancellation is exposed through a helper handler backed by the same app-owned coordinator. Live attach remains a coordinator primitive that applications call from streaming custom actions when they want reconnect events to flow through ChatKit's UI.

**Hosting model:** The package does not implement Redis, Postgres, pub/sub, queues, Vercel Workflows, or durable continuation. Applications provide that infrastructure behind `RunCoordinator`. On Vercel, SSE is still the browser transport, while app-owned infrastructure must own any backend run that outlives one request invocation.

**Execution note:** This implementation is being done on branch `codex/run-coordinator-contract` in the current workspace by user request. Do not create a worktree for this run.

---

## Scope Check

This plan covers the HTTP/run-lifecycle contract boundary for `chatkit-nodejs`.

In scope:

- Public `RunCoordinator` contract types.
- Required `runCoordinator` in `createChatKitHandler(...)`.
- Explicit cancel helper handler.
- Detach-only response cancellation behavior.
- Removal of the package-owned in-memory response run manager.
- Tests and docs proving Vercel-compatible ownership boundaries.

Out of scope:

- A durable Vercel coordinator implementation.
- Redis/pub-sub/Postgres/queue/workflow code in `src/`.
- Browser protocol changes in ChatKit JS.
- A production attach replay store.

## File Structure

- Create `src/run-coordinator.ts`.
- Create `src/stream-serialization.ts` or an equivalent shared serialization helper module.
- Modify `src/server.ts`.
- Modify `src/http.ts`.
- Modify `src/index.ts`.
- Delete `src/run-manager.ts`.
- Modify `tests/http.test.ts`.
- Create `tests/http-types.test.ts`.
- Delete `tests/run-manager.test.ts`.
- Modify `tests/parity-smoke.test.ts`.
- Modify `tests/package-smoke/package-smoke.test.ts`.
- Create `docs/protocol/chatkit-js-stop-refresh.md`.
- Modify `README.md`.
- Modify `docs/parity/matrix.json`.

## Review Corrections Applied

The external plan review found blockers in the first draft. Implement this corrected plan, not the superseded draft.

- Do not reparse streaming HTTP requests in `src/http.ts` after `server.processRequest(...)`. Expose parsed descriptor metadata from the canonical `ChatKitServer.processRequest(...)` path and read it from `StreamingEventResult`.
- Do not use invalid streaming test bodies such as `{"type":"threads.create","params":{}}`. Use the existing valid `createThreadRequest(...)` helper or an equivalent valid request body.
- Do not add raw `JSON.stringify(event)` SSE serialization for attach. Attach and normal streaming must share one sanitizer-backed SSE serialization helper.
- Do not remove explicit cancellation coverage without replacement. Add an HTTP-boundary test proving `createChatKitRunCancelHandler(...)` can abort an app-owned runtime and preserve Store cancellation side effects.
- Do not reuse the full `ChatKitHandlerOptions` type for helper routes if it grows beyond helper needs. Add a focused `RunCoordinatorHandlerOptions<TContext>` with only `getContext` and `runCoordinator`; have `ChatKitHandlerOptions<TContext>` extend it.
- Update `tests/parity-smoke.test.ts` handler construction to pass a coordinator fixture.
- Use the real package smoke path: `tests/package-smoke/package-smoke.test.ts`.
- Update README examples, not only the lifecycle section.
- Update `docs/parity/matrix.json` so old lifecycle wording does not survive.

---

## Task 1: Public RunCoordinator Contract

**Files:**

- Create `src/run-coordinator.ts`
- Modify `src/index.ts`
- Create `tests/http-types.test.ts`

### Steps

- [ ] Add `tests/http-types.test.ts` as a compile-time contract test.
- [ ] Verify `npm run build:test` fails before implementation because `src/run-coordinator.ts` does not exist and old handler options are still accepted.
- [ ] Create `src/run-coordinator.ts` with the public lifecycle contract:
  - `RunStatus`
  - `RunDetachReason`
  - `RunStartDescriptor`
  - `StartRunOptions<TContext, TEvent>`
  - `RunSubscription<TEvent>`
  - `StartRunResult<TEvent>`
  - `AttachRunOptions<TContext>`
  - `AttachRunResult<TEvent>`
  - `CancelRunOptions<TContext>`
  - `CancelRunResult`
  - `RunCoordinator<TContext, TEvent>`
- [ ] `StartRunOptions.source` must accept `ChatKitStreamRuntime` and return `AsyncIterable<TEvent>`.
- [ ] `RunStartDescriptor` must include:
  - `requestType: string`
  - optional `threadId`
  - optional `itemId`
  - `receivedAt: string`
  - `rawRequest: Uint8Array`
- [ ] `AttachRunResult` must use structured not-attachable reasons:
  - `not_found`
  - `forbidden`
  - `finished`
  - `expired`
  - `unavailable`
- [ ] `CancelRunResult` must include:
  - `cancelled`
  - `cancelling`
  - `already_finished`
  - `not_found`
  - `forbidden`
- [ ] Export the new contract from `src/index.ts`.
- [ ] Keep `export * from "./run-manager.js";` for now; remove it in Task 4.
- [ ] Verify `npm run build:test` now fails only because `createChatKitHandler(...)` has not been migrated yet.
- [ ] Commit: `Add RunCoordinator public contract`.

### Type Test Requirements

`tests/http-types.test.ts` must prove:

- `createChatKitHandler(server, { getContext, runCoordinator })` compiles.
- Calling `createChatKitHandler(server)` is a `@ts-expect-error`.
- Calling with only `getContext` and no `runCoordinator` is a `@ts-expect-error`.
- Passing removed `runManager` is a `@ts-expect-error`.
- Passing removed `disconnectBehavior` is a `@ts-expect-error`.
- Public types can be imported from `../src/run-coordinator.js`.

---

## Task 2: Required Coordinator in HTTP Handler

**Files:**

- Modify `src/server.ts`
- Modify `src/http.ts`
- Modify `tests/http.test.ts`
- Test `tests/http-types.test.ts`

### Steps

- [ ] Add streaming descriptor metadata to `StreamingEventResult` in `src/server.ts`.
- [ ] Keep `ChatKitServer.processRequest(...)` as the canonical parsing path. It already parses with `ChatKitRequestSchema.parse(decodeJsonBytes(request))`; extend that path to derive descriptor metadata from the parsed request.
- [ ] Do not parse the raw body again in `src/http.ts` for normal server requests.
- [ ] Add a small helper in `src/server.ts` to derive descriptor metadata:
  - `requestType` from `request.type`
  - `threadId` from `params.thread_id` when present
  - `itemId` from `params.item_id` when present
- [ ] Preserve public constructor compatibility for `StreamingEventResult` if practical by making descriptor metadata optional. Tests that create streaming results directly should provide descriptor metadata explicitly.
- [ ] Introduce `RunCoordinatorHandlerOptions<TContext>` in `src/http.ts` with only:
  - `getContext?: (request: Request) => TContext | Promise<TContext>`
  - `runCoordinator: RunCoordinator<TContext, ThreadStreamEvent>`
- [ ] Make `ChatKitHandlerOptions<TContext>` extend `RunCoordinatorHandlerOptions<TContext>`.
- [ ] Remove `ResponseRunManager`, `DisconnectBehavior`, `runManager`, `disconnectBehavior`, and `supportsExplicitCancel` from `src/http.ts`.
- [ ] `createChatKitHandler(...)` must require `options: ChatKitHandlerOptions<TContext>` at the type level.
- [ ] Keep a runtime guard for JavaScript callers: streaming requests without `runCoordinator` return HTTP 500 with `configuration_error`.
- [ ] For streaming results, call `runCoordinator.startRun(...)` with:
  - request context
  - descriptor metadata from `StreamingEventResult` plus `receivedAt` and a defensive copy of `rawRequest`
  - `source: (runtime) => result.stream(runtime)`
- [ ] If the coordinator returns `not_started`, map:
  - `forbidden` to 403
  - `conflict` to 409
  - `unavailable` to 503
- [ ] If the coordinator returns `started`, stream the returned subscription as SSE.
- [ ] Set `x-chatkit-run-id` on streaming responses.
- [ ] Response body cancellation must call `RunSubscription.detach("subscriber_cancelled")` and must not call `cancelRun(...)`.
- [ ] Keep non-streaming responses as JSON and do not require coordinator work for them beyond the handler option type.

### HTTP Test Requirements

Update `tests/http.test.ts` to use app-owned coordinator fakes instead of `ResponseRunManager`.

Required helpers:

- `RecordingRunSubscription` implementing `RunSubscription<ThreadStreamEvent>`.
- `RecordingRunCoordinator<TContext>` implementing `RunCoordinator<TContext, ThreadStreamEvent>`.
- A `defaultRuntime()` helper using a fresh non-aborted `AbortSignal`.

Required tests:

- Non-streaming responses still return JSON.
- `getContext` still resolves per-request context.
- Streaming responses return `text/event-stream` and `x-chatkit-run-id`.
- Streaming requests pass context, descriptor metadata, and raw request bytes to the coordinator.
- Streaming request without `runCoordinator` through an unsafe JavaScript call returns the runtime configuration error.
- Response body cancellation detaches the subscription and does not cancel the run.
- App-owned coordinator can drain a first-turn run after subscriber detach and persisted Store state is recoverable.
- Source failures still reject the response body instead of closing cleanly.

Use valid streaming bodies for all streaming request tests. Prefer the existing `createThreadRequest(...)` helper.

### Verification

- [ ] Before implementation, verify `npm run build:test && node --test .tmp/test/tests/http.test.js .tmp/test/tests/http-types.test.js` fails.
- [ ] After implementation, verify the same command passes.
- [ ] Commit: `Require RunCoordinator in HTTP handler`.

---

## Task 3: Explicit Cancel Helper and Shared Stream Serialization

**Files:**

- Modify `src/http.ts`
- Modify `src/server.ts`
- Create or modify shared serialization helper module
- Modify `tests/http.test.ts`

### Steps

- [ ] Export `createChatKitRunCancelHandler(...)` from `src/http.ts`.
- [ ] The helper handler must accept `RunCoordinatorHandlerOptions<TContext>`, not the broader handler-only options if those diverge later.
- [ ] The helper handler must parse JSON request bodies shaped as `{ "run_id": "run_..." }`.
- [ ] Invalid or missing `run_id` returns HTTP 400 with an `invalid_request` error.
- [ ] Cancel helper calls `runCoordinator.cancelRun({ runId, context })`.
- [ ] Cancel result mapping:
  - `cancelled`, `cancelling`, `already_finished` -> 200
  - `not_found` -> 404
  - `forbidden` -> 403
- [ ] `ChatKitServer.serializeStreamingEventForHandler(...)` should delegate to the shared sanitizer-backed helper used by normal HTTP streaming.

### HTTP Test Requirements

Add tests proving:

- Cancel helper calls `cancelRun(...)` with request context.
- Cancel helper maps forbidden and missing runs to structured responses.
- Cancel helper rejects missing `run_id`.
- Cancel helper can abort an app-owned runtime with `supportsExplicitCancel: true`.
- The abort path preserves existing Store behavior: partial assistant content is saved and hidden cancellation context is recorded.
- Package smoke coverage proves `createChatKitRunAttachHandler(...)` is not exported; applications should call `RunCoordinator.attachRun(...)` from streaming `ChatKitServer.action(...)` handlers instead.

### Verification

- [ ] Before each helper implementation, add the failing tests first.
- [ ] Verify `npm run build:test && node --test .tmp/test/tests/http.test.js` fails for missing helper exports before implementation.
- [ ] Verify `npm run build:test && node --test .tmp/test/tests/http.test.js` passes after implementation.
- [ ] Commit: `Add explicit run lifecycle helpers`.

---

## Task 4: Remove Package-Owned In-Memory Run Manager and Migrate Smokes

**Files:**

- Delete `src/run-manager.ts`
- Delete `tests/run-manager.test.ts`
- Modify `src/index.ts`
- Modify `tests/parity-smoke.test.ts`
- Modify `tests/package-smoke/package-smoke.test.ts`

### Steps

- [ ] Update `tests/parity-smoke.test.ts` to construct `createChatKitHandler(server, { getContext, runCoordinator })`.
- [ ] Add a tiny parity coordinator fixture that immediately streams `options.source(defaultRuntime())`.
- [ ] Keep the parseable SSE parity test intact.
- [ ] Update parity export assertions:
  - `createChatKitRunCancelHandler` is a function.
  - `createChatKitRunAttachHandler` is not exported.
  - `ResponseRunManager` is no longer expected.
- [ ] Update `tests/package-smoke/package-smoke.test.ts` to import and assert runtime exports:
  - `createChatKitRunCancelHandler`
  - `createChatKitRunAttachHandler` is not exported.
- [ ] Add package-smoke type-only imports from `"chatkit-nodejs"` to verify published declarations:
  - `RunCoordinator`
  - `RunSubscription`
  - `StartRunOptions`
- [ ] Remove `export * from "./run-manager.js";` from `src/index.ts`.
- [ ] Delete `src/run-manager.ts` and `tests/run-manager.test.ts`.
- [ ] Search for removed API leaks in `src/` and `tests/`:
  - `ResponseRunManager`
  - `run-manager`
  - `runManager`
  - `disconnectBehavior`
- [ ] Matches are not allowed in `src/` or `tests/`.

### Verification

- [ ] Run `npm run build:test && node --test .tmp/test/tests/parity-smoke.test.js`.
- [ ] Run `npm run test:package`.
- [ ] Commit: `Remove in-memory response run manager`.

---

## Task 5: README, Protocol, and Parity Documentation

**Files:**

- Modify `README.md`
- Create `docs/protocol/chatkit-js-stop-refresh.md`
- Modify `docs/parity/matrix.json`

### Steps

- [ ] Update the main README example around `createChatKitHandler(...)` so it includes a `runCoordinator`.
- [ ] Keep the example concise; it can use a placeholder app-owned coordinator factory, but it must not show package-owned run management.
- [ ] Replace the README stream lifecycle section with the new app-owned coordinator contract:
  - `createChatKitHandler(...)` requires `runCoordinator`.
  - HTTP/SSE disconnect means subscriber detach, not backend cancellation.
  - `x-chatkit-run-id` is the stable header.
  - `createChatKitRunCancelHandler(...)` maps explicit cancellation.
  - Live attach should be implemented through streaming `ChatKitServer.action(...)`.
  - Applications own durable execution, authorization, and replay.
- [ ] Document Vercel hosting compatibility:
  - Use Node.js runtime unless the whole coordinator stack is proven Edge-compatible.
  - Vercel streaming responses count against function max duration.
  - `waitUntil()` and Next.js `after()` can continue work only within the function timeout.
  - Durable continuation needs application infrastructure such as a database, pub/sub, queue, Vercel Workflows, or another worker system.
  - Deploy consumers with Node `24.x` because the package requires Node.js `>=24.15.0`.
- [ ] Create `docs/protocol/chatkit-js-stop-refresh.md` documenting the unverified stop/refresh protocol distinction:
  - Refresh/network/sleep/fetch abort detaches the subscriber.
  - Explicit backend cancellation must call `RunCoordinator.cancelRun(...)` until a browser protocol spike proves a distinct cancel request.
- [ ] Update `docs/parity/matrix.json` lifecycle notes and docs citations so old `ResponseRunManager` and transport-coupled cancellation wording does not remain.

### Verification

- [ ] Run `rg -n "ResponseRunManager|disconnectBehavior|in-process response run manager|transport abort as user cancellation" README.md docs/protocol docs/parity`.
- [ ] Expected: no matches, unless a design spec outside those paths intentionally names removed API.
- [ ] Commit: `Document RunCoordinator hosting contract`.

---

## Task 6: Final Verification and Packaging

**Files:**

- Verify all changed files.

### Steps

- [ ] Run focused lifecycle tests:

```bash
npm run build:test && node --test .tmp/test/tests/http.test.js .tmp/test/tests/http-types.test.js .tmp/test/tests/parity-smoke.test.js
```

- [ ] Run package smoke:

```bash
npm run test:package
```

- [ ] Run full verification:

```bash
npm run verify
```

- [ ] Search for removed API leaks:

```bash
rg -n "ResponseRunManager|run-manager|runManager|disconnectBehavior" src tests README.md docs/protocol docs/parity
```

Expected: no matches.

- [ ] Search for package-owned infrastructure:

```bash
rg -n "redis|postgres|workflow|queue|pubsub|pub-sub|waitUntil|after\\(" src
```

Expected: no matches. Documentation can mention host-app infrastructure, but `src/` must not import or implement it.

- [ ] If verification changes files, inspect and commit only intentional changes.
- [ ] Otherwise, do not create an empty commit.

---

## Self-Review Checklist

- Required `runCoordinator` is enforced by type tests and runtime guard.
- No default in-memory coordinator exists.
- `disconnectBehavior` is removed.
- SSE disconnect detaches only.
- Explicit cancellation is available through helper handler and preserves Store cancellation behavior when the app coordinator aborts runtime execution.
- Attach uses typed outcomes and sanitized SSE serialization.
- `ChatKitServer.processRequest(...)` remains the canonical parsing path; HTTP does not duplicate parse just to create descriptors.
- README and parity docs are Vercel-compatible.
- Package smoke covers runtime exports and public declaration exports.
- `src/` contains no package-owned Redis/Postgres/pub-sub/queue/workflow implementation.
