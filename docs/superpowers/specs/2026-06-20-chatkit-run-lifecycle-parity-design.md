# ChatKit Run Lifecycle and Parity Hardening Design

## Summary

This design fixes the code review findings as one cohesive parity and lifecycle
pass. The main product change is to separate the HTTP/SSE transport lifecycle
from the backend response run lifecycle.

Today, a dropped stream can be interpreted as cancellation. That is the wrong
default for mobile browsers: if an iPhone locks while a message is streaming,
the backend should continue processing in-process, persist the final answer, and
allow the client to recover by reloading or fetching updates. Explicit user
cancellation remains a real backend cancellation and stops upstream work.

The same pass also fixes attachment metadata serialization parity, Python-style
stream error semantics, cancellation hidden-context text, and the high-risk test
coverage gaps that let these issues escape.

## Context

The review found five relevant risks:

- HTTP response cancellation currently only calls `return()` on the async
  iterator. In JavaScript, that does not preempt an already pending async wait.
- Attachment `metadata` is returned to clients in Node responses, while Python
  treats it as integration-only metadata and strips it from ChatKit responses.
- Node flattens stream errors into generic retryable `stream.error` events,
  while Python supports `StreamError` and `CustomStreamError`.
- Node writes weaker cancellation hidden context than Python.
- `src/agents/stream.ts` is large and mixes enough responsibilities that stream
  lifecycle issues are hard to reason about.

The public `openai/chatkit-js` repository was inspected at commit
`26ffdd8e243c6ce42005b5ad103f2feb7f6b84bf`. Its public React/web-component
types expose `sendUserMessage`, `sendCustomAction`, `fetchUpdates`, and response
start/end events, but not a clearly named public `cancelResponse()` method. The
docs list response cancelling as a self-hosted backend capability, so this
design must support explicit cancellation without assuming a public frontend
method name.

## Goals

- Treat HTTP/SSE disconnects as subscriber loss by default, not user
  cancellation.
- Continue active response runs in-process after subscriber disconnect and
  persist final thread state.
- Preserve explicit user cancellation as a real backend cancellation.
- Keep response-run continuation in-process only; process crash or server
  restart durability is out of scope.
- Fix attachment metadata response serialization to match Python.
- Add Python-style stream error classes and event mapping.
- Align cancellation hidden context with Python.
- Update docs and parity metadata so the semantics are explicit.
- Add tests that reproduce dropped subscriber behavior, explicit cancellation,
  attachment metadata stripping, and stream error mapping.

## Non-Goals

- No durable background queue, persisted leases, or restart recovery for active
  runs.
- No distributed pub/sub or cross-process run coordination.
- No broad rewrite of `src/agents/stream.ts`.
- No change to attachment delete ordering unless implementation discovery shows
  a direct user-facing defect.
- No narrowing of Node's permissive `inference_options` catchall unless it
  conflicts with ChatKit JS.
- No frontend package changes.

## Public Behavior

### Disconnect

By default, closing the HTTP/SSE stream detaches the subscriber from the active
run. The run continues consuming `respond(...)` in-process and persists the
final state through the existing server event pipeline.

This covers mobile browser sleep, tab suspension, network changes, and browser
reloads. The original SSE request may report failure in the frontend, but the
thread state becomes recoverable through normal reload or `fetchUpdates()`.

For `threads.create`, the frontend may disconnect before it receives the
`thread.created` event and therefore may not know the new `thread_id`. The
server must still start the run eagerly, create and persist the thread, and
persist the submitted user message without waiting for the first subscriber pull.
Recovery for that case relies on `threads.list` for the authenticated context,
not `items.list` for a known thread id. The README should call this out so apps
with custom navigation know to refresh thread history after reconnect.

### Explicit Cancellation

Explicit cancellation is separate from transport close. A cancel signal stops
the backend run, closes upstream iteration, persists useful partial assistant
state, writes the Python-compatible SDK hidden context, and releases the run
from memory.

The package should first map any existing ChatKit frontend cancel request if one
is observed in the wire protocol and that request carries an unambiguous run
identity. If no such request is available in the public surface, the package
exposes a server-level cancellation API that applications can route from an
explicit cancel endpoint or custom action.

Cancellation must be keyed by a run id or run token, not by `thread_id`.
Multiple response turns may exist for one thread, and cancelling by thread id
would be ambiguous. Unknown run ids are treated as idempotent no-ops by default
and return a structured `"not_found"` or `"already_finished"` result rather than
throwing from the manager. Applications may choose to translate that result into
their own HTTP status.

The cancellation result should be explicit enough for handlers and tests:

```ts
type CancelRunResult =
  | { status: "cancelled" }
  | { status: "cancelling" }
  | { status: "already_finished" }
  | { status: "not_found" }
  | { status: "forbidden" };
```

### Configuration

The default behavior should be safe for normal integrations:

```ts
createChatKitHandler(server, {
  disconnectBehavior: "continue",
});
```

`disconnectBehavior` defaults to `"continue"`. A `"cancel"` option remains
available for applications that intentionally want old transport-coupled
behavior or tests that need it.

When no frontend wire-level cancel operation is available, integrations using
`disconnectBehavior: "continue"` should not advertise `allow_cancel: true` to
ChatKit JS unless they have provided an explicit cancel route/control that can
carry the active run id. If the only available cancel gesture is fetch/SSE abort,
that gesture cannot be distinguished from phone sleep and should not be treated
as explicit cancellation under the default continue policy.

## Architecture

### ResponseRunManager

Add an in-process response run manager. It owns active runs and provides:

- `startRun(...)` for a new streaming request.
- `subscribe(runId, ...)` for a live SSE subscriber.
- `cancelRun(...)` for explicit user cancellation.
- automatic cleanup after run completion, failure, or cancellation.

The manager belongs to the server-helper layer because most applications use
`createChatKitHandler` or copy the README server pattern. The application still
owns long-running production policy, authentication, infrastructure durability,
and any process-external orchestration.

The manager should be injectable and exportable:

```ts
const runManager = new ResponseRunManager<RequestContext>({
  getRunScope: (context) => context.userId,
});

const chatkitHandler = createChatKitHandler(server, {
  runManager,
  disconnectBehavior: "continue",
});

await runManager.cancelRun({ runId, context });
```

`getRunScope` is an authorization boundary for in-memory run operations. If
provided, subscribe and cancel operations only succeed when the caller's context
resolves to the same scope as the run's creator. Run ids should also be
unguessable. Applications remain responsible for authenticating their HTTP
routes before calling the manager.

### ResponseRun

A response run represents one backend response turn. It contains:

- a unique run id,
- thread id once known,
- request type and input item id when available,
- a cancellation `AbortController`,
- current status: running, completed, failed, cancelled,
- an event fanout for current subscribers,
- a completion promise,
- cleanup hooks.

The run drains the server's streaming event source independently of subscribers.
Subscriber count affects only live delivery, not backend execution.

A run is not keyed only by thread id. A thread may receive multiple turns over
time, and implementation must not attach a new message request to an unrelated
active run just because the thread id matches. The initial streaming request
starts the run; later live subscribers attach by the returned run id or another
explicit run token if the frontend wire protocol provides one.

Every normal streaming request starts a new run unless the request is an
explicit subscription request carrying a valid `run_id`/run token. The package
must not infer subscription or cancellation from `thread_id`.

The handler should expose the run id to capable clients through an HTTP response
header such as `x-chatkit-run-id`. If implementation discovery finds an existing
ChatKit JS run/cancel token, use that wire shape instead of inventing a second
one. Existing ChatKit JS integrations that cannot observe the run id still get
the default disconnect-continuation behavior and can recover completed state via
thread reload/list operations.

### Server Streaming Pipeline

The existing `ChatKitServer.processEvents(...)` pipeline remains the authority
for event validation, hidden item suppression, thread mutation persistence,
pending item tracking, and final state persistence.

The lifecycle change should not duplicate persistence in the run manager. The
run manager drives the pipeline and fans out serialized SSE frames after the
pipeline has applied the same persistence rules used today.

The manager must operate at the `ThreadStreamEvent` level, not by reparsing SSE
bytes. Add an internal event-level server API for parsed streaming requests plus
runtime, and make both `ChatKitServer.process(...)` and `createChatKitHandler`
reuse it. A possible shape:

```ts
protected processStreamingEvents(
  request: StreamingRequest,
  context: TContext,
  runtime: ChatKitStreamRuntime,
): AsyncIterable<ThreadStreamEvent>;
```

Serialization to SSE bytes should happen once, after the event has passed
through persistence, hidden-item filtering, cancellation handling, and client
payload sanitization.

### Cancellation Signal Semantics

Add a stream options object that carries an explicit cancellation signal through
server internals:

```ts
interface ChatKitStreamRuntime {
  signal: AbortSignal;
  supportsExplicitCancel: boolean;
}
```

`respond(...)` and `action(...)` should receive this runtime object:

```ts
respond(thread, inputUserMessage, context, runtime)
action(thread, action, sender, context, runtime)
```

The signal means explicit backend cancellation. It does not fire when a single
SSE subscriber disconnects under the default `"continue"` policy.

`getStreamOptions(...)` should receive this runtime or otherwise be
post-processed by the server pipeline so `stream_options.allow_cancel` is only
`true` when `supportsExplicitCancel` is true. Existing app overrides remain
possible, but the default package behavior must not advertise cancellation when
the only available signal is transport abort.

`streamAgentResponse(...)` also accepts the explicit cancellation signal. It
must race pending SDK/context reads against abort and close upstream iteration
promptly on explicit cancellation, including never-settling streams.

Cancellation propagates through a sentinel error, reusing or extending
`StreamCancelledError`. Abort-race helpers should reject with that sentinel, and
`processEvents(...)` must catch it before the generic stream-error path. The
pipeline should call `handleStreamCancelled(...)` exactly once per cancelled run,
after saving any thread mutation observed so far and before run cleanup.

`cancelRun(...)` aborts the run controller, requests iterator cleanup with
`return()` when available, closes subscriber streams, and waits for cooperative
cancellation handling to finish. Custom `respond(...)` and `action(...)`
implementations are expected to observe `runtime.signal` or delegate to helpers
such as `streamAgentResponse(...)`. Non-cooperative app code that ignores the
signal cannot be preempted by JavaScript; in that case cancellation remains
best-effort until the app iterator yields, throws, or returns.

### HTTP Handler

`createChatKitHandler` should:

1. parse request context as it does today,
2. start or attach to a response run for streaming requests,
3. return an SSE `ReadableStream` subscribed to that run,
4. detach that subscriber when the response body is cancelled,
5. only cancel the run on explicit cancellation or when
   `disconnectBehavior: "cancel"` is configured.

The README raw Node HTTP example should also be updated so `ServerResponse`
close detaches or cancels according to the configured behavior instead of
silently leaking the stream loop.

## Frontend Contract

The Node package should not rely on an undocumented public `chatkit-js`
cancellation method. It should support the frontend as observed:

- `fetchUpdates()` is the recovery path after a browser returns from sleep or a
  failed live stream.
- `threads.list` is the recovery path if a first-turn `threads.create`
  disconnect happens before the frontend sees `thread.created`.
- `stream_options.allow_cancel` remains meaningful only when explicit
  cancellation can reach the backend without being confused with transport
  disconnect.
- An existing wire-level cancel operation should be supported if implementation
  discovery finds one and it is distinguishable from ordinary fetch/SSE abort.
- If no existing cancel request is available, expose `cancelRun(...)` and
  handler helper primitives for applications to route explicit cancellation.

This design fixes the phone sleep case because thread state continues to
complete even when the original SSE subscription is gone.

## Attachment Metadata Serialization

Attachment metadata remains stored and available to server-side application
code. ChatKit responses must strip it.

Implement this with one client-safe payload sanitizer, not scattered endpoint
branches. The sanitizer should produce deep-enough copies for:

- attachments,
- thread items,
- pages of thread items,
- thread responses,
- stream events containing thread items or thread responses.

Every response serialization path and every SSE event path should pass through
the sanitizer before bytes are emitted to the client. Stored objects and objects
passed to application callbacks must not be mutated.

Coverage must include:

- `attachments.create`,
- `threads.get_by_id`,
- `items.list`,
- streamed `thread.item.done` user messages,
- `thread.item.replaced` and future item-bearing events,
- any thread response containing user-message attachments.

## Stream Error Semantics

Add Node equivalents of Python's stream error classes:

- `StreamError(code, { allowRetry })`
- `CustomStreamError(message, { allowRetry })`

The wire schema should treat error codes as open strings, matching Python's
comment that new error codes can be added. Known code defaults:

- `stream.error`: `allowRetry` defaults to `true`.
- `custom`: `allowRetry` defaults to `false`.
- any other string code: `allowRetry` defaults to `false` unless overridden.

`processEvents(...)` maps them before the generic error path:

- `CustomStreamError` emits `{ type: "error", code: "custom", message,
  allow_retry }`.
- `StreamError` emits `{ type: "error", code, allow_retry }`.
- generic thrown errors continue to emit retryable `stream.error`.
- explicit cancellation does not emit a retryable stream error.

These classes should be exported from the package's existing public error
surface.

## Cancellation Hidden Context

The default hidden context for explicit cancellation should match Python:

```text
The user cancelled the stream. Stop responding to the prior request.
```

Disconnects under the default `"continue"` behavior must not write this hidden
context.

## Structural Scope

This spec allows targeted extraction only where needed for lifecycle correctness
and testability:

- `src/run-manager.ts` or equivalent: response run manager, run records,
  subscriber fanout, run id generation, and run cleanup.
- `src/stream-runtime.ts` or equivalent: cancellation runtime types,
  abort-race helper, and cancellation sentinel helpers.
- `src/response-sanitizer.ts` or equivalent: client-safe copies for
  attachments, thread items, thread responses, pages, and stream events.
- `src/errors.ts`: exported error classes, including Python-style stream errors.

`src/server.ts` should stay responsible for ChatKit request routing and the
event persistence pipeline. It should not absorb run manager internals,
subscriber queues, or sanitizer traversal logic. `src/agents/stream.ts` should
receive the minimum cancellation hooks needed for `streamAgentResponse(...)`
without a broad decomposition of all agent stream responsibilities. That broader
refactor can be a later change after behavior is stable.

## Testing

Add focused tests for:

- HTTP response body cancellation detaches the subscriber while the background
  run continues and persists the final assistant item.
- `threads.create` continues and persists a discoverable thread when the
  subscriber disconnects before reading the first SSE chunk; recovery is possible
  through `threads.list`.
- subscriber disconnect does not write cancellation hidden context.
- explicit cancellation stops a run, persists partial assistant text, writes the
  Python-aligned hidden context, and cleans up the run.
- two simultaneous runs on the same thread cannot be cancelled, subscribed to,
  or completed by using only `thread_id`; run id is required.
- `cancelRun(...)` is idempotent for already-finished or unknown runs and
  respects the configured run scope/context boundary.
- `streamAgentResponse(...)` exits promptly on explicit cancellation with a
  never-settling SDK stream.
- `disconnectBehavior: "cancel"` preserves an opt-in transport-coupled behavior.
- attachment metadata is stripped from create/list/get/stream responses but
  retained in the store.
- `StreamError` and `CustomStreamError` produce the expected ChatKit error
  events and retry policies.
- a non-`stream.error` `StreamError` code passes schema validation and defaults
  to `allow_retry: false` unless overridden.
- existing parity smoke tests continue to pass.

Run:

```bash
npm run verify:parity
```

## Documentation and Parity Metadata

Update README guidance to make the lifecycle rules clear:

- ordinary disconnect means subscriber detach,
- explicit cancel means backend cancellation,
- active runs are in-process only,
- clients should call or rely on `fetchUpdates()` after reconnect/reload to
  recover completed thread state.

Update parity docs to record the intentional improvement over Python's current
transport-coupled cancellation behavior. Keep Python-aligned behavior for
explicit cancellation, hidden context, attachment metadata, and stream errors.

## Rollout Notes

This is a behavioral change with a safer default. Applications that depended on
transport close cancelling inference can opt into `disconnectBehavior: "cancel"`.
Applications that want production-grade durability across restarts must still
provide their own job infrastructure or run manager integration later.
