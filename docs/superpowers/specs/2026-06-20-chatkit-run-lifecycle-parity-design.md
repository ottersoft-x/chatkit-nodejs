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

### Explicit Cancellation

Explicit cancellation is separate from transport close. A cancel signal stops
the backend run, closes upstream iteration, persists useful partial assistant
state, writes the Python-compatible SDK hidden context, and releases the run
from memory.

The package should first map any existing ChatKit frontend cancel request if one
is observed in the wire protocol. If no such request is available in the public
surface, the package exposes a server-level cancellation API that applications
can route from an explicit cancel endpoint or custom action.

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

### Server Streaming Pipeline

The existing `ChatKitServer.processEvents(...)` pipeline remains the authority
for event validation, hidden item suppression, thread mutation persistence,
pending item tracking, and final state persistence.

The lifecycle change should not duplicate persistence in the run manager. The
run manager drives the pipeline and fans out serialized SSE frames after the
pipeline has applied the same persistence rules used today.

### Cancellation Signal Semantics

Add a stream options object that carries an explicit cancellation signal through
server internals:

```ts
interface ChatKitStreamRuntime {
  signal: AbortSignal;
}
```

`respond(...)` and `action(...)` should receive this runtime object:

```ts
respond(thread, inputUserMessage, context, runtime)
action(thread, action, sender, context, runtime)
```

The signal means explicit backend cancellation. It does not fire when a single
SSE subscriber disconnects under the default `"continue"` policy.

`streamAgentResponse(...)` also accepts the explicit cancellation signal. It
must race pending SDK/context reads against abort and close upstream iteration
promptly on explicit cancellation, including never-settling streams.

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
- `stream_options.allow_cancel` remains meaningful for self-hosted backends.
- An existing wire-level cancel operation should be supported if implementation
  discovery finds one.
- If no existing cancel request is available, expose `cancelRun(...)` and
  handler helper primitives for applications to route explicit cancellation.

This design fixes the phone sleep case because thread state continues to
complete even when the original SSE subscription is gone.

## Attachment Metadata Serialization

Attachment metadata remains stored and available to server-side application
code. ChatKit responses must strip it.

Apply stripping to:

- `attachments.create`,
- `threads.get_by_id`,
- `items.list`,
- streamed `thread.item.done` user messages,
- any thread response containing user-message attachments.

Do not mutate stored attachment objects to strip metadata. Serialization should
produce client-safe copies.

## Stream Error Semantics

Add Node equivalents of Python's stream error classes:

- `StreamError(code, { allowRetry })`
- `CustomStreamError(message, { allowRetry })`

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

- response run manager,
- subscriber fanout,
- abort/race helper for stream reads,
- attachment metadata response sanitizer,
- stream error classes.

It does not authorize a broad decomposition of all `src/agents/stream.ts`
responsibilities. That can be a later refactor after behavior is stable.

## Testing

Add focused tests for:

- HTTP response body cancellation detaches the subscriber while the background
  run continues and persists the final assistant item.
- subscriber disconnect does not write cancellation hidden context.
- explicit cancellation stops a run, persists partial assistant text, writes the
  Python-aligned hidden context, and cleans up the run.
- `streamAgentResponse(...)` exits promptly on explicit cancellation with a
  never-settling SDK stream.
- `disconnectBehavior: "cancel"` preserves an opt-in transport-coupled behavior.
- attachment metadata is stripped from create/list/get/stream responses but
  retained in the store.
- `StreamError` and `CustomStreamError` produce the expected ChatKit error
  events and retry policies.
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
