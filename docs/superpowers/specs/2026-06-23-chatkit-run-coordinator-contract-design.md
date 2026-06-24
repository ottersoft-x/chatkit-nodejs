# ChatKit RunCoordinator Contract Design

## Summary

Update `chatkit-nodejs` so response-run lifecycle coordination is supplied by
the host application instead of the package. The package should no longer ship
or default to an in-memory response run manager. `createChatKitHandler` should
require a `runCoordinator` option, support only disconnect-as-detach behavior,
and expose explicit cancellation through that coordinator without treating
transport abort as user intent.

This is a breaking API cleanup and is a prerequisite for the ePCR Holo port in
`/Users/agarcia17/source/repos/holo-latest`.

## Context

The current package has an in-process run lifecycle path that works for a single
long-lived Node process but is the wrong correctness boundary for Vercel-style
serverless/runtime environments. ePCR Holo needs durable conversation state,
coarse run status, and reconnect recovery without relying on package-owned
process memory.

The application will own persistence and deployment-specific coordination. The
package should keep protocol handling, request parsing, ChatKit server
processing, Store contracts, stream serialization, and type-safe integration
points.

Known constraints:

- The host app uses Vercel and cannot treat process memory as durable.
- Normal browser disconnect, refresh, mobile sleep, and network changes should
  not mean user cancellation.
- The app does not need sequence-numbered replay of every missed transient
  progress event.
- ChatKit JS has a visible stop button, but the public React/Web Component API
  does not currently prove that a stable backend cancellation request is sent.
- The package should not bundle a Postgres, Redis, Vercel Workflow, queue, or
  in-memory coordinator.

## Goals

- Rename the lifecycle boundary to `RunCoordinator`.
- Make `runCoordinator` required by TypeScript for `createChatKitHandler`.
- Remove the package-owned in-memory default.
- Remove `disconnectBehavior`.
- Support only disconnect-as-detach semantics.
- Keep explicit cancellation as a required coordinator capability.
- Provide package-level helpers for explicit run cancellation and optional live
  reattach, so applications have a concrete fallback even if ChatKit JS only
  aborts the transport.
- Fail fast with a clear configuration error if a streaming request reaches a
  handler without a coordinator.
- Keep ChatKit Store and AttachmentStore semantics separate from run lifecycle
  coordination.
- Add tests that lock the new contract and disconnect/cancel semantics.

## Non-Goals

- No in-memory fallback implementation.
- No package-owned SQL schema or database client.
- No bundled Redis/pub-sub implementation.
- No bundled Vercel Workflow, queue, or durable background worker
  implementation.
- No missed-event replay by sequence number.
- No Holo-specific PCR concepts in package APIs.
- No assumption that ChatKit JS stop-button transport abort equals backend
  cancellation.

## Vercel Hosting Contract

The contract must be compatible with Vercel, but the package must not pretend
that Vercel gives process memory or response-detached work durable semantics.

Current Vercel constraints that shape the design:

- Vercel supports Node.js `24.x`, `22.x`, and `20.x`; the package's
  `engines.node >=24.15.0` is deployable only when the consuming project runs on
  Node `24.x`.
- Vercel Functions may reuse an instance, but each incoming request is still a
  function invocation and Vercel scales active functions down when not needed.
- Streaming is supported in the Node.js runtime, but streamed responses count
  against the function's maximum duration and cannot run indefinitely.
- `waitUntil()` or Next.js `after()` can keep work alive after a response is
  sent, but that work still shares the function timeout. It is not durable after
  timeout, crash, deployment replacement, or scale-to-zero.
- Persistent run state, live fanout that crosses function instances, and
  reconnect recovery must use application infrastructure such as a database,
  pub/sub, queue, Vercel Workflows, or another worker system.

Design consequences:

- A Vercel-compatible coordinator owns execution from the start of a run. The
  handler may supply a source function and opaque request descriptor, but the
  coordinator decides whether to execute inline, schedule with `waitUntil()` or
  `after()`, or enqueue a durable worker.
- Detach means the current HTTP/SSE subscriber is gone. It does not imply the
  backend run remains alive unless the coordinator has explicitly arranged
  execution that survives the response close for the deployment target.
- In-process subscriber lists are allowed only as an optimization inside a host
  coordinator. They must not be the package default or the documented
  correctness model.
- The package should document that Vercel consumers should use the Node.js
  runtime, not Edge, for this handler unless an app proves its coordinator and
  dependencies are Edge-compatible.

Relevant Vercel references:

- https://vercel.com/docs/functions/runtimes/node-js/node-js-versions
- https://vercel.com/docs/functions
- https://vercel.com/docs/functions/limitations
- https://vercel.com/docs/functions/runtimes
- https://vercel.com/docs/functions/functions-api-reference/vercel-functions-package
- https://vercel.com/docs/workflows

## Target Public API

The target handler shape is:

```ts
createChatKitHandler(server, {
  getContext,
  runCoordinator,
});
```

Remove these handler options:

- `runManager`
- `disconnectBehavior`

If there is a temporary migration layer, it should be short-lived and clearly
marked as deprecated. The desired end state is a single required
`runCoordinator` option.

The handler must expose the coordinator-generated run id on streaming responses:

```http
x-chatkit-run-id: run_...
```

If the ChatKit JS protocol spike discovers a different stable run token, the
handler may map that token to the coordinator's run id, but it should keep the
header for application-owned cancel and attach actions.

Normal ChatKit streaming requests start a new run. The package must not infer
reattach or cancellation from `thread_id`. Reattach is only allowed through:

- a verified ChatKit JS wire request carrying a stable run token, or
- a streaming custom action that calls `RunCoordinator.attachRun(...)` with an
  explicit `run_id`.

## Contract Shape

`RunCoordinator<TContext, TEvent>` should be a small handler-facing interface
owned by the package and implemented by applications. It should coordinate
run-level execution without forcing the package to own a storage backend.

The contract is intentionally split into typed run and subscriber objects so the
coordinator does not become a catch-all manager:

```ts
type RunStatus = "running" | "completed" | "failed" | "cancelled" | "expired";

type RunDetachReason =
  | "response_closed"
  | "request_aborted"
  | "subscriber_cancelled";

interface RunStartDescriptor {
  requestType: string;
  threadId?: string;
  itemId?: string;
  receivedAt: string;
  rawRequest: Uint8Array;
}

interface StartRunOptions<TContext, TEvent> {
  context: TContext;
  descriptor: RunStartDescriptor;
  source: (runtime: ChatKitStreamRuntime) => AsyncIterable<TEvent>;
}

interface RunSubscription<TEvent> {
  events: AsyncIterable<TEvent>;
  detach(reason: RunDetachReason): Promise<void>;
}

type StartRunResult<TEvent> =
  | {
      status: "started";
      runId: string;
      subscription: RunSubscription<TEvent>;
    }
  | {
      status: "not_started";
      reason: "forbidden" | "conflict" | "unavailable";
      message?: string;
    };

interface AttachRunOptions<TContext> {
  runId: string;
  context: TContext;
}

type AttachRunResult<TEvent> =
  | {
      status: "attached";
      runId: string;
      subscription: RunSubscription<TEvent>;
    }
  | {
      status: "not_attachable";
      reason: "not_found" | "forbidden" | "finished" | "expired" | "unavailable";
      message?: string;
      retryAfterMs?: number;
    };

type CancelRunResult =
  | { status: "cancelled" }
  | { status: "cancelling" }
  | { status: "already_finished" }
  | { status: "not_found" }
  | { status: "forbidden" };

interface CancelRunOptions<TContext> {
  runId: string;
  context: TContext;
}

interface RunCoordinator<TContext, TEvent> {
  startRun(options: StartRunOptions<TContext, TEvent>): Promise<StartRunResult<TEvent>>;
  attachRun(options: AttachRunOptions<TContext>): Promise<AttachRunResult<TEvent>>;
  cancelRun(options: CancelRunOptions<TContext>): Promise<CancelRunResult>;
}
```

`RunStartDescriptor` is package-owned metadata. It gives applications enough
stable information to persist, authorize, enqueue, and observe a run without
making them parse ChatKit item JSON or transient stream events. The package may
add fields during implementation if the existing request schema exposes more
stable identifiers, but the descriptor must remain metadata, not a copy of
ChatKit request handling logic. `rawRequest` should be a defensive copy of the
validated request bytes so an app-owned worker can re-enter the package's normal
request path without depending on in-process closures.

`source` is the package-owned stream producer. A coordinator may execute it in
the current invocation, keep it alive with app infrastructure, or enqueue enough
descriptor/context data for an app-owned worker to execute the same ChatKit
server path. The coordinator, not `createChatKitHandler`, is responsible for
choosing the deployment-specific execution strategy. If the host uses durable
execution, the coordinator is responsible for deriving any serializable job
payload and authorization scope from `context`.

`RunStatus` is a shared vocabulary for app-owned status APIs and tests. It does
not imply a required `RunCoordinator.getStatus(...)` method.

## `RunCoordinator` Responsibilities

The coordinator implementation, including any app-owned storage or worker behind
it, is responsible for:

- starting a response run or returning a structured not-started outcome,
- returning the live subscription for the current HTTP/SSE response,
- ensuring the returned subscription can detach when the HTTP response is closed
  or aborted,
- arranging any backend continuation after detach that the host app supports,
- cancelling an explicit run id,
- enforcing scope and ownership using `TContext`,
- persisting or exposing coarse running, completed, failed, cancelled, and
  expired state,
- returning a clear structured not-attachable outcome when a live stream cannot
  be attached or resumed.

It should not be responsible for:

- ChatKit request parsing,
- ChatKit item JSON shape,
- Store or AttachmentStore persistence contracts,
- stream event validation,
- package-level serialization,
- every-transient-event replay.

The coordinator may keep in-process subscriber lists inside the host app if that
app chooses, but the package must not provide that implementation as the default
or correctness model.

## Disconnect Behavior

There is only one supported package behavior:

- HTTP/SSE close, fetch abort, browser refresh, mobile sleep, and network swap
  detach the live subscriber.
- Detach does not call `RunCoordinator.cancelRun(...)`.
- The active run may continue only if the host coordinator has arranged an
  execution strategy that survives the subscriber detach for the deployment
  target.
- Final conversation state remains recoverable through Store persistence and
  normal ChatKit item/thread fetches.
- Coarse run state remains recoverable through the host app's run persistence.

The package should not expose a `disconnectBehavior` option. Supporting multiple
runtime behaviors here would make app correctness depend on configuration
instead of a single clear contract.

## Cancellation Behavior

Explicit cancellation remains a first-class capability, but it must be distinct
from transport disconnect.

The package should keep cancellation as a coordinator primitive rather than
exposing a standalone HTTP helper. Applications that need explicit cancellation
should accept an app-owned control/action with an explicit run id, validate
authorization in the app context, call
`RunCoordinator.cancelRun({ runId, context })`, and map the structured
`CancelRunResult` to their own UI or route response.

Before finalizing whether `createChatKitHandler` itself maps a ChatKit JS stop
gesture to cancellation, run a protocol spike against ChatKit JS:

- Start a test backend that records requests, custom actions, stream options,
  stream aborts, and response lifecycle events.
- Trigger the visible ChatKit stop button during an active stream.
- Refresh the browser during an active stream.
- Record whether ChatKit JS sends a distinct cancel request, only aborts the
  fetch/stream, or performs another protocol action.
- Verify whether any cancel request carries a stable run id or token.

If ChatKit JS sends a distinct and stable cancel action, map it to
`RunCoordinator.cancelRun(...)`.

If ChatKit JS only aborts the transport, treat that as detach. Do not advertise
backend cancellation through `stream_options.allow_cancel` solely because the
transport can be aborted. The application can still expose cancellation through
its own explicit route, action, or control.

## Attach Behavior

The package should keep attach as a coordinator primitive rather than exposing a
standalone HTTP helper. Applications that need live resume should handle a
streaming custom action, validate its explicit `run_id`, call
`RunCoordinator.attachRun({ runId, context })`, and yield the returned
subscription events from `ChatKitServer.action(...)` when the outcome is
`attached`.

Structured not-attachable outcomes should map to clear responses:

- `not_found`: 404 or a non-retryable stream/API error.
- `forbidden`: 403 or a non-retryable stream/API error.
- `finished`: 409 or a non-retryable stream/API error that tells the client to
  recover through normal thread/item fetch.
- `expired`: 410 or a non-retryable stream/API error that tells the client to
  recover through normal thread/item fetch.
- `unavailable`: 503 with optional `retryAfterMs`.

Normal `items.list`, `threads.list`, and ChatKit JS `fetchUpdates()` remain the
primary recovery path for completed state. Live attach is a best-effort
optimization for coordinators that support it.

## Handler Behavior

The handler remains responsible for:

- validating ChatKit requests,
- constructing context with `getContext` when supplied,
- building the `RunStartDescriptor`,
- calling the ChatKit server through the package-owned stream `source`,
- passing stream lifecycle boundaries to `runCoordinator`,
- serializing stream events,
- preserving Store and AttachmentStore behavior,
- surfacing structured errors.

Expected failure mode:

- TypeScript requires `runCoordinator` for normal use.
- Runtime also guards against a missing coordinator and returns a clear
  configuration error for streaming requests.
- `not_started` and `not_attachable` outcomes become structured API/SSE errors
  instead of silent closed streams.

Expected successful flow:

1. Handler receives a streaming ChatKit request.
2. Handler builds context.
3. Handler builds a package-owned `RunStartDescriptor`.
4. Handler asks `runCoordinator.startRun(...)` to start the response run.
5. Handler streams live events from the returned subscription to the current
   HTTP/SSE subscriber.
6. If the subscriber disconnects, handler calls `subscription.detach(...)` only.
7. Coordinator records completion, failure, cancellation, expiration, or
   not-attachable state according to the host app implementation.

## Tests And Verification

Package-level tests should cover:

- Type tests showing `createChatKitHandler(..., { runCoordinator })` is required.
- Type tests showing the old `runManager` and `disconnectBehavior` options are
  rejected in the desired end state.
- Runtime guard for missing coordinator if an invalid JavaScript caller bypasses
  TypeScript.
- Streaming response exposes `x-chatkit-run-id`.
- Normal streaming request calls `runCoordinator.startRun(...)` and does not
  infer attach from `thread_id`.
- Disconnect calls `RunSubscription.detach(...)` and does not call
  `RunCoordinator.cancelRun(...)`.
- App-owned explicit cancel actions call `RunCoordinator.cancelRun(...)`.
- Unknown/already-finished/forbidden cancel outcomes remain structured at the
  coordinator boundary.
- Streaming custom action attach calls `RunCoordinator.attachRun(...)` only with
  explicit run ids.
- Not-attachable attach outcomes are structured and mapped to clear stream
  errors or normal persisted-state recovery.
- Existing Store and AttachmentStore behavior remains unchanged.
- Protocol spike fixture or test documents ChatKit JS stop/refresh behavior.
- Vercel-facing README docs state that process memory is not durable, streamed
  runs remain subject to max duration, and durable continuation requires
  app-owned infrastructure.

Local verification should use the package repo's npm scripts and Node.js
`>=24.15.0`.

## Acceptance Criteria

- `createChatKitHandler` requires `runCoordinator`.
- No default in-memory coordinator is created by the package.
- `disconnectBehavior` is removed.
- Disconnect means subscription detach, never implicit backend cancellation.
- `RunCoordinator` has narrow typed start, attach, and cancel methods, with
  subscriber detach represented by `RunSubscription`.
- Attach and cancel failure outcomes are structured, not stringly typed.
- Streaming responses expose a stable run id header unless a verified ChatKit JS
  protocol provides a better stable token and the header remains available for
  application-owned routes.
- Explicit cancellation works through an app-owned route, action, or control
  that calls `runCoordinator.cancelRun(...)`.
- The package does not export an in-memory fallback implementation.
- The package does not add package-owned persistence tables, clients, queues, or
  Vercel Workflow code.
- Tests document the ChatKit JS stop/refresh protocol behavior used by the
  implementation.
- README docs explain the Vercel hosting contract and the limits of
  response-detached work.
- The released package can be consumed by ePCR Holo without a local `file:`
  dependency.
