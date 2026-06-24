# ChatKit JS stop and refresh protocol

Status: unverified. This document records the conservative server contract until
a browser protocol spike proves that ChatKit JS sends a distinct, stable stop
request.

## Current server contract

Refresh, tab close, mobile sleep, network changes, and generic fetch aborts
detach the current SSE subscriber. They do not cancel the backend run.

Explicit backend cancellation must call
`RunCoordinator.cancelRun({ runId, context })`. Applications can expose that
through `createChatKitRunCancelHandler(...)` or through their own authenticated
route/control. The run id should come from the `x-chatkit-run-id` response
header or another app-owned durable run record.

Until ChatKit JS is verified to send a separate cancel request, server code must
not infer backend cancellation from a closed stream alone. Subscriber detach and
run cancellation stay separate operations.

## Protocol spike procedure

1. Start a test backend that logs request method, path, headers, body, response
   headers, stream lifecycle events, and calls to cancel or attach routes.
2. Start a long-running ChatKit response and record the emitted
   `x-chatkit-run-id`.
3. Click the visible ChatKit stop button while the stream is active. Save the
   browser network log and backend logs.
4. Start the same long-running response again, then refresh the page while the
   stream is active. Save the same logs.
5. Repeat with tab close, offline/online transition, and an explicit client-side
   fetch abort if the app exposes one.
6. Compare stop-button traffic with refresh traffic:
   - Does stop send a request that refresh does not?
   - Does that request carry a stable run id or token?
   - Is the request authenticated and scoped to the same app user or tenant?
   - Does refresh only close the stream?
7. If stop sends a distinct authenticated request with a stable run id, map that
   request to `RunCoordinator.cancelRun(...)`.
8. If stop only closes the active stream, keep backend cancellation behind the
   explicit app-owned cancel route/control and continue treating stream close as
   subscriber detach.
