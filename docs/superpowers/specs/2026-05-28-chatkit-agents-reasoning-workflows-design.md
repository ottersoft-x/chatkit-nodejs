# ChatKit Bun Agents Reasoning Workflows Design

## Purpose

This milestone extends the Agents stream bridge with reasoning workflow
conversion. The current bridge handles assistant text, citation annotations,
client tool calls, and generated images. The core ChatKit schemas and server
persistence path already support `workflow` items and `workflow.task.*` updates;
this slice wires those existing shapes to Responses reasoning summary stream
events.

The goal is parity with the focused upstream Python `chatkit.agents` workflow
tests while keeping custom workflow helper APIs, cross-turn workflow resume,
guardrail rollback, widgets, and input replay in later milestones.

## Scope

This milestone includes:

- Creating a reasoning `workflow` item when a Responses reasoning output item is
  added.
- Converting reasoning summary deltas and done events into thought task add and
  update events.
- Tracking the active workflow item on `AgentContext` so tests and future helper
  methods share the same state boundary.
- Automatically ending an active workflow before the next assistant message item
  is added.
- Preserving an existing workflow summary when auto-ending; otherwise adding a
  duration summary and collapsing the workflow.
- Focused parity tests mirroring the upstream Python workflow tests.

This milestone defers:

- Public `AgentContext` workflow helpers such as `startWorkflow(...)`,
  `addWorkflowTask(...)`, `updateWorkflowTask(...)`, and `endWorkflow(...)`.
- Loading a prior open workflow from thread history before the stream starts.
- Persisting an open workflow at the end of a stream when no done event is
  emitted.
- Auto-ending workflows around context-enqueued visual items.
- Guardrail rollback and produced-item removal.
- Workflow or generated-image input conversion and replay.
- Widget streaming helpers.

## Architecture

Keep the conversion inside the existing Agents bridge:

```text
src/agents/context.ts
src/agents/stream.ts
tests/agents.test.ts
```

`AgentContext` should own the mutable active workflow item, just as the Python
reference stores `workflow_item` on its context object. This slice only exposes
the state needed by stream conversion and tests; helper methods remain deferred
so the public API does not grow before the stream behavior is proven.

`stream.ts` remains responsible for raw SDK event unwrapping, event ordering,
active stream state, and outgoing `ThreadStreamEvent` validation. It should add
a small reasoning-workflow state object for tracking a single in-flight
streaming thought by SDK reasoning item id and summary index.

The existing type and server modules should not need schema changes. Workflow
items, thought tasks, `workflow.task.added`, and `workflow.task.updated` are
already modeled, and server persistence already merges workflow task updates
into the final done item.

## Agent Context State

Add a public mutable `workflowItem` field to `AgentContext<TContext>`:

```ts
workflowItem: Extract<ThreadItem, { type: "workflow" }> | null = null;
```

This matches the Python context state and keeps the bridge behavior observable in
tests. The field should be updated when the bridge starts a reasoning workflow
and cleared when the workflow is ended.

No public workflow helper methods are added in this milestone. Future helper
methods should build on the same field rather than adding a second state source.

## Stream Event Conversion

Handle these raw Responses events through the existing `rawResponseData(...)`
path, including direct `raw_response_event`, `raw_model_stream_event`, and nested
provider model events:

```text
response.output_item.added
response.reasoning_summary_text.delta
response.reasoning_summary_text.done
```

When `response.output_item.added` contains `item.type === "reasoning"`:

- Ignore the event if `context.workflowItem` is already set.
- Generate a workflow item id with
  `context.store.generateItemId("workflow", context.thread, context.context)`.
- Create a `workflow` item with:
  - `workflow.type: "reasoning"`
  - `workflow.tasks: []`
  - `workflow.expanded: false`
- Store it on `context.workflowItem`.
- Yield `thread.item.added` with the new workflow item.

When `response.reasoning_summary_text.delta` arrives:

- Ignore the event if there is no active `context.workflowItem`.
- Use `item_id` and `summary_index` to identify the in-flight thought.
- If the workflow is a reasoning workflow with no tasks yet, append a thought
  task with the delta text and yield `workflow.task.added` at index `0`.
- If the event matches the in-flight thought, append the delta to that task's
  content and yield `workflow.task.updated`.
- Ignore non-matching deltas for this slice. The Python reference only streams
  the first thought early; later thoughts are added when their done event
  arrives.

When `response.reasoning_summary_text.done` arrives:

- Ignore the event if there is no active `context.workflowItem`.
- If the event matches the in-flight thought, replace that thought's content
  with the final text and yield `workflow.task.updated`.
- Otherwise append a new thought task with the final text and yield
  `workflow.task.added` at the new index.
- Clear the in-flight thought state after a matching done event.

When `response.output_item.added` contains `item.type === "message"` and a
workflow is active:

- Yield `thread.item.done` for the active workflow before yielding the assistant
  message added event.
- Preserve an existing `workflow.summary` value.
- If no summary exists, set `workflow.summary` to `{ duration }`, where duration
  is the integer number of seconds between the workflow item's `created_at` and
  `context.createdAt()`.
- Set `workflow.expanded` to `false`.
- Clear `context.workflowItem`.
- Continue yielding the assistant message event exactly as the current bridge
  does today.

## Testing Strategy

Extend `tests/agents.test.ts` with network-free fixtures that mirror the three
upstream Python workflow tests:

- A reasoning item followed by summary deltas and done events creates a workflow
  item, streams the first thought through add/update events, and adds a later
  thought on its done event.
- A reasoning workflow is ended before the next assistant message is added, with
  a duration summary and `expanded: false`.
- An existing workflow summary is preserved when auto-ending a workflow before an
  assistant message.

Add focused coverage for nested provider/model raw event wrappers if the primary
tests do not already exercise that path.

Existing assistant text, annotations, client tool, generated image, merge
fairness, cancellation, server, and export tests must remain green.

Full verification for the implementation plan should include:

```bash
bun run verify
```

## Acceptance Criteria

The milestone is complete when:

- `streamAgentResponse(...)` emits validated workflow item and workflow task
  events for Responses reasoning summary streams.
- The first reasoning thought streams incrementally, and later thoughts are added
  on their done events.
- Active reasoning workflows are ended before assistant message items are added.
- Auto-ended workflows preserve custom summaries or receive a duration summary
  when no summary exists.
- `AgentContext.workflowItem` reflects active workflow state and is cleared when
  the workflow is ended.
- The implementation remains limited to reasoning workflow stream conversion and
  does not add public workflow helpers, cross-turn resume, open-workflow
  persistence, guardrails, widgets, or input replay.
- `bun run verify` passes.
