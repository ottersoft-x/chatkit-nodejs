# ChatKit Agents Reasoning Workflows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reasoning workflow stream conversion to the Agents bridge, including reasoning workflow items, thought task updates, and automatic workflow completion before assistant messages.

**Architecture:** Keep the work inside the existing `src/agents/` bridge. `AgentContext` owns the active workflow item, while `src/agents/stream.ts` unwraps Responses events and mutates that workflow state into existing `workflow.task.*` ChatKit stream events.

**Tech Stack:** Bun, TypeScript, Zod-validated ChatKit schemas, `bun:test`, existing fixture-shaped `@openai/agents` streams.

---

## Scope Check

This plan implements only the approved reasoning workflow stream slice:

- `AgentContext.workflowItem` state.
- `response.output_item.added` for reasoning items.
- `response.reasoning_summary_text.delta` and `.done` thought task conversion.
- Automatic active workflow completion before `response.output_item.added` assistant message events.
- Focused parity tests for the upstream Python workflow tests.

It does not add public workflow helper methods, cross-turn workflow resume, open-workflow persistence at stream end, context-event visual auto-end behavior, guardrail rollback, widgets, or input replay.

## File Structure

- Modify: `src/agents/context.ts`
  - Adds the active `workflowItem` state field.
- Modify: `src/agents/stream.ts`
  - Adds workflow item construction, streaming thought tracking, reasoning summary conversion, and workflow auto-end before assistant messages.
- Modify: `tests/agents.test.ts`
  - Adds AgentContext state coverage and reasoning workflow stream parity tests.

No schema or server changes are expected because workflow items and workflow task updates already exist.

## Task 1: Add AgentContext Workflow State

**Files:**
- Modify: `src/agents/context.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write the failing AgentContext workflow state test**

In `tests/agents.test.ts`, add this test inside `describe("AgentContext", () => {` after the deterministic timestamp test:

```ts
  test("tracks the active workflow item for stream conversion", () => {
    const agentContext = createContext();
    const workflow: Extract<ThreadItem, { type: "workflow" }> = {
      id: "workflow_generated",
      thread_id: thread.id,
      created_at: now,
      type: "workflow",
      workflow: {
        type: "reasoning",
        tasks: [],
        expanded: false,
      },
    };

    expect(agentContext.workflowItem).toBeNull();

    agentContext.workflowItem = workflow;
    expect(agentContext.workflowItem).toBe(workflow);

    agentContext.workflowItem = null;
    expect(agentContext.workflowItem).toBeNull();
  });
```

- [ ] **Step 2: Run focused tests to verify the new test fails**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because `AgentContext` does not define `workflowItem`.

- [ ] **Step 3: Run typecheck to verify the public field is missing**

Run:

```bash
bun run typecheck
```

Expected: FAIL because `workflowItem` is not part of `AgentContext`.

- [ ] **Step 4: Add the workflow item type and field**

Modify `src/agents/context.ts` imports:

```ts
import type { ThreadItem } from "../types/core";
import { ThreadStreamEventSchema, type ThreadStreamEvent } from "../types/server";
import type { AgentContextOptions, JsonObject } from "./types";
```

Add this type alias after `ClientToolCall`:

```ts
type WorkflowItem = Extract<ThreadItem, { type: "workflow" }>;
```

Add this field near the other `AgentContext` fields:

```ts
  workflowItem: WorkflowItem | null = null;
```

The class field section should read:

```ts
export class AgentContext<TContext> {
  readonly thread: AgentContextOptions<TContext>["thread"];
  readonly store: AgentContextOptions<TContext>["store"];
  readonly context: TContext;
  workflowItem: WorkflowItem | null = null;
  private readonly now: () => Date | string;
  private readonly queue = new AsyncEventQueue<ThreadStreamEvent>();
  private clientToolCall: ClientToolCall | null = null;
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 6: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 7: Commit AgentContext workflow state**

Run:

```bash
git add src/agents/context.ts tests/agents.test.ts
git commit -m "Track active Agents workflow state"
```

Expected: Commit succeeds.

## Task 2: Convert Reasoning Summary Thought Tasks

**Files:**
- Modify: `src/agents/stream.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write failing reasoning workflow stream tests**

Add these tests inside `describe("streamAgentResponse", () => {` after the provider response wrapper test and before the merge fairness tests:

```ts
  test("maps reasoning summary streams into workflow thought tasks", async () => {
    const agentContext = createContext();
    const events = await collect(
      streamAgentResponse(
        agentContext,
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: { type: "reasoning", id: "resp_1", summary: [] },
          }),
          rawResponse({
            type: "response.reasoning_summary_text.delta",
            item_id: "resp_1",
            summary_index: 0,
            delta: "Think",
          }),
          rawResponse({
            type: "response.reasoning_summary_text.delta",
            item_id: "resp_1",
            summary_index: 0,
            delta: "ing 1",
          }),
          rawResponse({
            type: "response.reasoning_summary_text.done",
            item_id: "resp_1",
            summary_index: 0,
            text: "Thinking 1",
          }),
          rawResponse({
            type: "response.reasoning_summary_text.delta",
            item_id: "resp_1",
            summary_index: 1,
            delta: "Think",
          }),
          rawResponse({
            type: "response.reasoning_summary_text.delta",
            item_id: "resp_1",
            summary_index: 1,
            delta: "ing 2",
          }),
          rawResponse({
            type: "response.reasoning_summary_text.done",
            item_id: "resp_1",
            summary_index: 1,
            text: "Thinking 2",
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "workflow_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "workflow",
          workflow: {
            type: "reasoning",
            tasks: [],
            expanded: false,
          },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "workflow_generated",
        update: {
          type: "workflow.task.added",
          task_index: 0,
          task: { type: "thought", content: "Think", status_indicator: "none" },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "workflow_generated",
        update: {
          type: "workflow.task.updated",
          task_index: 0,
          task: { type: "thought", content: "Thinking 1", status_indicator: "none" },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "workflow_generated",
        update: {
          type: "workflow.task.updated",
          task_index: 0,
          task: { type: "thought", content: "Thinking 1", status_indicator: "none" },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "workflow_generated",
        update: {
          type: "workflow.task.added",
          task_index: 1,
          task: { type: "thought", content: "Thinking 2", status_indicator: "none" },
        },
      },
    ]);
    expect(agentContext.workflowItem?.workflow.tasks).toEqual([
      { type: "thought", content: "Thinking 1", status_indicator: "none" },
      { type: "thought", content: "Thinking 2", status_indicator: "none" },
    ]);
  });

  test("maps nested provider reasoning events through the raw model path", async () => {
    const agentContext = createContext();
    const events = await collect(
      streamAgentResponse(
        agentContext,
        streamedRun([
          rawModel({
            type: "model",
            event: {
              type: "response.output_item.added",
              item: { type: "reasoning", id: "resp_1", summary: [] },
            },
          }),
          rawModel({
            type: "model",
            event: {
              type: "response.reasoning_summary_text.done",
              item_id: "resp_1",
              summary_index: 0,
              text: "Nested thought",
            },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "workflow_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "workflow",
          workflow: { type: "reasoning", tasks: [], expanded: false },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "workflow_generated",
        update: {
          type: "workflow.task.added",
          task_index: 0,
          task: { type: "thought", content: "Nested thought", status_indicator: "none" },
        },
      },
    ]);
  });
```

- [ ] **Step 2: Run agents tests to verify they fail**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because `streamAgentResponse(...)` ignores reasoning output items and reasoning summary events.

- [ ] **Step 3: Add workflow and thought tracking types**

Modify the top of `src/agents/stream.ts` after `type GeneratedImageItem`:

```ts
type WorkflowItem = Extract<ThreadItem, { type: "workflow" }>;
type ThoughtTask = Extract<WorkflowItem["workflow"]["tasks"][number], { type: "thought" }>;
```

Add this interface near `GeneratedImageState`:

```ts
interface StreamingThoughtState {
  itemId: string | null;
  summaryIndex: number;
  task: ThoughtTask;
}
```

Add this field to `AssistantTextState`:

```ts
  streamingThought: StreamingThoughtState | null;
```

Initialize it in `streamAgentResponse(...)` state:

```ts
  const state: AssistantTextState = {
    activeItemId: null,
    textByPart: new Map(),
    annotationCountByPart: new Map(),
    generatedImage: null,
    streamingThought: null,
  };
```

- [ ] **Step 4: Add workflow helper functions**

Add these helpers after `generatedImageItem(...)`:

```ts
function workflowItem<TContext>(context: AgentContext<TContext>, itemId: string): WorkflowItem {
  return {
    id: itemId,
    thread_id: context.thread.id,
    created_at: context.createdAt(),
    type: "workflow",
    workflow: {
      type: "reasoning",
      tasks: [],
      expanded: false,
    },
  };
}

function thoughtTask(content: string): ThoughtTask {
  return {
    type: "thought",
    content,
    status_indicator: "none",
  };
}

function matchingStreamingThought(
  state: AssistantTextState,
  itemId: string | null,
  summaryIndex: number,
): StreamingThoughtState | null {
  const streamingThought = state.streamingThought;

  if (
    streamingThought &&
    streamingThought.itemId === itemId &&
    streamingThought.summaryIndex === summaryIndex
  ) {
    return streamingThought;
  }

  return null;
}
```

- [ ] **Step 5: Convert reasoning output item added events**

In `src/agents/stream.ts`, inside the `response.output_item.added` case, add this branch after `if (!item) { return []; }` and before the image generation branch:

```ts
      if (item.type === "reasoning") {
        if (context.workflowItem) {
          return [];
        }

        const itemId = context.store.generateItemId("workflow", context.thread, context.context);
        const workflow = workflowItem(context, itemId);
        context.workflowItem = workflow;

        return [
          {
            type: "thread.item.added",
            item: workflow,
          },
        ];
      }
```

- [ ] **Step 6: Convert reasoning summary delta events**

Add this switch case before `response.image_generation_call.partial_image`:

```ts
    case "response.reasoning_summary_text.delta": {
      const workflow = context.workflowItem;
      const itemId = stringValue(rawData.item_id);
      const summaryIndex = numberValue(rawData.summary_index);
      const delta = stringValue(rawData.delta) ?? "";

      if (!workflow || summaryIndex === null) {
        return [];
      }

      if (workflow.workflow.type === "reasoning" && workflow.workflow.tasks.length === 0) {
        const task = thoughtTask(delta);
        state.streamingThought = { itemId, summaryIndex, task };
        workflow.workflow.tasks.push(task);

        return [
          {
            type: "thread.item.updated",
            item_id: workflow.id,
            update: {
              type: "workflow.task.added",
              task_index: 0,
              task,
            },
          },
        ];
      }

      const streamingThought = matchingStreamingThought(state, itemId, summaryIndex);

      if (!streamingThought) {
        return [];
      }

      streamingThought.task.content += delta;
      const taskIndex = workflow.workflow.tasks.indexOf(streamingThought.task);

      if (taskIndex < 0) {
        return [];
      }

      return [
        {
          type: "thread.item.updated",
          item_id: workflow.id,
          update: {
            type: "workflow.task.updated",
            task_index: taskIndex,
            task: streamingThought.task,
          },
        },
      ];
    }
```

- [ ] **Step 7: Convert reasoning summary done events**

Add this switch case immediately after the delta case:

```ts
    case "response.reasoning_summary_text.done": {
      const workflow = context.workflowItem;
      const itemId = stringValue(rawData.item_id);
      const summaryIndex = numberValue(rawData.summary_index);
      const text = stringValue(rawData.text) ?? "";

      if (!workflow || summaryIndex === null) {
        return [];
      }

      const streamingThought = matchingStreamingThought(state, itemId, summaryIndex);

      if (streamingThought) {
        streamingThought.task.content = text;
        state.streamingThought = null;
        const taskIndex = workflow.workflow.tasks.indexOf(streamingThought.task);

        if (taskIndex < 0) {
          return [];
        }

        return [
          {
            type: "thread.item.updated",
            item_id: workflow.id,
            update: {
              type: "workflow.task.updated",
              task_index: taskIndex,
              task: streamingThought.task,
            },
          },
        ];
      }

      const task = thoughtTask(text);
      workflow.workflow.tasks.push(task);

      return [
        {
          type: "thread.item.updated",
          item_id: workflow.id,
          update: {
            type: "workflow.task.added",
            task_index: workflow.workflow.tasks.length - 1,
            task,
          },
        },
      ];
    }
```

- [ ] **Step 8: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 9: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 10: Commit reasoning thought conversion**

Run:

```bash
git add src/agents/stream.ts tests/agents.test.ts
git commit -m "Convert Agents reasoning workflow thoughts"
```

Expected: Commit succeeds.

## Task 3: End Active Workflows Before Assistant Messages

**Files:**
- Modify: `src/agents/stream.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write failing workflow auto-end tests**

Add these tests inside `describe("streamAgentResponse", () => {` after the reasoning summary tests from Task 2:

```ts
  test("ends active reasoning workflows before assistant message items", async () => {
    const agentContext = createContext();
    const events = await collect(
      streamAgentResponse(
        agentContext,
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: { type: "reasoning", id: "resp_1", summary: [] },
          }),
          rawResponse({
            type: "response.reasoning_summary_text.done",
            item_id: "resp_1",
            summary_index: 0,
            text: "Thinking 1",
          }),
          rawResponse({
            type: "response.output_item.added",
            item: { type: "message", id: "msg_1" },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "workflow_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "workflow",
          workflow: { type: "reasoning", tasks: [], expanded: false },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "workflow_generated",
        update: {
          type: "workflow.task.added",
          task_index: 0,
          task: { type: "thought", content: "Thinking 1", status_indicator: "none" },
        },
      },
      {
        type: "thread.item.done",
        item: {
          id: "workflow_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "workflow",
          workflow: {
            type: "reasoning",
            tasks: [{ type: "thought", content: "Thinking 1", status_indicator: "none" }],
            summary: { duration: 0 },
            expanded: false,
          },
        },
      },
      {
        type: "thread.item.added",
        item: {
          id: "msg_1",
          thread_id: "thr_1",
          created_at: now,
          type: "assistant_message",
          content: [],
        },
      },
    ]);
    expect(agentContext.workflowItem).toBeNull();
  });

  test("preserves existing workflow summaries when auto-ending before assistant messages", async () => {
    const agentContext = createContext();
    agentContext.workflowItem = {
      id: "wf_existing",
      thread_id: thread.id,
      created_at: now,
      type: "workflow",
      workflow: {
        type: "custom",
        tasks: [],
        summary: { title: "Test" },
        expanded: true,
      },
    };

    const events = await collect(
      streamAgentResponse(
        agentContext,
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: { type: "message", id: "msg_1" },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.done",
        item: {
          id: "wf_existing",
          thread_id: "thr_1",
          created_at: now,
          type: "workflow",
          workflow: {
            type: "custom",
            tasks: [],
            summary: { title: "Test" },
            expanded: false,
          },
        },
      },
      {
        type: "thread.item.added",
        item: {
          id: "msg_1",
          thread_id: "thr_1",
          created_at: now,
          type: "assistant_message",
          content: [],
        },
      },
    ]);
    expect(agentContext.workflowItem).toBeNull();
  });
```

- [ ] **Step 2: Run agents tests to verify they fail**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because active workflows are not ended before assistant message added events.

- [ ] **Step 3: Add workflow duration and auto-end helpers**

In `src/agents/stream.ts`, add these helpers after `matchingStreamingThought(...)`:

```ts
function durationSeconds(startedAt: string, endedAt: string): number {
  const started = Date.parse(startedAt);
  const ended = Date.parse(endedAt);

  if (!Number.isFinite(started) || !Number.isFinite(ended)) {
    return 0;
  }

  return Math.max(0, Math.floor((ended - started) / 1000));
}

function endActiveWorkflow<TContext>(
  context: AgentContext<TContext>,
): Extract<ThreadStreamEvent, { type: "thread.item.done" }> | null {
  const workflow = context.workflowItem;

  if (!workflow) {
    return null;
  }

  const endedAt = context.createdAt();
  const summary =
    workflow.workflow.summary ?? { duration: durationSeconds(workflow.created_at, endedAt) };
  const doneItem: WorkflowItem = {
    ...workflow,
    workflow: {
      ...workflow.workflow,
      summary,
      expanded: false,
    },
  };
  context.workflowItem = null;

  return {
    type: "thread.item.done",
    item: doneItem,
  };
}
```

- [ ] **Step 4: End workflows before assistant message added events**

In the `response.output_item.added` case, replace the current message branch:

```ts
      const itemId =
        stringValue(item.id) ?? context.store.generateItemId("message", context.thread, context.context);
      state.activeItemId = itemId;

      return [
        {
          type: "thread.item.added",
          item: assistantItem(context, itemId, []),
        },
      ];
```

with:

```ts
      const itemId =
        stringValue(item.id) ?? context.store.generateItemId("message", context.thread, context.context);
      state.activeItemId = itemId;
      const events: ThreadStreamEvent[] = [];
      const workflowDone = endActiveWorkflow(context);

      if (workflowDone) {
        events.push(workflowDone);
      }

      events.push({
        type: "thread.item.added",
        item: assistantItem(context, itemId, []),
      });

      return events;
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 6: Run full verification**

Run:

```bash
bun run verify
```

Expected: PASS.

- [ ] **Step 7: Commit workflow auto-end behavior**

Run:

```bash
git add src/agents/stream.ts tests/agents.test.ts
git commit -m "End Agents workflows before messages"
```

Expected: Commit succeeds.

## Final Verification

After all tasks complete, run:

```bash
bun run verify
git status --short --branch
```

Expected:

- TypeScript typecheck passes.
- All Bun tests pass.
- Git status is clean on the implementation branch.

## Implementation Notes

- Keep the milestone limited to reasoning workflow stream conversion.
- Do not add `AgentContext` workflow helper methods yet.
- Do not read thread history to resume prior workflows in this milestone.
- Do not persist open workflows at SDK stream end in this milestone.
- Use existing `rawResponseData(...)` so direct raw response events and nested provider model events share the same conversion path.
- Continue validating all emitted events through `ThreadStreamEventSchema.parse(...)`.
- Preserve existing assistant text, annotations, client tool, generated image, context merge, cancellation, and server behavior.
