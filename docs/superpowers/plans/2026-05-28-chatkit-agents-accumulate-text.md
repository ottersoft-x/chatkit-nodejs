# ChatKit Agents Accumulate Text Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add strict Python parity for the Agents `accumulate_text` helper as Bun's exported `accumulateText(...)`.

**Architecture:** Add a focused Agents helper that transforms raw Agents text delta events into cumulative `Text` or `Markdown` component values. Keep it separate from `streamAgentResponse(...)`; composition with widget streaming is proven by wrapping accumulated components in a `Card` root for the existing `streamWidget(...)` path.

**Tech Stack:** Bun, TypeScript, Bun test runner, existing ChatKit widget helpers, existing parity matrix JSON.

---

## File Structure

- Create `src/agents/accumulate.ts`: strict upstream parity implementation for `accumulateText(...)`.
- Create `tests/agents-accumulate.test.ts`: direct parity test plus widget-stream composition test.
- Modify `src/agents/index.ts`: export `accumulateText` and its public types.
- Modify `src/widgets/components.ts`: narrow `Text(...)` and `Markdown(...)` return types so callers can pass those component objects to `accumulateText(...)` without casts.
- Modify `docs/parity/matrix.json`: mark `agents-accumulate-text` as covered and cite the new source/test.
- Modify `tests/parity-smoke.test.ts`: remove `agents-accumulate-text` from the known deferred rows assertion.

Do not change `src/agents/stream.ts`, server code, HTTP/SSE code, widget serialization, or `diffWidget(...)`.

## Task 1: Add Red Tests For `accumulateText`

**Files:**
- Create: `tests/agents-accumulate.test.ts`

- [ ] **Step 1: Create the focused test file with failing tests**

```ts
import { describe, expect, test } from "bun:test";

import { accumulateText } from "../src/agents";
import { Card, Text, streamWidget } from "../src/widgets";

const testThread = {
  id: "thr_accumulate",
  created_at: "2026-05-28T00:00:00.000Z",
  status: { type: "active" as const },
  metadata: {},
};

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) values.push(value);
  return values;
}

async function* streamFrom(events: unknown[]): AsyncIterable<unknown> {
  for (const event of events) yield event;
}

function rawTextDelta(delta: string): unknown {
  return {
    type: "raw_response_event",
    data: {
      type: "response.output_text.delta",
      delta,
    },
  };
}

describe("accumulateText", () => {
  test("yields initial text, cumulative deltas, and final non-streaming text", async () => {
    const baseText = Text({ key: "text", value: "", streaming: true });

    const widgets = await collect(
      accumulateText(streamFrom([rawTextDelta("Hello"), rawTextDelta(", world!")]), baseText),
    );

    expect(widgets).toEqual([
      { type: "Text", key: "text", value: "", streaming: true },
      { type: "Text", key: "text", value: "Hello", streaming: true },
      { type: "Text", key: "text", value: "Hello, world!", streaming: true },
      { type: "Text", key: "text", value: "Hello, world!", streaming: false },
    ]);
    expect(widgets[0]).toBe(baseText);
    expect(widgets[1]).not.toBe(baseText);
    expect(baseText).toEqual({ type: "Text", key: "text", value: "", streaming: true });
  });

  test("composes accumulated text components with widget streaming roots", async () => {
    async function* accumulatedCards() {
      for await (const text of accumulateText(
        streamFrom([rawTextDelta("Hello"), rawTextDelta(", world!")]),
        Text({ id: "text", value: "", streaming: true }),
      )) {
        yield Card({ children: [text] });
      }
    }

    const events = await collect(
      streamWidget(testThread, accumulatedCards(), {
        generateId: () => "msg_accumulate",
        now: () => "2026-05-28T00:00:01.000Z",
      }),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "msg_accumulate",
          type: "widget",
          thread_id: "thr_accumulate",
          created_at: "2026-05-28T00:00:01.000Z",
          widget: {
            type: "Card",
            children: [{ type: "Text", id: "text", value: "", streaming: true }],
          },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "msg_accumulate",
        update: {
          type: "widget.streaming_text.value_delta",
          component_id: "text",
          delta: "Hello",
          done: false,
        },
      },
      {
        type: "thread.item.updated",
        item_id: "msg_accumulate",
        update: {
          type: "widget.streaming_text.value_delta",
          component_id: "text",
          delta: ", world!",
          done: false,
        },
      },
      {
        type: "thread.item.updated",
        item_id: "msg_accumulate",
        update: {
          type: "widget.root.updated",
          widget: {
            type: "Card",
            children: [{ type: "Text", id: "text", value: "Hello, world!", streaming: false }],
          },
        },
      },
      {
        type: "thread.item.done",
        item: {
          id: "msg_accumulate",
          type: "widget",
          thread_id: "thr_accumulate",
          created_at: "2026-05-28T00:00:01.000Z",
          widget: {
            type: "Card",
            children: [{ type: "Text", id: "text", value: "Hello, world!", streaming: false }],
          },
        },
      },
    ]);
  });
});
```

- [ ] **Step 2: Run the red tests**

Run:

```bash
bun test tests/agents-accumulate.test.ts
```

Expected: fail because `../src/agents` does not export `accumulateText`.

## Task 2: Implement The Minimal Helper

**Files:**
- Create: `src/agents/accumulate.ts`
- Modify: `src/agents/index.ts`
- Modify: `src/widgets/components.ts`
- Test: `tests/agents-accumulate.test.ts`

- [ ] **Step 1: Narrow `Text` and `Markdown` return types**

In `src/widgets/components.ts`, replace the current `TextProps` and `MarkdownProps` block with:

```ts
export type TextProps = WidgetProps & { value: string; streaming?: boolean };
export type TextWidget = { type: "Text" } & TextProps;
export function Text(props: TextProps): TextWidget {
  return component("Text", props);
}

export type MarkdownProps = WidgetProps & { value: string; streaming?: boolean };
export type MarkdownWidget = { type: "Markdown" } & MarkdownProps;
export function Markdown(props: MarkdownProps): MarkdownWidget {
  return component("Markdown", props);
}
```

- [ ] **Step 2: Add the helper implementation**

Create `src/agents/accumulate.ts`:

```ts
import type { MarkdownWidget, TextWidget } from "../widgets";

type AccumulatableTextWidget = TextWidget | MarkdownWidget;
type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null;
}

function outputTextDelta(event: unknown): string | null {
  if (!isRecord(event) || event.type !== "raw_response_event" || !isRecord(event.data)) {
    return null;
  }

  if (event.data.type !== "response.output_text.delta") {
    return null;
  }

  return typeof event.data.delta === "string" ? event.data.delta : null;
}

export type { AccumulatableTextWidget };

export async function* accumulateText<TWidget extends AccumulatableTextWidget>(
  events: AsyncIterable<unknown>,
  baseWidget: TWidget,
): AsyncGenerator<TWidget, void, unknown> {
  let text = "";

  yield baseWidget;

  for await (const event of events) {
    const delta = outputTextDelta(event);
    if (delta === null) {
      continue;
    }

    text += delta;
    yield { ...baseWidget, value: text } as TWidget;
  }

  yield { ...baseWidget, value: text, streaming: false } as TWidget;
}
```

- [ ] **Step 3: Export the helper**

In `src/agents/index.ts`, add:

```ts
export { accumulateText } from "./accumulate";
export type { AccumulatableTextWidget } from "./accumulate";
```

- [ ] **Step 4: Run the focused tests**

Run:

```bash
bun test tests/agents-accumulate.test.ts
```

Expected: pass.

- [ ] **Step 5: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: pass. If it fails because existing code depended on `Text(...)` returning the wider `DynamicWidgetComponent`, keep the runtime behavior unchanged and adjust only the return type so `TextWidget` remains assignable to `DynamicWidgetComponent`.

## Task 3: Update The Parity Matrix

**Files:**
- Modify: `docs/parity/matrix.json`
- Modify: `tests/parity-smoke.test.ts`
- Test: `tests/parity-smoke.test.ts`

- [ ] **Step 1: Update the matrix row**

In `docs/parity/matrix.json`, replace the `agents-accumulate-text` row with:

```json
{
  "id": "agents-accumulate-text",
  "area": "agents-output",
  "status": "covered",
  "upstream": {
    "files": ["packages/chatkit-python/tests/test_agents.py"],
    "tests": ["test_accumulate_text"]
  },
  "bun": {
    "tests": ["tests/agents-accumulate.test.ts"],
    "sources": ["src/agents/accumulate.ts"],
    "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-accumulate-text-design.md"]
  },
  "notes": "Bun exports accumulateText for strict raw response output text delta parity and covers composition with widget streaming roots."
}
```

- [ ] **Step 2: Remove the row from deferred smoke expectations**

In `tests/parity-smoke.test.ts`, remove this assertion from the known deferred gaps test:

```ts
expect(deferredIds).toContain("agents-accumulate-text");
```

- [ ] **Step 3: Run the parity smoke test**

Run:

```bash
bun test tests/parity-smoke.test.ts
```

Expected: pass.

## Task 4: Final Verification

**Files:**
- Verify all changed files.

- [ ] **Step 1: Run focused verification**

Run:

```bash
bun test tests/agents-accumulate.test.ts tests/parity-smoke.test.ts
```

Expected: all tests pass.

- [ ] **Step 2: Run full verification**

Run:

```bash
bun run verify
```

Expected: typecheck passes and all Bun tests pass.

- [ ] **Step 3: Run opt-in parity verification**

Run:

```bash
bun run verify:parity
```

Expected: normal verification passes and the parity helper reports the optional upstream Python command without requiring pytest.

- [ ] **Step 4: Check working tree**

Run:

```bash
git status --short --branch
```

Expected: only the planned files are modified or untracked:

```text
## agents-accumulate-text
 M docs/parity/matrix.json
 M src/agents/index.ts
 M src/widgets/components.ts
 M tests/parity-smoke.test.ts
?? docs/superpowers/specs/2026-05-28-chatkit-agents-accumulate-text-design.md
?? docs/superpowers/plans/2026-05-28-chatkit-agents-accumulate-text.md
?? src/agents/accumulate.ts
?? tests/agents-accumulate.test.ts
```

## Optional Commit Checkpoint

Only commit if the user explicitly asks for a commit.

If authorized, use:

```bash
git add docs/parity/matrix.json \
  docs/superpowers/specs/2026-05-28-chatkit-agents-accumulate-text-design.md \
  docs/superpowers/plans/2026-05-28-chatkit-agents-accumulate-text.md \
  src/agents/accumulate.ts \
  src/agents/index.ts \
  src/widgets/components.ts \
  tests/agents-accumulate.test.ts \
  tests/parity-smoke.test.ts
git commit -m "$(cat <<'EOF'
Add agents accumulate text parity

EOF
)"
```

## Self-Review Notes

- Spec coverage: all approved requirements map to tasks: helper/export/type ergonomics in Task 2, direct parity/composition tests in Task 1, matrix update in Task 3, verification in Task 4.
- Placeholder scan: no placeholder instructions remain; every code-changing step includes concrete code or exact edits.
- Type consistency: `TextWidget` and `MarkdownWidget` are defined before `AccumulatableTextWidget`; tests call `accumulateText(...)` through the public `src/agents` export; composition wraps components in `Card` roots before calling `streamWidget(...)`.
