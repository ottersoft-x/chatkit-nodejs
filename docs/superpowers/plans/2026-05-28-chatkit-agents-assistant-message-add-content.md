# ChatKit Agents Assistant Message Add Content Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve supported initial assistant content from `response.output_item.added` in ChatKit `thread.item.added` events.

**Architecture:** Keep the change inside the Agents stream conversion layer. Reuse the existing `assistantContentFromItem(...)` path so initial and final assistant message content are converted with the same rules, then pass that content into `assistantMessageAddedEvents(...)`. Do not change public schemas, final-message conversion, delta/done streaming, guardrails, input replay, persistence, or server APIs.

**Tech Stack:** Bun, TypeScript, `bun:test`, OpenAI Responses raw stream events, existing ChatKit `ThreadStreamEvent` schemas.

---

## Scope Check

This plan implements the approved assistant-message-add content parity spec:

- Preserve supported initial `output_text` content on `response.output_item.added`.
- Preserve initial `output_text` annotations through the existing converter.
- Preserve initial `refusal` content as ChatKit `output_text` with empty annotations.
- Continue skipping unsupported initial content parts.
- Keep streams with no initial content emitting the existing empty assistant message.

This plan does not change ChatKit schemas, stream delta/done behavior, final item conversion, normalized `response_done`, guardrail rollback, input conversion, widget behavior, generated image behavior, or public `ResponseStreamConverter` APIs.

Commit checkpoints appear for review-sized boundaries. Only run commit commands when the operator has explicitly requested commits.

## File Structure

- Modify: `tests/agents.test.ts`
  - Adds focused tests inside the existing `describe("streamAgentResponse", () => { ... })` block near the assistant message event tests.
- Modify: `src/agents/stream.ts`
  - Extends `assistantMessageAddedEvents(...)` to accept converted assistant content.
  - Reuses `assistantContentFromItem(...)` in the `response.output_item.added` message branch.

## Task 1: Add Failing Coverage

**Files:**
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Add tests for initial assistant content**

Insert these tests after the existing `test("maps assistant message text events", async () => { ... })` block in `tests/agents.test.ts`:

```ts
  test("preserves initial assistant content on output item added events", async () => {
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: {
              type: "message",
              id: "msg_initial",
              content: [
                {
                  type: "output_text",
                  text: "Initial text",
                  annotations: [
                    {
                      type: "url_citation",
                      url: "https://example.com/source",
                      title: "Example Source",
                      end_index: 12,
                    },
                  ],
                },
                { type: "refusal", refusal: "I can't help with that." },
              ],
            },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "msg_initial",
          thread_id: "thr_1",
          created_at: now,
          type: "assistant_message",
          content: [
            {
              type: "output_text",
              text: "Initial text",
              annotations: [
                {
                  type: "annotation",
                  source: {
                    type: "url",
                    url: "https://example.com/source",
                    title: "Example Source",
                  },
                  index: 12,
                },
              ],
            },
            { type: "output_text", text: "I can't help with that.", annotations: [] },
          ],
        },
      },
    ]);
  });

  test("skips unsupported initial assistant content parts", async () => {
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: {
              type: "message",
              id: "msg_unsupported_initial",
              content: [
                { type: "reasoning_text", text: "private reasoning" },
                { type: "input_text", text: "not assistant output" },
              ],
            },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "msg_unsupported_initial",
          thread_id: "thr_1",
          created_at: now,
          type: "assistant_message",
          content: [],
        },
      },
    ]);
  });
```

- [ ] **Step 2: Run tests to verify the new coverage fails**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL. The first new test should show `thread.item.added.item.content` as `[]` instead of the expected converted content. The second new test may already pass; the milestone still fails until the first test passes.

## Task 2: Preserve Initial Added Content

**Files:**
- Modify: `src/agents/stream.ts`
- Test: `tests/agents.test.ts`

- [ ] **Step 1: Update `assistantMessageAddedEvents(...)` to accept content**

Replace the current helper:

```ts
function assistantMessageAddedEvents<TContext>(
  context: AgentContext<TContext>,
  state: AssistantTextState,
  itemId: string,
): ThreadStreamEvent[] {
  state.activeItemId = itemId;
  const events: ThreadStreamEvent[] = [];
  const workflowDone = finishWorkflow(context);

  if (workflowDone) {
    events.push(workflowDone);
  }

  events.push({
    type: "thread.item.added",
    item: assistantItem(context, itemId, []),
  });

  return events;
}
```

with:

```ts
function assistantMessageAddedEvents<TContext>(
  context: AgentContext<TContext>,
  state: AssistantTextState,
  itemId: string,
  content: AssistantMessageContent[] = [],
): ThreadStreamEvent[] {
  state.activeItemId = itemId;
  const events: ThreadStreamEvent[] = [];
  const workflowDone = finishWorkflow(context);

  if (workflowDone) {
    events.push(workflowDone);
  }

  events.push({
    type: "thread.item.added",
    item: assistantItem(context, itemId, content),
  });

  return events;
}
```

- [ ] **Step 2: Convert content in the `response.output_item.added` message branch**

Replace this branch body in `convertSdkEvent(...)`:

```ts
      const itemId =
        stringValue(item.id) ?? context.store.generateItemId("message", context.thread, context.context);
      return assistantMessageAddedEvents(context, state, itemId);
```

with:

```ts
      const itemId =
        stringValue(item.id) ?? context.store.generateItemId("message", context.thread, context.context);
      const content = assistantContentFromItem(item, "", converter);
      return assistantMessageAddedEvents(context, state, itemId, content);
```

- [ ] **Step 3: Run the focused Agents tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS. The new initial-content tests and existing assistant message streaming tests should pass.

- [ ] **Step 4: Check TypeScript**

Run:

```bash
bun run typecheck
```

Expected: PASS. If TypeScript reports a type mismatch around `AssistantMessageContent[]`, keep the helper parameter typed as the imported `AssistantMessageContent[]` from `src/agents/stream.ts`.

- [ ] **Step 5: Optional commit checkpoint**

Only if the operator has explicitly requested commits, run:

```bash
git add src/agents/stream.ts tests/agents.test.ts
git commit -m "$(cat <<'EOF'
Preserve initial assistant message content

EOF
)"
```

## Task 3: Regression Verification

**Files:**
- Verify: `src/agents/stream.ts`
- Verify: `tests/agents.test.ts`
- Verify: `docs/superpowers/specs/2026-05-28-chatkit-agents-assistant-message-add-content-design.md`
- Verify: `docs/superpowers/plans/2026-05-28-chatkit-agents-assistant-message-add-content.md`

- [ ] **Step 1: Review the diff for scope**

Run:

```bash
git diff -- src/agents/stream.ts tests/agents.test.ts docs/superpowers/specs/2026-05-28-chatkit-agents-assistant-message-add-content-design.md docs/superpowers/plans/2026-05-28-chatkit-agents-assistant-message-add-content.md
```

Expected: the diff only changes the added-event content path, adds focused tests, and includes the approved design spec. It should not modify final message conversion, refusal delta/done handling, guardrail rollback, input conversion, widgets, or server code.

- [ ] **Step 2: Run full verification**

Run:

```bash
bun run verify
```

Expected: PASS.

- [ ] **Step 3: Check final working tree status**

Run:

```bash
git status --short --branch
```

Expected: only the approved spec, implementation plan, `src/agents/stream.ts`, and `tests/agents.test.ts` are changed unless the operator has explicitly requested commits.

- [ ] **Step 4: Optional final commit checkpoint**

Only if the operator has explicitly requested commits and Task 2 was not already committed, run:

```bash
git add docs/superpowers/specs/2026-05-28-chatkit-agents-assistant-message-add-content-design.md docs/superpowers/plans/2026-05-28-chatkit-agents-assistant-message-add-content.md src/agents/stream.ts tests/agents.test.ts
git commit -m "$(cat <<'EOF'
Preserve initial assistant content in agent streams

EOF
)"
```
