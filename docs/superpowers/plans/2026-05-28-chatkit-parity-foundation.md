# ChatKit Parity Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a tracked parity matrix, sync procedure, local smoke harness, and opt-in parity verification command so future full-parity work is chosen from evidence.

**Architecture:** Keep parity inventory under `docs/parity`, with `upstream.json` remaining the upstream pin and `matrix.json` becoming the row-level coverage map. Add `tests/parity-smoke.test.ts` for matrix validation and thin end-to-end ChatKit JS event-path smoke tests. Add `scripts/verify-parity.ts` and `verify:parity` as opt-in tooling without changing the default `bun run verify` contract.

**Tech Stack:** Bun, TypeScript, `bun:test`, JSON parity fixtures, existing `ChatKitServer`, `SQLiteStore`, `AgentContext`, `streamAgentResponse`, and `createChatKitHandler` APIs.

---

## Scope Check

This plan implements the approved design in `docs/superpowers/specs/2026-05-28-chatkit-parity-foundation-design.md`.

In scope:

- `docs/parity/matrix.json` with explicit row-level parity status.
- `docs/parity/sync.md` with the upstream sync procedure.
- `tests/parity-smoke.test.ts` with matrix validation, Agents-to-server annotation smoke, and HTTP/SSE smoke.
- `scripts/verify-parity.ts` and `package.json` script `verify:parity`.
- `tests/package.test.ts` updated to assert the new script.

Out of scope:

- Implementing `accumulate_text`, `previous_response_id`, entity annotation mapping, input-side annotation replay, attachment content conversion, or new ChatKit stream event schemas.
- Making Python pytest mandatory for `bun run verify`.
- Networked OpenAI calls, browser tests, or production imports from `packages/chatkit-python`.

Commit checkpoints appear for review-sized boundaries. Only run commit commands when the operator has explicitly requested commits.

## File Structure

- Create: `docs/parity/matrix.json`
  - Machine-readable parity inventory tied to `docs/parity/upstream.json`.
- Create: `docs/parity/sync.md`
  - Human procedure for updating the upstream Python pin and parity matrix.
- Create: `tests/parity-smoke.test.ts`
  - Matrix validation plus full-path local smoke tests.
- Create: `scripts/verify-parity.ts`
  - Opt-in helper that reports the pinned upstream reference and Python command.
- Modify: `package.json`
  - Add `verify:parity`.
- Modify: `tests/package.test.ts`
  - Assert `verify:parity` is exposed.

## Task 1: Matrix And Sync Procedure

**Files:**
- Create: `docs/parity/matrix.json`
- Create: `docs/parity/sync.md`
- Create: `tests/parity-smoke.test.ts`

- [ ] **Step 1: Write failing matrix validation tests**

Create `tests/parity-smoke.test.ts` with this initial content:

```ts
import { describe, expect, test } from "bun:test";

import matrix from "../docs/parity/matrix.json";
import upstream from "../docs/parity/upstream.json";

const validStatuses = new Set([
  "covered",
  "partial",
  "intentional-difference",
  "deferred",
  "not-applicable",
]);

const validAreas = new Set([
  "agents-output",
  "agents-input",
  "server",
  "widgets",
  "types",
  "http",
  "metadata",
]);

type ParityRow = {
  id?: unknown;
  area?: unknown;
  status?: unknown;
  upstream?: unknown;
  bun?: {
    tests?: string[];
    sources?: string[];
    docs?: string[];
  };
  notes?: unknown;
};

function expectString(value: unknown, label: string): asserts value is string {
  expect(typeof value, label).toBe("string");
  expect((value as string).length, label).toBeGreaterThan(0);
}

async function expectLocalFilesExist(rowId: string, paths: string[]): Promise<void> {
  for (const path of paths) {
    expect(await Bun.file(path).exists(), `${rowId} references missing local file ${path}`).toBe(true);
  }
}

describe("parity matrix", () => {
  test("matches the pinned upstream metadata", () => {
    expect(matrix.schemaVersion).toBe(1);
    expect(matrix.upstream).toEqual(upstream);
  });

  test("has valid, unique rows with useful references", async () => {
    expect(Array.isArray(matrix.rows)).toBe(true);
    expect(matrix.rows.length).toBeGreaterThan(8);

    const ids = new Set<string>();
    for (const row of matrix.rows as ParityRow[]) {
      expectString(row.id, "row id");
      expect(row.id).toMatch(/^[a-z0-9]+(?:-[a-z0-9]+)*$/);
      expect(ids.has(row.id), `duplicate parity row id ${row.id}`).toBe(false);
      ids.add(row.id);

      expectString(row.area, `${row.id} area`);
      expect(validAreas.has(row.area), `${row.id} has unknown area ${row.area}`).toBe(true);

      expectString(row.status, `${row.id} status`);
      expect(validStatuses.has(row.status), `${row.id} has unknown status ${row.status}`).toBe(true);

      expect(row.upstream, `${row.id} upstream reference`).toBeTruthy();
      expect(row.bun, `${row.id} Bun reference`).toBeTruthy();
      expectString(row.notes, `${row.id} notes`);

      const localReferences = [
        ...(row.bun?.tests ?? []),
        ...(row.bun?.sources ?? []),
        ...(row.bun?.docs ?? []),
      ];
      if (row.status !== "deferred" && row.status !== "not-applicable") {
        expect(localReferences.length, `${row.id} should cite local coverage`).toBeGreaterThan(0);
      }
      await expectLocalFilesExist(row.id, localReferences);
    }
  });

  test("tracks the known deferred full-parity gaps", () => {
    const deferredIds = new Set(
      (matrix.rows as ParityRow[])
        .filter((row) => row.status === "deferred")
        .map((row) => row.id),
    );

    expect(deferredIds).toContain("agents-accumulate-text");
    expect(deferredIds).toContain("agents-previous-response-id");
    expect(deferredIds).toContain("annotations-entity-sources");
    expect(deferredIds).toContain("annotations-input-replay");
    expect(deferredIds).toContain("attachments-content-conversion");
  });
});
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
bun test tests/parity-smoke.test.ts
```

Expected: FAIL because `docs/parity/matrix.json` does not exist.

- [ ] **Step 3: Add the parity matrix**

Create `docs/parity/matrix.json`:

```json
{
  "schemaVersion": 1,
  "upstream": {
    "packageName": "openai-chatkit",
    "version": "1.6.5",
    "submodulePath": "packages/chatkit-python",
    "commit": "dacc133c280b39b9334d06ea73f0f1c199e59927"
  },
  "rows": [
    {
      "id": "upstream-metadata-pin",
      "area": "metadata",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/pyproject.toml"],
        "tests": []
      },
      "bun": {
        "tests": ["tests/parity-metadata.test.ts", "tests/parity-smoke.test.ts"],
        "sources": [],
        "docs": ["docs/parity/upstream.json"]
      },
      "notes": "The pinned Python package name, version, submodule path, and commit are recorded and tested locally."
    },
    {
      "id": "agents-output-streaming",
      "area": "agents-output",
      "status": "partial",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_event_mapping", "test_stream_agent_response_assistant_message_content_types"]
      },
      "bun": {
        "tests": ["tests/agents.test.ts", "tests/parity-smoke.test.ts"],
        "sources": ["src/agents/stream.ts", "src/agents/annotations.ts"],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-parity-foundation-design.md"]
      },
      "notes": "Bun covers the main raw and normalized Agents stream paths, with remaining content-type gaps tracked as separate deferred rows."
    },
    {
      "id": "agents-input-conversion",
      "area": "agents-input",
      "status": "partial",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_thread_item_converter", "test_simple_to_agent_input"]
      },
      "bun": {
        "tests": ["tests/agents-converter.test.ts"],
        "sources": ["src/agents/converter.ts"],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-input-conversion-design.md"]
      },
      "notes": "Thread item and simple input conversion are covered, while input-side annotation replay remains deferred."
    },
    {
      "id": "annotations-output-conversion",
      "area": "agents-output",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_event_mapping"]
      },
      "bun": {
        "tests": ["tests/agents.test.ts", "tests/server.test.ts", "tests/parity-smoke.test.ts"],
        "sources": ["src/agents/annotations.ts", "src/server.ts"],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-annotation-hardening-design.md"]
      },
      "notes": "File, container file, and URL citation conversion plus server-side annotation merge behavior are covered locally."
    },
    {
      "id": "agents-workflows",
      "area": "agents-output",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_reasoning_workflow", "test_custom_workflow"]
      },
      "bun": {
        "tests": ["tests/agents.test.ts", "tests/server.test.ts"],
        "sources": ["src/agents/context.ts", "src/agents/workflows.ts", "src/agents/stream.ts"],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-reasoning-workflows-design.md"]
      },
      "notes": "Reasoning and custom workflow streaming, persistence, and resume behavior are covered by focused Bun suites."
    },
    {
      "id": "agents-generated-images",
      "area": "agents-output",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_image_generation_events"]
      },
      "bun": {
        "tests": ["tests/agents.test.ts", "tests/server.test.ts"],
        "sources": ["src/agents/annotations.ts", "src/agents/stream.ts", "src/server.ts"],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-generated-images-design.md"]
      },
      "notes": "Generated image added, partial update, final update, and server merge behavior are covered."
    },
    {
      "id": "agents-guardrail-rollback",
      "area": "agents-output",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_guardrail_tripwire"]
      },
      "bun": {
        "tests": ["tests/agents.test.ts"],
        "sources": ["src/agents/stream.ts"],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-guardrail-rollback-design.md"]
      },
      "notes": "Input, output, tool input, and tool output guardrail rollback paths are covered for produced SDK and context items."
    },
    {
      "id": "agents-client-tool-deferral",
      "area": "agents-output",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_client_tool_call"]
      },
      "bun": {
        "tests": ["tests/agents.test.ts", "tests/server.test.ts"],
        "sources": ["src/agents/context.ts", "src/agents/stream.ts", "src/server.ts"],
        "docs": ["docs/superpowers/specs/2026-05-27-chatkit-agents-core-slice-design.md"]
      },
      "notes": "Client tool call deferral, metadata retention, pending call cleanup, and resume paths are covered."
    },
    {
      "id": "server-processing",
      "area": "server",
      "status": "partial",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_chatkit_server.py"],
        "tests": []
      },
      "bun": {
        "tests": ["tests/server.test.ts", "tests/http.test.ts"],
        "sources": ["src/server.ts", "src/http.ts"],
        "docs": ["docs/superpowers/specs/2026-05-27-chatkit-server-parity-slice-design.md"]
      },
      "notes": "Core request processing, persistence, cancellation, retry, actions, transcription, and HTTP streaming are covered; some broader Python server surfaces remain future parity rows."
    },
    {
      "id": "widgets-fixtures",
      "area": "widgets",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_widgets.py"],
        "tests": []
      },
      "bun": {
        "tests": ["tests/widgets.test.ts"],
        "sources": ["src/widgets/template.ts", "src/widgets/components.ts"],
        "docs": ["docs/superpowers/specs/2026-05-27-chatkit-widget-parity-design.md"]
      },
      "notes": "Copied upstream widget fixtures and local widget rendering/diff behavior are covered by golden fixture tests."
    },
    {
      "id": "chatkit-wire-schemas",
      "area": "types",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_types.py"],
        "tests": []
      },
      "bun": {
        "tests": ["tests/types.test.ts", "tests/server-types.test.ts", "tests/parity-smoke.test.ts"],
        "sources": ["src/types/core.ts", "src/types/server.ts"],
        "docs": ["docs/superpowers/specs/2026-05-26-chatkit-bun-port-design.md"]
      },
      "notes": "Core thread item schemas and stream event wire names used by ChatKit clients are validated locally."
    },
    {
      "id": "http-sse-boundary",
      "area": "http",
      "status": "covered",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_chatkit_server.py"],
        "tests": []
      },
      "bun": {
        "tests": ["tests/http.test.ts", "tests/parity-smoke.test.ts"],
        "sources": ["src/http.ts", "src/server.ts"],
        "docs": ["docs/superpowers/specs/2026-05-27-chatkit-bun-http-handler-design.md"]
      },
      "notes": "The HTTP handler returns JSON or text/event-stream responses and preserves streaming chunks for ChatKit clients."
    },
    {
      "id": "agents-accumulate-text",
      "area": "agents-output",
      "status": "deferred",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_accumulate_text"]
      },
      "bun": {
        "tests": [],
        "sources": [],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-parity-foundation-design.md"]
      },
      "notes": "Python exposes text accumulation helpers; Bun does not export an equivalent yet."
    },
    {
      "id": "agents-previous-response-id",
      "area": "agents-output",
      "status": "deferred",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": []
      },
      "bun": {
        "tests": [],
        "sources": [],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-parity-foundation-design.md"]
      },
      "notes": "Python AgentContext tracks previous_response_id; Bun has not added that state or API surface."
    },
    {
      "id": "annotations-entity-sources",
      "area": "agents-output",
      "status": "deferred",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": []
      },
      "bun": {
        "tests": [],
        "sources": [],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-annotation-hardening-design.md"]
      },
      "notes": "Entity annotation source mapping is intentionally deferred until there is a concrete upstream payload contract to mirror."
    },
    {
      "id": "annotations-input-replay",
      "area": "agents-input",
      "status": "deferred",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": []
      },
      "bun": {
        "tests": [],
        "sources": [],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-annotation-hardening-design.md"]
      },
      "notes": "Input-side annotation replay is not implemented because current JS Agents input shapes do not preserve assistant annotation arrays."
    },
    {
      "id": "attachments-content-conversion",
      "area": "agents-input",
      "status": "deferred",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": []
      },
      "bun": {
        "tests": [],
        "sources": [],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-input-conversion-design.md"]
      },
      "notes": "Default attachment-to-message-content conversion still requires an application override."
    },
    {
      "id": "non-text-assistant-content",
      "area": "agents-output",
      "status": "deferred",
      "upstream": {
        "files": ["packages/chatkit-python/tests/test_agents.py"],
        "tests": ["test_stream_agent_response_assistant_message_content_types"]
      },
      "bun": {
        "tests": [],
        "sources": [],
        "docs": ["docs/superpowers/specs/2026-05-28-chatkit-agents-refusal-content-part-design.md"]
      },
      "notes": "Text and refusal content are covered; additional non-text assistant content parts are deferred until ChatKit wire support is defined."
    }
  ]
}
```

- [ ] **Step 4: Add the parity sync procedure**

Create `docs/parity/sync.md`:

```md
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
bun run verify
bun run verify:parity
```

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
```

- [ ] **Step 5: Run matrix tests to verify green**

Run:

```bash
bun test tests/parity-smoke.test.ts
```

Expected: PASS.

- [ ] **Step 6: Optional commit checkpoint**

Only if commits were explicitly requested:

```bash
git add docs/parity/matrix.json docs/parity/sync.md tests/parity-smoke.test.ts
git commit -m "$(cat <<'EOF'
Add parity matrix foundation

EOF
)"
```

## Task 2: Agents-To-Server Smoke

**Files:**
- Modify: `tests/parity-smoke.test.ts`

- [ ] **Step 1: Add full-path smoke test with a deliberate failing expectation**

Extend the imports at the top of `tests/parity-smoke.test.ts`:

```ts
import { AgentContext, streamAgentResponse } from "../src/agents";
import { ChatKitServer, StreamingResult } from "../src/server";
import { SQLiteStore } from "../src/sqlite-store";
import type { ThreadItem, ThreadMetadata } from "../src/types/core";
import { ThreadStreamEventSchema, type ThreadStreamEvent } from "../src/types/server";
```

Add these helpers above the `describe("parity matrix", ...)` block:

```ts
interface RequestContext {
  userId: string;
}

const now = "2026-05-28T00:00:00.000Z";
const requestContext: RequestContext = { userId: "user_parity" };

type UserMessageItem = Extract<ThreadItem, { type: "user_message" }>;

function makeThread(id = "thr_parity"): ThreadMetadata {
  return {
    id,
    created_at: now,
    status: { type: "active" },
    metadata: {},
  };
}

function rawResponse(data: Record<string, unknown>): unknown {
  return { type: "raw_response_event", data };
}

async function* streamFrom(events: unknown[]): AsyncIterable<unknown> {
  for (const event of events) {
    yield event;
  }
}

function streamedRun(events: unknown[]): { toStream: () => AsyncIterable<unknown> } {
  return { toStream: () => streamFrom(events) };
}

async function decodeStream(result: StreamingResult): Promise<ThreadStreamEvent[]> {
  const decoder = new TextDecoder();
  const events: ThreadStreamEvent[] = [];

  for await (const chunk of result.jsonEvents) {
    for (const frame of decoder.decode(chunk).split("\n\n")) {
      if (!frame) {
        continue;
      }

      const json = frame.startsWith("data: ") ? frame.slice("data: ".length) : frame;
      events.push(ThreadStreamEventSchema.parse(JSON.parse(json)));
    }
  }

  return events;
}

class AgentSmokeServer extends ChatKitServer<RequestContext> {
  constructor() {
    super(
      new SQLiteStore<RequestContext>({
        path: ":memory:",
        getUserId: (context) => context.userId,
      }),
    );
  }

  override respond(
    thread: ThreadMetadata,
    _inputUserMessage: UserMessageItem | null,
    context: RequestContext,
  ): AsyncIterable<ThreadStreamEvent> {
    return streamAgentResponse(
      new AgentContext({
        thread,
        store: this.store,
        context,
        now: () => now,
      }),
      streamedRun([
        rawResponse({
          type: "response.output_item.added",
          item: {
            type: "message",
            id: "msg_parity_agent",
            role: "assistant",
            content: [],
          },
        }),
        rawResponse({
          type: "response.content_part.added",
          item_id: "msg_parity_agent",
          content_index: 0,
          part: { type: "output_text", text: "" },
        }),
        rawResponse({
          type: "response.output_text.delta",
          item_id: "msg_parity_agent",
          content_index: 0,
          delta: "Hello!",
        }),
        rawResponse({
          type: "response.output_text.annotation.added",
          item_id: "msg_parity_agent",
          content_index: 0,
          annotation_index: 0,
          annotation: {
            type: "url_citation",
            url: "https://example.com/parity",
            title: "Wrong title for red test",
            end_index: 6,
          },
        }),
        rawResponse({
          type: "response.output_text.done",
          item_id: "msg_parity_agent",
          content_index: 0,
          text: "Hello!",
          annotations: [],
        }),
        rawResponse({
          type: "response.output_item.done",
          item: {
            type: "message",
            id: "msg_parity_agent",
            content: [{ type: "output_text", text: "Hello!", annotations: [] }],
          },
        }),
      ]),
    );
  }
}
```

Add this describe block after the existing `describe("parity matrix", ...)` block:

```ts
describe("ChatKit JS parity smoke", () => {
  test("preserves streamed Agents annotations through server persistence", async () => {
    const server = new AgentSmokeServer();
    const thread = makeThread();
    await server.store.saveThread(thread, requestContext);

    const result = (await server.process(
      JSON.stringify({
        type: "threads.add_user_message",
        params: {
          thread_id: thread.id,
          input: {
            content: [{ type: "input_text", text: "Run parity smoke" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      requestContext,
    )) as StreamingResult;

    const events = await decodeStream(result);
    expect(events.every((event) => ThreadStreamEventSchema.safeParse(event).success)).toBe(true);
    expect(events.map((event) => event.type)).toContain("thread.item.updated");
    expect(events.map((event) => event.type)).toContain("thread.item.done");

    await expect(server.store.loadItem(thread.id, "msg_parity_agent", requestContext)).resolves.toMatchObject({
      type: "assistant_message",
      content: [
        {
          type: "output_text",
          text: "Hello!",
          annotations: [
            {
              type: "annotation",
              source: {
                type: "url",
                url: "https://example.com/parity",
                title: "",
              },
              index: 6,
            },
          ],
        },
      ],
    });
  });
});
```

- [ ] **Step 2: Run test to verify red**

Run:

```bash
bun test tests/parity-smoke.test.ts
```

Expected: FAIL because the stored URL annotation title is `""`, not `"Wrong title for red test"`. This proves the smoke test exercises `streamAgentResponse`, `ChatKitServer.process`, and stored assistant content.

- [ ] **Step 3: Correct the failing expectation**

In the same test, replace the deliberate wrong title:

```ts
                title: "Wrong title for red test",
```

with:

```ts
                title: "",
```

- [ ] **Step 4: Fix only test compilation issues**

If TypeScript or Bun reports unused imports or mismatched types, fix the test file without changing production code. Do not edit `src/agents` or `src/server` for this task; existing behavior should already satisfy the smoke.

- [ ] **Step 5: Run focused smoke test to verify green**

Run:

```bash
bun test tests/parity-smoke.test.ts
```

Expected: PASS.

- [ ] **Step 6: Optional commit checkpoint**

Only if commits were explicitly requested:

```bash
git add tests/parity-smoke.test.ts
git commit -m "$(cat <<'EOF'
Add Agents server parity smoke

EOF
)"
```

## Task 3: HTTP/SSE Smoke And Opt-In Script

**Files:**
- Modify: `tests/parity-smoke.test.ts`
- Create: `scripts/verify-parity.ts`
- Modify: `package.json`
- Modify: `tests/package.test.ts`

- [ ] **Step 1: Add HTTP/SSE smoke test with a deliberate failing expectation**

Extend the imports in `tests/parity-smoke.test.ts`:

```ts
import { createChatKitHandler } from "../src/http";
```

Add this helper above the smoke describe block:

```ts
function parseSseFrames(text: string): unknown[] {
  return text
    .split("\n\n")
    .filter(Boolean)
    .map((frame) => {
      expect(frame.startsWith("data: ")).toBe(true);
      return JSON.parse(frame.slice("data: ".length));
    });
}
```

Add this test inside `describe("ChatKit JS parity smoke", ...)`:

```ts
  test("emits parseable ChatKit SSE frames from the HTTP handler", async () => {
    const server = new AgentSmokeServer();
    const handler = createChatKitHandler(server, {
      getContext: () => requestContext,
    });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({
          type: "threads.create",
          params: {
            input: {
              content: [{ type: "input_text", text: "Create parity thread" }],
              attachments: [],
              inference_options: {},
            },
          },
          metadata: {},
        }),
      }),
    );

    expect(response.headers.get("content-type")).toBe("application/json");
    const frames = parseSseFrames(await response.text());
    const events = frames.map((frame) => ThreadStreamEventSchema.parse(frame));

    expect(events[0]?.type).toBe("thread.created");
    expect(events.some((event) => event.type === "thread.item.done")).toBe(true);
    expect(events.some((event) => event.type === "thread.item.updated")).toBe(true);
  });
```

- [ ] **Step 2: Run test to verify red**

Run:

```bash
bun test tests/parity-smoke.test.ts
```

Expected: FAIL because the streaming response content type is `text/event-stream`, not `application/json`.

- [ ] **Step 3: Correct the failing HTTP/SSE expectation**

In the HTTP/SSE smoke test, replace:

```ts
    expect(response.headers.get("content-type")).toBe("application/json");
```

with:

```ts
    expect(response.headers.get("content-type")).toBe("text/event-stream");
```

- [ ] **Step 4: Add opt-in parity script test**

In `tests/package.test.ts`, extend the script assertion:

```ts
    expect(packageJson.scripts).toMatchObject({
      test: "bun test",
      typecheck: "bunx tsc --noEmit",
      verify: "bun run typecheck && bun test",
      "verify:parity": "bun run verify && bun scripts/verify-parity.ts",
    });
```

- [ ] **Step 5: Run package test to verify red**

Run:

```bash
bun test tests/package.test.ts
```

Expected: FAIL because `verify:parity` is not in `package.json`.

- [ ] **Step 6: Add the package script**

Modify `package.json` scripts:

```json
  "scripts": {
    "test": "bun test",
    "typecheck": "bunx tsc --noEmit",
    "verify": "bun run typecheck && bun test",
    "verify:parity": "bun run verify && bun scripts/verify-parity.ts"
  }
```

- [ ] **Step 7: Add the opt-in helper**

Create `scripts/verify-parity.ts`:

```ts
import matrix from "../docs/parity/matrix.json";
import upstream from "../docs/parity/upstream.json";

const deferredRows = matrix.rows.filter((row) => row.status === "deferred");

console.log(`Parity reference: ${upstream.packageName} ${upstream.version}`);
console.log(`Pinned commit: ${upstream.commit}`);
console.log(`Submodule path: ${upstream.submodulePath}`);
console.log(`Matrix rows: ${matrix.rows.length}`);
console.log(`Deferred rows: ${deferredRows.length}`);
console.log("");
console.log("Optional upstream check when the Python environment is available:");
console.log(`cd ${upstream.submodulePath} && make test`);
```

- [ ] **Step 8: Run focused tests to verify green**

Run:

```bash
bun test tests/parity-smoke.test.ts tests/package.test.ts
bun run verify:parity
```

Expected: PASS. `bun run verify:parity` should finish successfully and print the optional Python upstream command.

- [ ] **Step 9: Optional commit checkpoint**

Only if commits were explicitly requested:

```bash
git add tests/parity-smoke.test.ts scripts/verify-parity.ts package.json tests/package.test.ts
git commit -m "$(cat <<'EOF'
Add opt-in parity verification smoke

EOF
)"
```

## Task 4: Final Verification And Review

**Files:**
- Verify: all files changed by Tasks 1-3

- [ ] **Step 1: Run focused parity verification**

Run:

```bash
bun test tests/parity-metadata.test.ts tests/parity-smoke.test.ts
```

Expected: PASS.

- [ ] **Step 2: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 3: Run full verification**

Run:

```bash
bun run verify
```

Expected: PASS.

- [ ] **Step 4: Run opt-in parity verification**

Run:

```bash
bun run verify:parity
```

Expected: PASS. The command should print the pinned upstream package, commit, submodule path, matrix row count, deferred row count, and the optional `cd packages/chatkit-python && make test` command.

- [ ] **Step 5: Review scoped diff**

Run:

```bash
git diff -- docs/parity/matrix.json docs/parity/sync.md tests/parity-smoke.test.ts scripts/verify-parity.ts package.json tests/package.test.ts docs/superpowers/specs/2026-05-28-chatkit-parity-foundation-design.md docs/superpowers/plans/2026-05-28-chatkit-parity-foundation.md
```

Expected:

- New parity data and sync docs live only under `docs/parity`.
- New smoke coverage lives in `tests/parity-smoke.test.ts`.
- No production source files changed.
- `package.json` only adds `verify:parity`.
- `tests/package.test.ts` only asserts the new script.
- The design spec and this plan are included if they remain uncommitted.

- [ ] **Step 6: Final code review**

Dispatch a final code review with this context:

```md
Review the parity foundation implementation in the parity-foundation worktree.

Requirements:
- Matrix is tied to `docs/parity/upstream.json`.
- Matrix tests validate row structure, status values, local file references, and known deferred gaps.
- Smoke tests exercise local-only ChatKit JS event paths across Agents conversion, server persistence, and HTTP/SSE framing.
- `verify:parity` is opt-in and does not make Python pytest mandatory.
- No production behavior or public schemas changed.

Verification expected:
- `bun test tests/parity-metadata.test.ts tests/parity-smoke.test.ts`
- `bun run typecheck`
- `bun run verify`
- `bun run verify:parity`
```

- [ ] **Step 7: Mark implementation ready for finishing workflow**

After final review approval, use `superpowers:finishing-a-development-branch` to decide whether to merge locally, push a PR, keep the branch, or discard the work.
