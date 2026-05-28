# ChatKit Agents Core Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first `@openai/agents` to ChatKit stream bridge with assistant text streaming, context event merging, and deferred client tool calls.

**Architecture:** Add a focused `src/agents/` module with `AgentContext`, `ClientToolCall`, and `streamAgentResponse(...)`. The bridge accepts a real Agents SDK streamed result or compatible fixture stream, emits existing `ThreadStreamEvent` shapes, and leaves persistence/error conversion to `ChatKitServer.processEvents(...)`.

**Tech Stack:** Bun, TypeScript, `@openai/agents`, `bun:test`, existing Zod schemas, existing `Store<TContext>`, and `AsyncIterable<ThreadStreamEvent>`.

---

## Scope Check

The approved spec is a single core-slice subsystem. It explicitly excludes annotations, generated images, workflows, widget helpers, guardrail rollback, input conversion, and custom converter APIs. This plan implements only the accepted first slice.

## File Structure

- Modify: `package.json`
  - Adds `@openai/agents` as a runtime dependency through `bun add`.
  - Updates the package dependency test to include the new runtime dependency.
- Modify: `bun.lock`
  - Updated by `bun add @openai/agents`.
- Create: `src/agents/context.ts`
  - Defines `AgentContext`, `ClientToolCall`, and the internal async event queue.
- Create: `src/agents/types.ts`
  - Defines structural stream input types and context constructor options.
- Create: `src/agents/stream.ts`
  - Defines `streamAgentResponse(...)` and maps the first supported Agents SDK events to ChatKit stream events.
- Create: `src/agents/index.ts`
  - Re-exports the public agents API.
- Create: `tests/agents.test.ts`
  - Tests context queueing, assistant text streaming, context/SDK merge behavior, client tool call emission, cancellation, and ignored events.
- Modify: `src/index.ts`
  - Re-exports `./agents`.
- Modify: `tests/exports.test.ts`
  - Asserts the agents API is exported from the package root.

## Task 1: Add Agents SDK Dependency Metadata

**Files:**
- Modify: `package.json`
- Modify: `bun.lock`
- Modify: `tests/package.test.ts`

- [ ] **Step 1: Update the dependency test first**

Replace `tests/package.test.ts` with:

```ts
import { describe, expect, test } from "bun:test";

import packageJson from "../package.json";

describe("package metadata", () => {
  test("stays private and exposes Bun verification scripts", async () => {
    expect(packageJson.name).toBe("chatkit-bun");
    expect(packageJson.private).toBe(true);
    expect(packageJson.type).toBe("module");
    expect(packageJson.module).toBe("src/index.ts");
    expect(await Bun.file(packageJson.module).exists()).toBe(true);
    expect(packageJson.scripts).toMatchObject({
      test: "bun test",
      typecheck: "bunx tsc --noEmit",
      verify: "bun run typecheck && bun test",
    });
  });

  test("declares only the required runtime and development dependencies", () => {
    expect(Object.keys(packageJson.dependencies ?? {}).sort()).toEqual([
      "@openai/agents",
      "nunjucks",
      "zod",
    ]);
    expect(Object.keys(packageJson.devDependencies ?? {}).sort()).toEqual([
      "@types/bun",
      "@types/nunjucks",
      "typescript",
    ]);
    expect(packageJson.peerDependencies).toEqual({ typescript: "^5" });

    expect(typeof packageJson.dependencies?.["@openai/agents"]).toBe("string");
    expect(typeof packageJson.dependencies?.nunjucks).toBe("string");
    expect(typeof packageJson.dependencies?.zod).toBe("string");
    expect(typeof packageJson.devDependencies?.["@types/bun"]).toBe("string");
    expect(typeof packageJson.devDependencies?.["@types/nunjucks"]).toBe(
      "string",
    );
    expect(typeof packageJson.devDependencies?.typescript).toBe("string");
    expect(typeof packageJson.peerDependencies?.typescript).toBe("string");
  });
});
```

- [ ] **Step 2: Run the package test to verify it fails**

Run:

```bash
bun test tests/package.test.ts
```

Expected: FAIL because `@openai/agents` is not declared in `package.json`.

- [ ] **Step 3: Install the official Agents SDK with Bun**

Run:

```bash
bun add @openai/agents
```

Expected: `package.json` and `bun.lock` update. `@openai/agents` appears under `dependencies`.

- [ ] **Step 4: Run the package test to verify it passes**

Run:

```bash
bun test tests/package.test.ts
```

Expected: PASS.

- [ ] **Step 5: Commit dependency metadata**

Run:

```bash
git add package.json bun.lock tests/package.test.ts
git commit -m "Add OpenAI Agents SDK dependency"
```

Expected: Commit succeeds.

## Task 2: Add Agent Context Event Queue

**Files:**
- Create: `tests/agents.test.ts`
- Create: `src/agents/context.ts`
- Create: `src/agents/types.ts`
- Create: `src/agents/index.ts`

- [ ] **Step 1: Write failing AgentContext tests**

Create `tests/agents.test.ts`:

```ts
import { describe, expect, test } from "bun:test";

import { AgentContext, ClientToolCall } from "../src/agents";
import { BaseStore, type StoreItemType } from "../src/store";
import type { Attachment, Page, ThreadItem, ThreadMetadata } from "../src/types/core";
import type { ThreadStreamEvent } from "../src/types/server";

interface RequestContext {
  userId: string;
}

const now = "2026-05-27T00:00:00.000Z";
const thread: ThreadMetadata = { id: "thr_1", created_at: now };
const requestContext: RequestContext = { userId: "user_1" };

class TestStore extends BaseStore<RequestContext> {
  override generateItemId(itemType: StoreItemType): string {
    return `${itemType}_generated`;
  }

  override async loadThread(_threadId: string, _context: RequestContext): Promise<ThreadMetadata> {
    throw new Error("loadThread is not used by agents tests");
  }

  override async saveThread(_thread: ThreadMetadata, _context: RequestContext): Promise<void> {
    throw new Error("saveThread is not used by agents tests");
  }

  override async loadThreadItems(
    _threadId: string,
    _after: string | null,
    _limit: number,
    _order: "asc" | "desc",
    _context: RequestContext,
  ): Promise<Page<ThreadItem>> {
    throw new Error("loadThreadItems is not used by agents tests");
  }

  override async saveAttachment(_attachment: Attachment, _context: RequestContext): Promise<void> {
    throw new Error("saveAttachment is not used by agents tests");
  }

  override async loadAttachment(_attachmentId: string, _context: RequestContext): Promise<Attachment> {
    throw new Error("loadAttachment is not used by agents tests");
  }

  override async deleteAttachment(_attachmentId: string, _context: RequestContext): Promise<void> {
    throw new Error("deleteAttachment is not used by agents tests");
  }

  override async loadThreads(
    _limit: number,
    _after: string | null,
    _order: "asc" | "desc",
    _context: RequestContext,
  ): Promise<Page<ThreadMetadata>> {
    throw new Error("loadThreads is not used by agents tests");
  }

  override async addThreadItem(
    _threadId: string,
    _item: ThreadItem,
    _context: RequestContext,
  ): Promise<void> {
    throw new Error("addThreadItem is not used by agents tests");
  }

  override async saveItem(_threadId: string, _item: ThreadItem, _context: RequestContext): Promise<void> {
    throw new Error("saveItem is not used by agents tests");
  }

  override async loadItem(
    _threadId: string,
    _itemId: string,
    _context: RequestContext,
  ): Promise<ThreadItem> {
    throw new Error("loadItem is not used by agents tests");
  }

  override async deleteThread(_threadId: string, _context: RequestContext): Promise<void> {
    throw new Error("deleteThread is not used by agents tests");
  }

  override async deleteThreadItem(
    _threadId: string,
    _itemId: string,
    _context: RequestContext,
  ): Promise<void> {
    throw new Error("deleteThreadItem is not used by agents tests");
  }
}

function createContext(): AgentContext<RequestContext> {
  return new AgentContext({
    thread,
    store: new TestStore(),
    context: requestContext,
    now: () => now,
  });
}

async function collect(iterable: AsyncIterable<ThreadStreamEvent>): Promise<ThreadStreamEvent[]> {
  const events: ThreadStreamEvent[] = [];

  for await (const event of iterable) {
    events.push(event);
  }

  return events;
}

describe("AgentContext", () => {
  test("stores thread, store, request context, and deterministic timestamps", () => {
    const agentContext = createContext();

    expect(agentContext.thread).toEqual(thread);
    expect(agentContext.context).toEqual(requestContext);
    expect(agentContext.createdAt()).toBe(now);
    expect(agentContext.store.generateItemId("tool_call", thread, requestContext)).toBe(
      "tool_call_generated",
    );
  });

  test("queues validated stream events", async () => {
    const agentContext = createContext();

    agentContext.stream({
      type: "progress_update",
      icon: "sparkle",
      text: "Thinking",
    });
    agentContext.closeEvents();

    await expect(collect(agentContext.events())).resolves.toEqual([
      {
        type: "progress_update",
        icon: "sparkle",
        text: "Thinking",
      },
    ]);
  });

  test("rejects invalid stream events before queueing them", () => {
    const agentContext = createContext();

    expect(() => agentContext.stream({ type: "progress_update" } as never)).toThrow();
  });

  test("records one client tool call per turn", () => {
    const agentContext = createContext();
    const toolCall = new ClientToolCall("get_selection", { includeHtml: true });

    agentContext.setClientToolCall(toolCall);

    expect(agentContext.getClientToolCall()).toBe(toolCall);
    expect(() => agentContext.setClientToolCall(new ClientToolCall("other"))).toThrow(
      "Only one client tool call can be set per response.",
    );
  });
});
```

- [ ] **Step 2: Run the new agents test to verify it fails**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because `../src/agents` does not exist.

- [ ] **Step 3: Add shared agents types**

Create `src/agents/types.ts`:

```ts
import type { Store } from "../store";
import type { ThreadMetadata } from "../types/core";

export interface AgentContextOptions<TContext> {
  thread: ThreadMetadata;
  store: Store<TContext>;
  context: TContext;
  now?: () => Date | string;
}

export interface AgentStreamInput {
  [Symbol.asyncIterator]?: () => AsyncIterator<unknown>;
  toStream?: () => AsyncIterable<unknown>;
}

export interface ToolCallMetadata {
  itemId: string | null;
  callId: string | null;
}

export type JsonObject = Record<string, unknown>;
```

- [ ] **Step 4: Add AgentContext and ClientToolCall**

Create `src/agents/context.ts`:

```ts
import { ThreadStreamEventSchema, type ThreadStreamEvent } from "../types/server";
import type { AgentContextOptions, JsonObject } from "./types";

class AsyncEventQueue<T> implements AsyncIterable<T> {
  private readonly values: T[] = [];
  private readonly waiting: Array<(result: IteratorResult<T>) => void> = [];
  private closed = false;

  push(value: T): void {
    if (this.closed) {
      throw new Error("Cannot stream events after the agent context has completed.");
    }

    const resolve = this.waiting.shift();

    if (resolve) {
      resolve({ done: false, value });
      return;
    }

    this.values.push(value);
  }

  close(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;

    for (const resolve of this.waiting.splice(0)) {
      resolve({ done: true, value: undefined });
    }
  }

  async next(): Promise<IteratorResult<T>> {
    const value = this.values.shift();

    if (value !== undefined) {
      return { done: false, value };
    }

    if (this.closed) {
      return { done: true, value: undefined };
    }

    return new Promise((resolve) => {
      this.waiting.push(resolve);
    });
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => this.next(),
      return: async () => {
        this.close();
        return { done: true, value: undefined };
      },
    };
  }
}

export class ClientToolCall {
  readonly arguments: JsonObject;

  constructor(
    readonly name: string,
    args: JsonObject = {},
  ) {
    this.arguments = args;
  }
}

export class AgentContext<TContext> {
  readonly thread: AgentContextOptions<TContext>["thread"];
  readonly store: AgentContextOptions<TContext>["store"];
  readonly context: TContext;
  private readonly now: () => Date | string;
  private readonly queue = new AsyncEventQueue<ThreadStreamEvent>();
  private clientToolCall: ClientToolCall | null = null;

  constructor(options: AgentContextOptions<TContext>) {
    this.thread = options.thread;
    this.store = options.store;
    this.context = options.context;
    this.now = options.now ?? (() => new Date());
  }

  stream(event: ThreadStreamEvent): void {
    this.queue.push(ThreadStreamEventSchema.parse(event));
  }

  events(): AsyncIterable<ThreadStreamEvent> {
    return this.queue;
  }

  closeEvents(): void {
    this.queue.close();
  }

  setClientToolCall(toolCall: ClientToolCall): void {
    if (this.clientToolCall) {
      throw new Error("Only one client tool call can be set per response.");
    }

    this.clientToolCall = toolCall;
  }

  getClientToolCall(): ClientToolCall | null {
    return this.clientToolCall;
  }

  createdAt(): string {
    const value = this.now();
    return typeof value === "string" ? value : value.toISOString();
  }
}
```

- [ ] **Step 5: Add the agents module barrel**

Create `src/agents/index.ts`:

```ts
export { AgentContext, ClientToolCall } from "./context";
export type { AgentContextOptions, AgentStreamInput } from "./types";
```

- [ ] **Step 6: Run the focused agents tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS for the `AgentContext` tests.

- [ ] **Step 7: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 8: Commit AgentContext**

Run:

```bash
git add src/agents/context.ts src/agents/types.ts src/agents/index.ts tests/agents.test.ts
git commit -m "Add Agents context event queue"
```

Expected: Commit succeeds.

## Task 3: Add Core Stream Conversion

**Files:**
- Modify: `tests/agents.test.ts`
- Create: `src/agents/stream.ts`
- Modify: `src/agents/index.ts`

- [ ] **Step 1: Expand agents tests for stream conversion**

Replace `tests/agents.test.ts` with:

```ts
import { describe, expect, test } from "bun:test";

import { AgentContext, ClientToolCall, streamAgentResponse } from "../src/agents";
import { BaseStore, type StoreItemType } from "../src/store";
import type { Attachment, Page, ThreadItem, ThreadMetadata } from "../src/types/core";
import type { ThreadStreamEvent } from "../src/types/server";

interface RequestContext {
  userId: string;
}

const now = "2026-05-27T00:00:00.000Z";
const thread: ThreadMetadata = { id: "thr_1", created_at: now };
const requestContext: RequestContext = { userId: "user_1" };

class TestStore extends BaseStore<RequestContext> {
  override generateItemId(itemType: StoreItemType): string {
    return `${itemType}_generated`;
  }

  override async loadThread(_threadId: string, _context: RequestContext): Promise<ThreadMetadata> {
    throw new Error("loadThread is not used by agents tests");
  }

  override async saveThread(_thread: ThreadMetadata, _context: RequestContext): Promise<void> {
    throw new Error("saveThread is not used by agents tests");
  }

  override async loadThreadItems(
    _threadId: string,
    _after: string | null,
    _limit: number,
    _order: "asc" | "desc",
    _context: RequestContext,
  ): Promise<Page<ThreadItem>> {
    throw new Error("loadThreadItems is not used by agents tests");
  }

  override async saveAttachment(_attachment: Attachment, _context: RequestContext): Promise<void> {
    throw new Error("saveAttachment is not used by agents tests");
  }

  override async loadAttachment(_attachmentId: string, _context: RequestContext): Promise<Attachment> {
    throw new Error("loadAttachment is not used by agents tests");
  }

  override async deleteAttachment(_attachmentId: string, _context: RequestContext): Promise<void> {
    throw new Error("deleteAttachment is not used by agents tests");
  }

  override async loadThreads(
    _limit: number,
    _after: string | null,
    _order: "asc" | "desc",
    _context: RequestContext,
  ): Promise<Page<ThreadMetadata>> {
    throw new Error("loadThreads is not used by agents tests");
  }

  override async addThreadItem(
    _threadId: string,
    _item: ThreadItem,
    _context: RequestContext,
  ): Promise<void> {
    throw new Error("addThreadItem is not used by agents tests");
  }

  override async saveItem(_threadId: string, _item: ThreadItem, _context: RequestContext): Promise<void> {
    throw new Error("saveItem is not used by agents tests");
  }

  override async loadItem(
    _threadId: string,
    _itemId: string,
    _context: RequestContext,
  ): Promise<ThreadItem> {
    throw new Error("loadItem is not used by agents tests");
  }

  override async deleteThread(_threadId: string, _context: RequestContext): Promise<void> {
    throw new Error("deleteThread is not used by agents tests");
  }

  override async deleteThreadItem(
    _threadId: string,
    _itemId: string,
    _context: RequestContext,
  ): Promise<void> {
    throw new Error("deleteThreadItem is not used by agents tests");
  }
}

function createContext(): AgentContext<RequestContext> {
  return new AgentContext({
    thread,
    store: new TestStore(),
    context: requestContext,
    now: () => now,
  });
}

async function collect(iterable: AsyncIterable<ThreadStreamEvent>): Promise<ThreadStreamEvent[]> {
  const events: ThreadStreamEvent[] = [];

  for await (const event of iterable) {
    events.push(event);
  }

  return events;
}

function streamFrom(events: unknown[], onReturn?: () => void): AsyncIterable<unknown> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<unknown> {
      let index = 0;

      return {
        async next(): Promise<IteratorResult<unknown>> {
          if (index >= events.length) {
            return { done: true, value: undefined };
          }

          return { done: false, value: events[index++] };
        },
        async return(): Promise<IteratorResult<unknown>> {
          onReturn?.();
          return { done: true, value: undefined };
        },
      };
    },
  };
}

function streamedRun(events: unknown[]): { toStream: () => AsyncIterable<unknown> } {
  return { toStream: () => streamFrom(events) };
}

function rawResponse(data: Record<string, unknown>): unknown {
  return { type: "raw_response_event", data };
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((innerResolve) => {
    resolve = innerResolve;
  });
  return { promise, resolve };
}

describe("AgentContext", () => {
  test("stores thread, store, request context, and deterministic timestamps", () => {
    const agentContext = createContext();

    expect(agentContext.thread).toEqual(thread);
    expect(agentContext.context).toEqual(requestContext);
    expect(agentContext.createdAt()).toBe(now);
    expect(agentContext.store.generateItemId("tool_call", thread, requestContext)).toBe(
      "tool_call_generated",
    );
  });

  test("queues validated stream events", async () => {
    const agentContext = createContext();

    agentContext.stream({
      type: "progress_update",
      icon: "sparkle",
      text: "Thinking",
    });
    agentContext.closeEvents();

    await expect(collect(agentContext.events())).resolves.toEqual([
      {
        type: "progress_update",
        icon: "sparkle",
        text: "Thinking",
      },
    ]);
  });

  test("rejects invalid stream events before queueing them", () => {
    const agentContext = createContext();

    expect(() => agentContext.stream({ type: "progress_update" } as never)).toThrow();
  });

  test("records one client tool call per turn", () => {
    const agentContext = createContext();
    const toolCall = new ClientToolCall("get_selection", { includeHtml: true });

    agentContext.setClientToolCall(toolCall);

    expect(agentContext.getClientToolCall()).toBe(toolCall);
    expect(() => agentContext.setClientToolCall(new ClientToolCall("other"))).toThrow(
      "Only one client tool call can be set per response.",
    );
  });
});

describe("streamAgentResponse", () => {
  test("yields context-only events when the SDK stream is empty", async () => {
    const agentContext = createContext();

    agentContext.stream({ type: "progress_update", icon: null, text: "Queued" });

    await expect(collect(streamAgentResponse(agentContext, streamedRun([])))).resolves.toEqual([
      { type: "progress_update", icon: null, text: "Queued" },
    ]);
  });

  test("maps assistant message text events", async () => {
    const agentContext = createContext();
    const events = await collect(
      streamAgentResponse(
        agentContext,
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: { type: "message", id: "msg_1" },
          }),
          rawResponse({
            type: "response.output_text.delta",
            item_id: "msg_1",
            content_index: 0,
            delta: "Hello, ",
          }),
          rawResponse({
            type: "response.output_text.delta",
            item_id: "msg_1",
            content_index: 0,
            delta: "world!",
          }),
          rawResponse({
            type: "response.output_text.done",
            item_id: "msg_1",
            content_index: 0,
            text: "Hello, world!",
          }),
          rawResponse({
            type: "response.output_item.done",
            item: {
              type: "message",
              id: "msg_1",
              content: [{ type: "output_text", text: "Hello, world!" }],
            },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
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
      {
        type: "thread.item.updated",
        item_id: "msg_1",
        update: {
          type: "assistant_message.content_part.text_delta",
          content_index: 0,
          delta: "Hello, ",
        },
      },
      {
        type: "thread.item.updated",
        item_id: "msg_1",
        update: {
          type: "assistant_message.content_part.text_delta",
          content_index: 0,
          delta: "world!",
        },
      },
      {
        type: "thread.item.updated",
        item_id: "msg_1",
        update: {
          type: "assistant_message.content_part.done",
          content_index: 0,
          content: { type: "output_text", text: "Hello, world!", annotations: [] },
        },
      },
      {
        type: "thread.item.done",
        item: {
          id: "msg_1",
          thread_id: "thr_1",
          created_at: now,
          type: "assistant_message",
          content: [{ type: "output_text", text: "Hello, world!", annotations: [] }],
        },
      },
    ]);
  });

  test("yields context events while waiting for SDK events", async () => {
    const agentContext = createContext();
    const gate = deferred();
    const sdkEvents = {
      async *[Symbol.asyncIterator](): AsyncIterator<unknown> {
        await gate.promise;
        yield rawResponse({
          type: "response.output_text.delta",
          item_id: "msg_1",
          content_index: 0,
          delta: "after",
        });
      },
    };
    const iterator = streamAgentResponse(agentContext, sdkEvents)[Symbol.asyncIterator]();
    const first = iterator.next();

    await Promise.resolve();
    agentContext.stream({ type: "progress_update", icon: null, text: "Still working" });

    await expect(first).resolves.toEqual({
      done: false,
      value: { type: "progress_update", icon: null, text: "Still working" },
    });

    gate.resolve();

    await expect(iterator.next()).resolves.toEqual({
      done: false,
      value: {
        type: "thread.item.updated",
        item_id: "msg_1",
        update: {
          type: "assistant_message.content_part.text_delta",
          content_index: 0,
          delta: "after",
        },
      },
    });
    await expect(iterator.next()).resolves.toEqual({ done: true, value: undefined });
  });

  test("ignores unknown SDK events in the first slice", async () => {
    const agentContext = createContext();

    await expect(
      collect(
        streamAgentResponse(
          agentContext,
          streamedRun([
            rawResponse({ type: "response.created" }),
            { type: "run_item_stream_event", item: { type: "server_tool_call_item" } },
          ]),
        ),
      ),
    ).resolves.toEqual([]);
  });
});
```

- [ ] **Step 2: Run the agents tests to verify the new stream tests fail**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because `streamAgentResponse` is not exported.

- [ ] **Step 3: Add stream conversion implementation**

Create `src/agents/stream.ts`:

```ts
import { ThreadStreamEventSchema, type ThreadStreamEvent } from "../types/server";
import type { AgentContext } from "./context";
import type { AgentStreamInput, ToolCallMetadata } from "./types";

type UnknownRecord = Record<string, unknown>;

interface AssistantTextState {
  activeItemId: string | null;
  textByPart: Map<string, string>;
}

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) ? value : null;
}

function normalizeStream(streamedRun: AgentStreamInput | AsyncIterable<unknown>): AsyncIterable<unknown> {
  if (isRecord(streamedRun) && typeof streamedRun.toStream === "function") {
    return streamedRun.toStream();
  }

  if (
    isRecord(streamedRun) &&
    typeof streamedRun[Symbol.asyncIterator] === "function"
  ) {
    return streamedRun as AsyncIterable<unknown>;
  }

  throw new Error("streamAgentResponse requires an async iterable or an object with toStream().");
}

function rawResponseData(event: unknown): UnknownRecord | null {
  if (!isRecord(event)) {
    return null;
  }

  if (
    (event.type === "raw_response_event" || event.type === "raw_model_stream_event") &&
    isRecord(event.data)
  ) {
    return event.data;
  }

  if (typeof event.type === "string" && event.type.startsWith("response.")) {
    return event;
  }

  return null;
}

function partKey(itemId: string, contentIndex: number): string {
  return `${itemId}:${contentIndex}`;
}

function assistantItem<TContext>(
  context: AgentContext<TContext>,
  itemId: string,
  content: Array<{ type: "output_text"; text: string; annotations: [] }>,
): Extract<ThreadStreamEvent, { type: "thread.item.added" }>["item"] {
  return {
    id: itemId,
    thread_id: context.thread.id,
    created_at: context.createdAt(),
    type: "assistant_message",
    content,
  };
}

function assistantContentFromItem(
  item: UnknownRecord,
  fallbackText: string,
): Array<{ type: "output_text"; text: string; annotations: [] }> {
  const rawContent = Array.isArray(item.content) ? item.content : [];
  const content = rawContent.flatMap((part) => {
    if (!isRecord(part)) {
      return [];
    }

    const text = stringValue(part.text);

    if (text === null) {
      return [];
    }

    return [{ type: "output_text" as const, text, annotations: [] }];
  });

  if (content.length > 0) {
    return content;
  }

  return fallbackText.length > 0
    ? [{ type: "output_text", text: fallbackText, annotations: [] }]
    : [];
}

function trackToolCallMetadata(event: unknown): ToolCallMetadata | null {
  if (!isRecord(event) || event.type !== "run_item_stream_event" || !isRecord(event.item)) {
    return null;
  }

  const item = event.item;

  if (item.type !== "tool_call_item") {
    return null;
  }

  const rawItem = isRecord(item.raw_item)
    ? item.raw_item
    : isRecord(item.rawItem)
      ? item.rawItem
      : item;

  return {
    itemId: stringValue(rawItem.id) ?? stringValue(item.id),
    callId:
      stringValue(rawItem.call_id) ??
      stringValue(rawItem.callId) ??
      stringValue(item.call_id) ??
      stringValue(item.callId),
  };
}

function convertSdkEvent<TContext>(
  context: AgentContext<TContext>,
  state: AssistantTextState,
  event: unknown,
): ThreadStreamEvent[] {
  const rawData = rawResponseData(event);

  if (!rawData) {
    return [];
  }

  switch (rawData.type) {
    case "response.output_item.added": {
      const item = isRecord(rawData.item) ? rawData.item : null;

      if (!item || item.type !== "message") {
        return [];
      }

      const itemId =
        stringValue(item.id) ?? context.store.generateItemId("message", context.thread, context.context);
      state.activeItemId = itemId;
      return [
        {
          type: "thread.item.added",
          item: assistantItem(context, itemId, []),
        },
      ];
    }

    case "response.output_text.delta": {
      const itemId = stringValue(rawData.item_id) ?? state.activeItemId;

      if (!itemId) {
        return [];
      }

      const contentIndex = numberValue(rawData.content_index) ?? 0;
      const delta = stringValue(rawData.delta) ?? "";
      const key = partKey(itemId, contentIndex);
      state.textByPart.set(key, `${state.textByPart.get(key) ?? ""}${delta}`);

      return [
        {
          type: "thread.item.updated",
          item_id: itemId,
          update: {
            type: "assistant_message.content_part.text_delta",
            content_index: contentIndex,
            delta,
          },
        },
      ];
    }

    case "response.output_text.done": {
      const itemId = stringValue(rawData.item_id) ?? state.activeItemId;

      if (!itemId) {
        return [];
      }

      const contentIndex = numberValue(rawData.content_index) ?? 0;
      const text = stringValue(rawData.text) ?? state.textByPart.get(partKey(itemId, contentIndex)) ?? "";

      return [
        {
          type: "thread.item.updated",
          item_id: itemId,
          update: {
            type: "assistant_message.content_part.done",
            content_index: contentIndex,
            content: { type: "output_text", text, annotations: [] },
          },
        },
      ];
    }

    case "response.output_item.done": {
      const item = isRecord(rawData.item) ? rawData.item : null;

      if (!item || item.type !== "message") {
        return [];
      }

      const itemId = stringValue(item.id) ?? state.activeItemId;

      if (!itemId) {
        return [];
      }

      const fallbackText = state.textByPart.get(partKey(itemId, 0)) ?? "";

      return [
        {
          type: "thread.item.done",
          item: assistantItem(context, itemId, assistantContentFromItem(item, fallbackText)),
        },
      ];
    }

    default:
      return [];
  }
}

function pendingClientToolCallEvent<TContext>(
  context: AgentContext<TContext>,
  metadata: ToolCallMetadata | null,
): ThreadStreamEvent | null {
  const toolCall = context.getClientToolCall();

  if (!toolCall) {
    return null;
  }

  const fallbackId = context.store.generateItemId("tool_call", context.thread, context.context);
  const id = metadata?.itemId ?? fallbackId;

  return {
    type: "thread.item.done",
    item: {
      id,
      thread_id: context.thread.id,
      created_at: context.createdAt(),
      type: "client_tool_call",
      status: "pending",
      call_id: metadata?.callId ?? id,
      name: toolCall.name,
      arguments: toolCall.arguments,
    },
  };
}

export async function* streamAgentResponse<TContext>(
  context: AgentContext<TContext>,
  streamedRun: AgentStreamInput | AsyncIterable<unknown>,
): AsyncIterable<ThreadStreamEvent> {
  const sdkIterator = normalizeStream(streamedRun)[Symbol.asyncIterator]();
  const contextIterator = context.events()[Symbol.asyncIterator]();
  const state: AssistantTextState = { activeItemId: null, textByPart: new Map() };
  let latestToolCallMetadata: ToolCallMetadata | null = null;
  let sdkDone = false;
  let contextDone = false;
  let sdkNext = sdkIterator.next();
  let contextNext = contextIterator.next();

  try {
    while (!sdkDone || !contextDone) {
      const contenders: Array<Promise<{ source: "sdk" | "context"; result: IteratorResult<unknown> }>> = [];

      if (!sdkDone) {
        contenders.push(sdkNext.then((result) => ({ source: "sdk", result })));
      }

      if (!contextDone) {
        contenders.push(contextNext.then((result) => ({ source: "context", result })));
      }

      if (contenders.length === 0) {
        break;
      }

      const next = await Promise.race(contenders);

      if (next.source === "context") {
        if (next.result.done) {
          contextDone = true;
        } else {
          contextNext = contextIterator.next();
          yield ThreadStreamEventSchema.parse(next.result.value);
        }
        continue;
      }

      if (next.result.done) {
        sdkDone = true;
        context.closeEvents();
        continue;
      }

      sdkNext = sdkIterator.next();
      latestToolCallMetadata = trackToolCallMetadata(next.result.value) ?? latestToolCallMetadata;

      for (const event of convertSdkEvent(context, state, next.result.value)) {
        yield ThreadStreamEventSchema.parse(event);
      }
    }

    const clientToolCallEvent = pendingClientToolCallEvent(context, latestToolCallMetadata);

    if (clientToolCallEvent) {
      yield ThreadStreamEventSchema.parse(clientToolCallEvent);
    }
  } finally {
    context.closeEvents();
    await sdkIterator.return?.();
    await contextIterator.return?.();
  }
}
```

- [ ] **Step 4: Export streamAgentResponse from the agents barrel**

Replace `src/agents/index.ts` with:

```ts
export { AgentContext, ClientToolCall } from "./context";
export { streamAgentResponse } from "./stream";
export type { AgentContextOptions, AgentStreamInput } from "./types";
```

- [ ] **Step 5: Run focused agents tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS for context and stream conversion tests.

- [ ] **Step 6: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 7: Commit stream conversion**

Run:

```bash
git add src/agents/index.ts src/agents/stream.ts tests/agents.test.ts
git commit -m "Add Agents stream conversion"
```

Expected: Commit succeeds.

## Task 4: Cover Deferred Client Tool Calls And Cancellation

**Files:**
- Modify: `tests/agents.test.ts`
- Modify: `src/agents/stream.ts`

- [ ] **Step 1: Add client tool and cancellation tests**

Append these tests inside the existing `describe("streamAgentResponse", () => { ... })` block in `tests/agents.test.ts`:

```ts
  test("emits a deferred pending client tool call with SDK metadata", async () => {
    const agentContext = createContext();
    agentContext.setClientToolCall(new ClientToolCall("get_selection", { includeHtml: true }));

    const events = await collect(
      streamAgentResponse(
        agentContext,
        streamedRun([
          {
            type: "run_item_stream_event",
            item: {
              type: "tool_call_item",
              raw_item: {
                type: "function_call",
                id: "fc_123",
                call_id: "call_123",
              },
            },
          },
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.done",
        item: {
          id: "fc_123",
          thread_id: "thr_1",
          created_at: now,
          type: "client_tool_call",
          status: "pending",
          call_id: "call_123",
          name: "get_selection",
          arguments: { includeHtml: true },
        },
      },
    ]);
  });

  test("generates fallback client tool ids when SDK metadata is absent", async () => {
    const agentContext = createContext();
    agentContext.setClientToolCall(new ClientToolCall("get_selection"));

    const events = await collect(streamAgentResponse(agentContext, streamedRun([])));

    expect(events).toEqual([
      {
        type: "thread.item.done",
        item: {
          id: "tool_call_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "client_tool_call",
          status: "pending",
          call_id: "tool_call_generated",
          name: "get_selection",
          arguments: {},
        },
      },
    ]);
  });

  test("cancelling the merged stream returns the SDK iterator", async () => {
    const agentContext = createContext();
    let returned = false;
    const iterator = streamAgentResponse(
      agentContext,
      streamFrom(
        [
          rawResponse({
            type: "response.output_text.delta",
            item_id: "msg_1",
            content_index: 0,
            delta: "first",
          }),
          rawResponse({
            type: "response.output_text.delta",
            item_id: "msg_1",
            content_index: 0,
            delta: "second",
          }),
        ],
        () => {
          returned = true;
        },
      ),
    )[Symbol.asyncIterator]();

    await expect(iterator.next()).resolves.toMatchObject({ done: false });
    await iterator.return?.();

    expect(returned).toBe(true);
    expect(() => agentContext.stream({ type: "progress_update", text: "late" })).toThrow(
      "Cannot stream events after the agent context has completed.",
    );
  });
```

- [ ] **Step 2: Run the agents tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS. The `stream.ts` implementation from Task 3 already contains the client tool and cancellation behavior required by these tests. If this command fails, fix only `src/agents/stream.ts` or `src/agents/context.ts` until these tests pass without expanding scope.

- [ ] **Step 3: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 4: Commit client tool and cancellation coverage**

Run:

```bash
git add src/agents/context.ts src/agents/stream.ts tests/agents.test.ts
git commit -m "Cover Agents client tool stream behavior"
```

Expected: Commit succeeds.

## Task 5: Export The Public Agents API

**Files:**
- Modify: `src/index.ts`
- Modify: `tests/exports.test.ts`

- [ ] **Step 1: Add failing root export assertions**

Replace `tests/exports.test.ts` with:

```ts
import { describe, expect, test } from "bun:test";

import * as exports from "../src";
import {
  ActionConfigSchema,
  AgentContext,
  BaseStore,
  Card,
  ChatKitServer,
  ClientToolCall,
  NonStreamingResult,
  SQLiteStore,
  StreamingResult,
  ThreadMetadataSchema,
  WidgetTemplate,
  createActionConfig,
  createChatKitHandler,
  decodeJsonBytes,
  diffWidget,
  encodeJsonBytes,
  defaultGenerateId,
  streamAgentResponse,
  streamWidget,
} from "../src";

describe("public exports", () => {
  test("exports foundation APIs", () => {
    expect(createActionConfig("x")).toMatchObject({ type: "x" });
    expect(ActionConfigSchema.parse({ type: "x" }).type).toBe("x");
    expect(ThreadMetadataSchema.parse({ id: "thr_1", created_at: "2026-05-26T00:00:00.000Z" }).id).toBe("thr_1");
    expect(defaultGenerateId("thread")).toMatch(/^thr_[0-9a-f]{8}$/);
    expect(typeof BaseStore).toBe("function");
    expect(typeof SQLiteStore).toBe("function");
    expect(ChatKitServer).toBeDefined();
    expect(typeof createChatKitHandler).toBe("function");
    expect(StreamingResult).toBeDefined();
    expect(NonStreamingResult).toBeDefined();
    expect(decodeJsonBytes(encodeJsonBytes({ ok: true }))).toEqual({ ok: true });
    expect(exports.StreamCancelledError).toBeDefined();
    expect(exports.ChatKitRequestSchema).toBeDefined();
    expect(exports.ThreadStreamEventSchema).toBeDefined();
    expect(typeof Card).toBe("function");
    expect(typeof WidgetTemplate).toBe("function");
    expect(typeof diffWidget).toBe("function");
    expect(typeof streamWidget).toBe("function");
    expect(typeof AgentContext).toBe("function");
    expect(typeof ClientToolCall).toBe("function");
    expect(typeof streamAgentResponse).toBe("function");
  });
});
```

- [ ] **Step 2: Run the export test to verify it fails**

Run:

```bash
bun test tests/exports.test.ts
```

Expected: FAIL because `src/index.ts` does not export `./agents`.

- [ ] **Step 3: Export the agents module from the package root**

Replace `src/index.ts` with:

```ts
export * from "./actions";
export * from "./agents";
export * from "./errors";
export * from "./http";
export * from "./serialization";
export * from "./server";
export * from "./sqlite-store";
export * from "./store";
export * from "./types/core";
export * from "./types/server";
export * from "./widgets";
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
bun test tests/exports.test.ts tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:

```bash
bun run verify
```

Expected: PASS for typecheck and all Bun tests.

- [ ] **Step 6: Commit public exports**

Run:

```bash
git add src/index.ts tests/exports.test.ts
git commit -m "Export Agents core bridge"
```

Expected: Commit succeeds.

## Final Verification

After all tasks are complete, run:

```bash
bun run verify
git status --short --branch
```

Expected:

- TypeScript typecheck passes.
- All Bun tests pass.
- Git status is clean on the implementation branch.

## Implementation Notes

- Do not change `ChatKitServer`.
- Do not add annotations, generated image handling, workflows, widget helpers, guardrail rollback, input conversion, or converter extension APIs.
- Do not parse or serialize SSE bytes in the agents bridge. The bridge emits `ThreadStreamEvent` objects only.
- Use `ThreadStreamEventSchema.parse(...)` before events leave the agents module.
- Keep fixture streams deterministic and network-free; tests must not call OpenAI APIs.
- Keep `@openai/agents` as a runtime dependency because applications pass real streamed results from that package to `streamAgentResponse(...)`.
