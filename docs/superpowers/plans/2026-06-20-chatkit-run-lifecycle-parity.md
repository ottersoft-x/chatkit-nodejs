# ChatKit Run Lifecycle Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate SSE subscriber disconnect from explicit backend cancellation, while fixing the reviewed Python parity gaps for attachment metadata, stream errors, and cancellation context.

**Architecture:** Add focused modules for stream runtime/cancellation helpers, client-safe response sanitization, and in-process response run fanout. Keep `ChatKitServer` responsible for parsed request routing and event persistence, while `createChatKitHandler` owns HTTP subscriber behavior through an injectable `ResponseRunManager`.

**Tech Stack:** TypeScript, Node.js `>=24.15.0`, npm scripts, `node:test`, Zod, Web `ReadableStream`.

---

## File Structure

- Create `src/stream-runtime.ts`: cancellation sentinel, runtime options, abort-racing helper, iterator cleanup helper.
- Create `src/response-sanitizer.ts`: pure helpers that clone client-bound payloads and strip attachment metadata everywhere it can leak.
- Create `src/run-manager.ts`: in-process response run manager, run id generation, scope checks, fanout subscribers, cancellation result semantics.
- Modify `src/errors.ts`: add Python-style `StreamError` and `CustomStreamError`.
- Modify `src/types/server.ts`: make error event codes open strings.
- Modify `src/server.ts`: add event-level streaming result/API, pass runtime to `respond`/`action`/`getStreamOptions`, use sanitizer before serialization, map stream errors, align cancellation hidden context.
- Modify `src/agents/types.ts` and `src/agents/stream.ts`: pass explicit cancellation signal through `streamAgentResponse`.
- Modify `src/http.ts`: integrate `ResponseRunManager`, default disconnect continuation, run id header, opt-in disconnect cancellation.
- Modify `src/index.ts`: export new public helpers.
- Modify `tests/server.test.ts`, `tests/http.test.ts`, `tests/agents.test.ts`, `tests/server-types.test.ts`; create `tests/stream-runtime.test.ts`, `tests/response-sanitizer.test.ts`, `tests/run-manager.test.ts`.
- Modify `README.md`, `docs/parity/matrix.json`, and optionally `docs/parity/sync.md` to document intentional lifecycle behavior.

## Task 1: Stream Error Primitives and Open Error Codes

**Files:**
- Modify: `src/errors.ts`
- Modify: `src/types/server.ts`
- Test: `tests/server-types.test.ts`

- [ ] **Step 1: Write schema tests for open error codes**

Add these tests inside `describe("server request schemas", () => { ... })` in `tests/server-types.test.ts` after the existing error-event parsing tests:

```ts
  test("parses custom stream error codes as open strings", () => {
    expect(
      ThreadStreamEventSchema.parse({
        type: "error",
        code: "rate_limit.exceeded",
        allow_retry: false,
      }),
    ).toEqual({
      type: "error",
      code: "rate_limit.exceeded",
      allow_retry: false,
    });
  });

  test("defaults error code to custom and allow_retry to false", () => {
    expect(ThreadStreamEventSchema.parse({ type: "error" })).toEqual({
      type: "error",
      code: "custom",
      allow_retry: false,
    });
  });
```

In the existing `"parses stream event wire names used by ChatKit clients"` test, replace the final assertion that expects `code: "stream_error"` to throw with:

```ts
    expect(
      ThreadStreamEventSchema.parse({
        type: "error",
        code: "stream_error",
        allow_retry: true,
      }),
    ).toEqual({
      type: "error",
      code: "stream_error",
      allow_retry: true,
    });
```

- [ ] **Step 2: Run schema tests to verify failure**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/server-types.test.js
```

Expected: FAIL because `ErrorEventSchema` rejects `rate_limit.exceeded`.

- [ ] **Step 3: Implement open error schema**

In `src/types/server.ts`, replace the `ErrorEventSchema` definition with:

```ts
export const ErrorEventSchema = z.object({
  type: z.literal("error"),
  code: z.string().default("custom"),
  message: z.string().nullable().optional(),
  allow_retry: z.boolean().default(false),
});
```

- [ ] **Step 4: Add stream error classes**

Replace `src/errors.ts` with this complete file:

```ts
export class ChatKitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class NotFoundError extends ChatKitError {}

export class UnsupportedOperationError extends ChatKitError {}

export class ValidationError extends ChatKitError {
  constructor(
    message: string,
    override readonly cause?: unknown,
  ) {
    super(message);
  }
}

export type StreamErrorCode = "stream.error" | "custom" | (string & {});

export interface StreamErrorOptions {
  allowRetry?: boolean;
}

function defaultAllowRetry(code: StreamErrorCode): boolean {
  return code === "stream.error";
}

export class StreamError extends ChatKitError {
  readonly allowRetry: boolean;

  constructor(
    readonly code: StreamErrorCode,
    options: StreamErrorOptions = {},
  ) {
    super(code);
    this.allowRetry = options.allowRetry ?? defaultAllowRetry(code);
  }
}

export class CustomStreamError extends ChatKitError {
  readonly code = "custom";
  readonly allowRetry: boolean;

  constructor(
    message: string,
    options: StreamErrorOptions = {},
  ) {
    super(message);
    this.allowRetry = options.allowRetry ?? false;
  }
}
```

- [ ] **Step 5: Run schema tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/server-types.test.js
```

Expected: PASS for `server-types.test.js`.

- [ ] **Step 6: Commit**

```bash
git add src/errors.ts src/types/server.ts tests/server-types.test.ts
git commit -m "Add open stream error primitives"
```

## Task 2: Stream Runtime Cancellation Helpers

**Files:**
- Create: `src/stream-runtime.ts`
- Modify: `src/server.ts`
- Modify: `src/index.ts`
- Test: `tests/stream-runtime.test.ts`

- [ ] **Step 1: Write runtime helper tests**

Create `tests/stream-runtime.test.ts`:

```ts
import test from "node:test";
import { strict as assert } from "node:assert";

import {
  StreamCancelledError,
  defaultChatKitStreamRuntime,
  nextWithAbort,
  returnIterator,
  throwIfAborted,
} from "../src/stream-runtime.js";

test("default runtime does not advertise explicit cancellation", () => {
  const runtime = defaultChatKitStreamRuntime();
  assert.equal(runtime.signal.aborted, false);
  assert.equal(runtime.supportsExplicitCancel, false);
});

test("throwIfAborted throws StreamCancelledError when signal is aborted", () => {
  const controller = new AbortController();
  controller.abort();

  assert.throws(() => throwIfAborted(controller.signal), StreamCancelledError);
});

test("nextWithAbort rejects before a never-settling operation resolves", async () => {
  const controller = new AbortController();
  const pending = nextWithAbort(new Promise<IteratorResult<string>>(() => {}), controller.signal);

  controller.abort();

  await assert.rejects(pending, StreamCancelledError);
});

test("nextWithAbort removes abort listener after operation resolves", async () => {
  const controller = new AbortController();
  const signal = controller.signal;
  const originalAdd = signal.addEventListener.bind(signal);
  const originalRemove = signal.removeEventListener.bind(signal);
  let abortListeners = 0;

  signal.addEventListener = ((type, listener, options) => {
    if (type === "abort") abortListeners++;
    return originalAdd(type, listener, options);
  }) as typeof signal.addEventListener;
  signal.removeEventListener = ((type, listener, options) => {
    if (type === "abort") abortListeners--;
    return originalRemove(type, listener, options);
  }) as typeof signal.removeEventListener;

  await nextWithAbort(
    Promise.resolve<IteratorResult<string>>({ done: true, value: undefined }),
    signal,
  );

  assert.equal(abortListeners, 0);
});

test("returnIterator invokes iterator return when available", async () => {
  let returned = false;
  await returnIterator({
    async next() {
      return { done: true, value: undefined };
    },
    async return() {
      returned = true;
      return { done: true, value: undefined };
    },
  });

  assert.equal(returned, true);
});
```

- [ ] **Step 2: Run runtime tests to verify failure**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/stream-runtime.test.js
```

Expected: FAIL because `src/stream-runtime.ts` does not exist.

- [ ] **Step 3: Implement stream runtime helpers**

Create `src/stream-runtime.ts`:

```ts
export interface ChatKitStreamRuntime {
  signal: AbortSignal;
  supportsExplicitCancel: boolean;
}

export interface ChatKitStreamRuntimeOptions {
  signal?: AbortSignal;
  supportsExplicitCancel?: boolean;
}

export class StreamCancelledError extends Error {
  constructor(message = "Stream cancelled") {
    super(message);
    this.name = "StreamCancelledError";
  }
}

export function defaultChatKitStreamRuntime(
  options: ChatKitStreamRuntimeOptions = {},
): ChatKitStreamRuntime {
  return {
    signal: options.signal ?? new AbortController().signal,
    supportsExplicitCancel: options.supportsExplicitCancel ?? false,
  };
}

export function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) {
    throw new StreamCancelledError();
  }
}

export async function nextWithAbort<T>(
  next: Promise<IteratorResult<T>>,
  signal: AbortSignal,
): Promise<IteratorResult<T>> {
  throwIfAborted(signal);
  let cleanup = (): void => {};
  const abort = new Promise<never>((_, reject) => {
    const rejectCancelled = () => reject(new StreamCancelledError());
    signal.addEventListener("abort", rejectCancelled, { once: true });
    cleanup = () => signal.removeEventListener("abort", rejectCancelled);
  });

  try {
    return await Promise.race([next, abort]);
  } finally {
    cleanup();
  }
}

export async function returnIterator<T>(iterator: AsyncIterator<T>): Promise<void> {
  await iterator.return?.();
}
```

- [ ] **Step 4: Re-export runtime helpers**

Update `src/index.ts` to include:

```ts
export * from "./stream-runtime.js";
```

In `src/server.ts`, replace the local `StreamCancelledError` class with an import and export:

```ts
import {
  StreamCancelledError,
  defaultChatKitStreamRuntime,
  type ChatKitStreamRuntime,
} from "./stream-runtime.js";

export { StreamCancelledError } from "./stream-runtime.js";
export type { ChatKitStreamRuntime } from "./stream-runtime.js";
```

- [ ] **Step 5: Run runtime tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/stream-runtime.test.js
```

Expected: PASS for `stream-runtime.test.js`.

- [ ] **Step 6: Commit**

```bash
git add src/stream-runtime.ts src/server.ts src/index.ts tests/stream-runtime.test.ts
git commit -m "Add stream runtime cancellation helpers"
```

## Task 3: Client-Safe Response Sanitizer

**Files:**
- Create: `src/response-sanitizer.ts`
- Modify: `src/index.ts`
- Test: `tests/response-sanitizer.test.ts`

- [ ] **Step 1: Write sanitizer tests**

Create `tests/response-sanitizer.test.ts`:

```ts
import test from "node:test";
import { strict as assert } from "node:assert";

import {
  sanitizeClientPayload,
  sanitizeThreadItem,
  sanitizeThreadStreamEvent,
} from "../src/response-sanitizer.js";
import type { Attachment, Thread, ThreadItem, ThreadStreamEvent } from "../src/index.js";

const attachment: Attachment = {
  id: "atc_secret",
  type: "file",
  name: "secret.txt",
  mime_type: "text/plain",
  metadata: { source: "internal" },
};

const userMessage: Extract<ThreadItem, { type: "user_message" }> = {
  id: "msg_user",
  type: "user_message",
  thread_id: "thr_test",
  created_at: "2026-06-20T00:00:00.000Z",
  content: [{ type: "input_text", text: "hello" }],
  attachments: [attachment],
  inference_options: {},
};

const threadResponse: Thread = {
  id: "thr_test",
  created_at: "2026-06-20T00:00:00.000Z",
  status: { type: "active" },
  metadata: {},
  items: { data: [userMessage], has_more: false, after: null },
};

test("sanitizeThreadItem strips attachment metadata without mutating source", () => {
  const sanitized = sanitizeThreadItem(userMessage);

  assert.equal(sanitized.type, "user_message");
  if (sanitized.type !== "user_message") {
    throw new Error("Expected user message");
  }

  assert.deepEqual(sanitized.attachments[0], {
    id: "atc_secret",
    type: "file",
    name: "secret.txt",
    mime_type: "text/plain",
  });
  assert.deepEqual(userMessage.attachments[0]?.metadata, { source: "internal" });
});

test("sanitizeThreadStreamEvent strips metadata from item-bearing events", () => {
  const event: ThreadStreamEvent = { type: "thread.item.done", item: userMessage };
  const sanitized = sanitizeThreadStreamEvent(event);

  assert.equal(sanitized.type, "thread.item.done");
  if (sanitized.type !== "thread.item.done" || sanitized.item.type !== "user_message") {
    throw new Error("Expected sanitized user message event");
  }

  assert.equal("metadata" in sanitized.item.attachments[0]!, false);
});

test("sanitizeThreadStreamEvent strips metadata from replaced item events", () => {
  const event: ThreadStreamEvent = { type: "thread.item.replaced", item: userMessage };
  const sanitized = sanitizeThreadStreamEvent(event);

  assert.equal(sanitized.type, "thread.item.replaced");
  if (sanitized.type !== "thread.item.replaced" || sanitized.item.type !== "user_message") {
    throw new Error("Expected sanitized replaced user message event");
  }

  assert.equal("metadata" in sanitized.item.attachments[0]!, false);
});

test("sanitizeThreadStreamEvent strips metadata from thread response events", () => {
  const event: ThreadStreamEvent = { type: "thread.created", thread: threadResponse };
  const sanitized = sanitizeThreadStreamEvent(event);

  assert.equal(sanitized.type, "thread.created");
  const item = sanitized.thread.items.data[0];
  if (!item || item.type !== "user_message") {
    throw new Error("Expected sanitized thread response");
  }

  assert.equal("metadata" in item.attachments[0]!, false);
  assert.deepEqual(threadResponse.items.data[0]!.attachments[0]!.metadata, { source: "internal" });
});

test("sanitizeClientPayload strips metadata from paginated item responses", () => {
  const sanitized = sanitizeClientPayload({
    data: [userMessage],
    has_more: false,
    after: null,
  });

  assert.deepEqual(sanitized, {
    data: [
      {
        ...userMessage,
        attachments: [
          {
            id: "atc_secret",
            type: "file",
            name: "secret.txt",
            mime_type: "text/plain",
          },
        ],
      },
    ],
    has_more: false,
    after: null,
  });
});
```

- [ ] **Step 2: Run sanitizer tests to verify failure**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/response-sanitizer.test.js
```

Expected: FAIL because `src/response-sanitizer.ts` does not exist.

- [ ] **Step 3: Implement sanitizer**

Create `src/response-sanitizer.ts`:

```ts
import type { Attachment, Page, ThreadItem } from "./types/core.js";
import type { Thread, ThreadStreamEvent } from "./types/server.js";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPage(value: unknown): value is Page<unknown> {
  return (
    isRecord(value) &&
    Array.isArray(value.data) &&
    typeof value.has_more === "boolean" &&
    ("after" in value)
  );
}

function isAttachment(value: unknown): value is Attachment {
  return isRecord(value) && typeof value.id === "string" && typeof value.mime_type === "string";
}

export function sanitizeAttachment<TAttachment extends Attachment>(attachment: TAttachment): TAttachment {
  const { metadata: _metadata, ...rest } = attachment;
  return rest as TAttachment;
}

export function sanitizeThreadItem<TItem extends ThreadItem>(item: TItem): TItem {
  if (item.type !== "user_message") {
    return structuredClone(item);
  }

  return {
    ...structuredClone(item),
    attachments: item.attachments.map((attachment) => sanitizeAttachment(attachment)),
  } as TItem;
}

export function sanitizeThreadResponse<TThread extends Thread>(thread: TThread): TThread {
  return {
    ...structuredClone(thread),
    items: sanitizePage(thread.items, sanitizeThreadItem),
  };
}

export function sanitizePage<T>(page: Page<T>, sanitizeData: (value: T) => T): Page<T> {
  return {
    ...page,
    data: page.data.map((item) => sanitizeData(item)),
  };
}

export function sanitizeThreadStreamEvent<TEvent extends ThreadStreamEvent>(event: TEvent): TEvent {
  if (event.type === "thread.item.added" || event.type === "thread.item.done" || event.type === "thread.item.replaced") {
    return { ...event, item: sanitizeThreadItem(event.item) } as TEvent;
  }

  if (event.type === "thread.created" || event.type === "thread.updated") {
    return { ...event, thread: sanitizeThreadResponse(event.thread) } as TEvent;
  }

  return structuredClone(event);
}

export function sanitizeClientPayload<T>(value: T): T {
  if (isAttachment(value)) {
    return sanitizeAttachment(value) as T;
  }

  if (isPage(value)) {
    return {
      ...value,
      data: value.data.map((item) => sanitizeClientPayload(item)),
    } as T;
  }

  if (isRecord(value) && value.type === "user_message") {
    return sanitizeThreadItem(value as ThreadItem) as T;
  }

  if (isRecord(value) && "items" in value && isPage(value.items)) {
    return sanitizeThreadResponse(value as Thread) as T;
  }

  if (isRecord(value) && typeof value.type === "string") {
    return sanitizeThreadStreamEvent(value as ThreadStreamEvent) as T;
  }

  return structuredClone(value);
}
```

- [ ] **Step 4: Re-export sanitizer**

Update `src/index.ts` to include:

```ts
export * from "./response-sanitizer.js";
```

- [ ] **Step 5: Run sanitizer tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/response-sanitizer.test.js
```

Expected: PASS for `response-sanitizer.test.js`.

- [ ] **Step 6: Commit**

```bash
git add src/response-sanitizer.ts src/index.ts tests/response-sanitizer.test.ts
git commit -m "Add client-safe response sanitizer"
```

## Task 4: Server Runtime, Event-Level API, Sanitized Serialization, and Error Mapping

**Files:**
- Modify: `src/server.ts`
- Test: `tests/server.test.ts`

- [ ] **Step 1: Add server tests for stream errors and sanitized responses**

In `tests/server.test.ts`, update the import from `../src/server.js` to include `StreamingEventResult` after it exists in the implementation step. Add this import from `../src/errors.js`:

```ts
import { CustomStreamError, StreamError } from "../src/errors.js";
```

Add these tests inside `describe("ChatKitServer", () => { ... })` after `"streams a retryable stream error when the responder throws"`:

```ts
  test("streams custom stream errors with message and retry policy", async () => {
    const server = new TestServer(async function* () {
      throw new CustomStreamError("The user-facing failure", { allowRetry: false });
    });

    const result = (await server.process(
      JSON.stringify({
        type: "threads.create",
        params: {
          input: {
            content: [{ type: "input_text", text: "Start" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
    )) as StreamingResult;

    const events = await decodeStream(result);
    expect(events.at(-1)).toEqual({
      type: "error",
      code: "custom",
      message: "The user-facing failure",
      allow_retry: false,
    });
  });

  test("streams typed stream errors with default retry policy", async () => {
    const server = new TestServer(async function* () {
      throw new StreamError("rate_limit.exceeded");
    });

    const result = (await server.process(
      JSON.stringify({
        type: "threads.create",
        params: {
          input: {
            content: [{ type: "input_text", text: "Start" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
    )) as StreamingResult;

    const events = await decodeStream(result);
    expect(events.at(-1)).toEqual({
      type: "error",
      code: "rate_limit.exceeded",
      allow_retry: false,
    });
  });

  test("streams stream.error as retryable by default", async () => {
    const server = new TestServer(async function* () {
      throw new StreamError("stream.error");
    });

    const result = (await server.process(
      JSON.stringify({
        type: "threads.create",
        params: {
          input: {
            content: [{ type: "input_text", text: "Start" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
    )) as StreamingResult;

    const events = await decodeStream(result);
    expect(events.at(-1)).toEqual({
      type: "error",
      code: "stream.error",
      allow_retry: true,
    });
  });

  test("streams typed stream errors with retry override", async () => {
    const server = new TestServer(async function* () {
      throw new StreamError("stream.error", { allowRetry: false });
    });

    const result = (await server.process(
      JSON.stringify({
        type: "threads.create",
        params: {
          input: {
            content: [{ type: "input_text", text: "Start" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
    )) as StreamingResult;

    const events = await decodeStream(result);
    expect(events.at(-1)).toEqual({
      type: "error",
      code: "stream.error",
      allow_retry: false,
    });
  });
```

Add this assertion to the existing `"creates a thread, persists the user message, and streams responder events"` test after the `userMessageEvent` match:

```ts
    if (userMessageEvent.type !== "thread.item.done" || userMessageEvent.item.type !== "user_message") {
      throw new Error("Expected streamed user message");
    }
    expect(userMessageEvent.item.attachments[0]).not.toHaveProperty("metadata");
```

Update the existing `"creates and persists an attachment"` test so the decoded response omits metadata but the stored attachment keeps it:

```ts
    const attachment = decodeJson(result) as Attachment;
    expect(attachment).toEqual({
      id: "atc_1",
      type: "image",
      name: "image.png",
      mime_type: "image/png",
      preview_url: "https://example.com/preview.png",
      upload_descriptor: { url: "https://example.com/upload", method: "PUT", headers: {} },
    });
    await expect(server.store.loadAttachment("atc_1", defaultContext)).resolves.toMatchObject({
      id: "atc_1",
      metadata: { source: "test" },
    });
```

Add this test near the existing `items.list` and `threads.get_by_id` tests:

```ts
  test("strips attachment metadata from item list and thread get responses without mutating store", async () => {
    const server = new TestServer();
    const thread = makeThread("thr_metadata_sanitize");
    const attachment: Attachment = {
      id: "atc_metadata_sanitize",
      type: "file",
      name: "notes.txt",
      mime_type: "text/plain",
      metadata: { source: "internal" },
    };
    const userMessage: UserMessageItem = {
      ...makeUserMessage("msg_metadata_sanitize", thread.id),
      attachments: [{ ...attachment, thread_id: thread.id }],
    };
    await server.store.saveThread(thread, defaultContext);
    await server.store.saveAttachment(attachment, defaultContext);
    await server.store.addThreadItem(thread.id, userMessage, defaultContext);

    const itemList = (await server.process(
      JSON.stringify({
        type: "items.list",
        params: { thread_id: thread.id, limit: 10, order: "asc" },
        metadata: {},
      }),
      defaultContext,
    )) as NonStreamingResult;
    const listedItem = (decodeJson(itemList) as { data: ThreadItem[] }).data[0] as UserMessageItem;
    expect(listedItem.attachments[0]).not.toHaveProperty("metadata");

    const threadGet = (await server.process(
      JSON.stringify({
        type: "threads.get_by_id",
        params: { thread_id: thread.id },
        metadata: {},
      }),
      defaultContext,
    )) as NonStreamingResult;
    const responseItem = (decodeJson(threadGet) as { items: { data: ThreadItem[] } }).items
      .data[0] as UserMessageItem;
    expect(responseItem.attachments[0]).not.toHaveProperty("metadata");

    await expect(server.store.loadAttachment(attachment.id, defaultContext)).resolves.toMatchObject({
      metadata: { source: "internal" },
    });
  });
```

Add this stream-event sanitizer assertion after the streamed user-message metadata assertion:

```ts
  test("strips attachment metadata from streamed replacement events", async () => {
    const attachment: Attachment = {
      id: "atc_replace_sanitize",
      type: "file",
      name: "replace.txt",
      mime_type: "text/plain",
      metadata: { source: "internal" },
    };
    const server = new TestServer(async function* (thread) {
      yield {
        type: "thread.item.replaced",
        item: {
          ...makeUserMessage("msg_replace_sanitize", thread.id),
          attachments: [{ ...attachment, thread_id: thread.id }],
        },
      };
    });

    const result = (await server.process(
      JSON.stringify({
        type: "threads.create",
        params: {
          input: {
            content: [{ type: "input_text", text: "Start" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
    )) as StreamingResult;
    const events = await decodeStream(result);
    const replaced = events.find((event) => event.type === "thread.item.replaced");
    if (!replaced || replaced.type !== "thread.item.replaced" || replaced.item.type !== "user_message") {
      throw new Error("Expected replacement event");
    }

    expect(replaced.item.attachments[0]).not.toHaveProperty("metadata");
  });
```

- [ ] **Step 2: Run server tests to verify failure**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/server.test.js
```

Expected: FAIL because `processEvents(...)` does not map `StreamError`/`CustomStreamError`, and responses still include attachment metadata.

- [ ] **Step 3: Implement event-level result and runtime plumbing**

In `src/server.ts`, update imports:

```ts
import {
  CustomStreamError,
  NotFoundError,
  StreamError,
  UnsupportedOperationError,
  ValidationError,
} from "./errors.js";
import { sanitizeClientPayload, sanitizeThreadStreamEvent } from "./response-sanitizer.js";
import {
  StreamCancelledError,
  defaultChatKitStreamRuntime,
  type ChatKitStreamRuntime,
} from "./stream-runtime.js";
```

Add after `NonStreamingResult`:

```ts
export class StreamingEventResult {
  constructor(
    private readonly createEvents: (runtime: ChatKitStreamRuntime) => AsyncIterable<ThreadStreamEvent>,
  ) {}

  stream(runtime: ChatKitStreamRuntime = defaultChatKitStreamRuntime()): AsyncIterable<ThreadStreamEvent> {
    return this.createEvents(runtime);
  }
}

export interface ChatKitProcessOptions {
  runtime?: ChatKitStreamRuntime;
}
```

Change `respond`, `action`, and `getStreamOptions` signatures:

```ts
  abstract respond(
    thread: ThreadMetadata,
    inputUserMessage: UserMessageItem | null,
    context: TContext,
    runtime: ChatKitStreamRuntime,
  ): AsyncIterable<ThreadStreamEvent>;

  async *action(
    _thread: ThreadMetadata,
    _action: ThreadCustomActionParams["action"],
    _sender: WidgetItem | null,
    _context: TContext,
    _runtime: ChatKitStreamRuntime,
  ): AsyncIterable<ThreadStreamEvent> {
    throw new UnsupportedOperationError(
      "The action() method must be overridden to react to actions.",
    );
  }

  getStreamOptions(
    _thread: ThreadMetadata,
    _context: TContext,
    runtime: ChatKitStreamRuntime,
  ): StreamOptions {
    return { allow_cancel: runtime.supportsExplicitCancel };
  }
```

Add a new public parser/result method before `process(...)`:

```ts
  async processRequest(
    request: ProcessRequestInput,
    context: TContext,
  ): Promise<StreamingEventResult | NonStreamingResult> {
    const parsed: ChatKitRequest = ChatKitRequestSchema.parse(decodeJsonBytes(request));

    if (isStreamingRequest(parsed)) {
      return new StreamingEventResult((runtime) =>
        this.processStreamingEvents(parsed, context, runtime),
      );
    }

    return new NonStreamingResult(await this.processNonStreaming(parsed, context));
  }
```

Change `process(...)` to reuse it:

```ts
  async process(
    request: ProcessRequestInput,
    context: TContext,
    options: ChatKitProcessOptions = {},
  ): Promise<StreamingResult | NonStreamingResult> {
    const result = await this.processRequest(request, context);

    if (result instanceof NonStreamingResult) {
      return result;
    }

    const runtime = options.runtime ?? defaultChatKitStreamRuntime();
    return new StreamingResult(this.serializeStreamingEvents(result.stream(runtime)));
  }
```

Replace `processStreaming(...)` with:

```ts
  protected async *serializeStreamingEvents(
    events: AsyncIterable<ThreadStreamEvent>,
  ): AsyncIterable<Uint8Array> {
    for await (const event of events) {
      const json = sseDecoder.decode(this.serialize(event));
      yield sseEncoder.encode(`data: ${json}\n\n`);
    }
  }
```

Rename `processStreamingImpl(...)` to `processStreamingEvents(...)` and add the runtime parameter:

```ts
  protected async *processStreamingEvents(
    request: StreamingRequest,
    context: TContext,
    runtime: ChatKitStreamRuntime,
  ): AsyncIterable<ThreadStreamEvent> {
```

Inside each call site, pass `runtime` into `processEvents`, `respond`, and `action`.
Change the `processEvents(...)` signature to accept the runtime before the stream factory:

```ts
  protected async *processEvents(
    thread: ThreadMetadata,
    context: TContext,
    runtime: ChatKitStreamRuntime,
    stream: () => AsyncIterable<ThreadStreamEvent>,
  ): AsyncIterable<ThreadStreamEvent> {
```

Update every test helper override of `respond(...)`, every `Responder` type, every `action(...)` override, and every `ActionResponder` type in `tests/server.test.ts` so they include the new trailing `_runtime: ChatKitStreamRuntime` parameter.

- [ ] **Step 4: Implement sanitized serialization and error mapping**

Change `handleStreamCancelled(...)` hidden context content:

```ts
        content: "The user cancelled the stream. Stop responding to the prior request.",
```

Change `serialize(...)`:

```ts
  protected serialize(value: unknown): Uint8Array {
    return encodeJsonBytes(sanitizeClientPayload(value));
  }
```

At the top of `processEvents(...)`, use runtime-aware options:

```ts
    const streamOptions = this.getStreamOptions(thread, context, runtime);
    yield {
      type: "stream_options",
      stream_options: {
        ...streamOptions,
        allow_cancel: runtime.supportsExplicitCancel && streamOptions.allow_cancel,
      },
    };
```

Before yielding any non-suppressed event in `processEvents(...)`, sanitize it:

```ts
        if (!suppressClientEvent) {
          yield sanitizeThreadStreamEvent(event);
        }
```

When yielding `thread.updated`, sanitize by relying on `toThreadResponse(...)` and the `processEvents(...)` event sanitizer:

```ts
          yield sanitizeThreadStreamEvent({ type: "thread.updated", thread: this.toThreadResponse(thread) });
```

In the `catch` block, insert stream error handling before generic errors:

```ts
      if (error instanceof CustomStreamError) {
        completedNormally = true;
        yield {
          type: "error",
          code: "custom",
          message: error.message,
          allow_retry: error.allowRetry,
        };
        return;
      }

      if (error instanceof StreamError) {
        completedNormally = true;
        yield {
          type: "error",
          code: error.code,
          allow_retry: error.allowRetry,
        };
        return;
      }
```

- [ ] **Step 5: Run server tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/server.test.js
```

Expected: PASS for `server.test.js`.

- [ ] **Step 6: Run typecheck**

Run:

```bash
npm run typecheck
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/server.ts tests/server.test.ts
git commit -m "Add server runtime and stream error parity"
```

## Task 5: Agent Stream Explicit Cancellation

**Files:**
- Modify: `src/agents/types.ts`
- Modify: `src/agents/stream.ts`
- Test: `tests/agents.test.ts`

- [ ] **Step 1: Add cancellation test for never-settling SDK stream**

Add this test inside `describe("streamAgentResponse", () => { ... })` near the existing cancellation test:

```ts
  test("explicit cancellation stops a never-settling SDK stream", async () => {
    const agentContext = makeAgentContext();
    const controller = new AbortController();
    let returned = false;
    const stream = {
      [Symbol.asyncIterator](): AsyncIterator<unknown> {
        return {
          next: () => new Promise<IteratorResult<unknown>>(() => {}),
          async return() {
            returned = true;
            return { done: true, value: undefined };
          },
        };
      },
    };

    const iterator = streamAgentResponse(agentContext, stream, {
      signal: controller.signal,
    })[Symbol.asyncIterator]();

    const next = iterator.next();
    controller.abort();

    await expect(next).rejects.toBeInstanceOf(StreamCancelledError);
    expect(returned).toBe(true);
    expect(() => agentContext.stream({ type: "progress_update", text: "late" })).toThrow(
      "Cannot stream events after the agent context has completed.",
    );
  });
```

Add this import at the top of `tests/agents.test.ts`:

```ts
import { StreamCancelledError } from "../src/stream-runtime.js";
```

- [ ] **Step 2: Run agent test to verify failure**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/agents.test.js
```

Expected: FAIL because `StreamAgentResponseOptions` has no `signal` and the stream loop does not race abort.

- [ ] **Step 3: Add signal option type**

Update `src/agents/types.ts`:

```ts
export interface StreamAgentResponseOptions {
  converter?: ResponseStreamConverter;
  signal?: AbortSignal;
}
```

- [ ] **Step 4: Wire abort into `streamAgentResponse(...)`**

In `src/agents/stream.ts`, add:

```ts
import { nextWithAbort, returnIterator as returnIteratorWithAbort } from "../stream-runtime.js";
```

Remove or rename the local `returnIterator(...)` helper if it conflicts. In `streamAgentResponse(...)`, after the converter:

```ts
  const signal = options.signal ?? new AbortController().signal;
```

Change tagged `next()` calls to use `nextWithAbort(...)`:

```ts
  let sdkNext = tagNext("sdk", nextWithAbort(sdkIterator.next(), signal));
  let contextNext = tagNext("context", nextWithAbort(contextIterator.next(), signal));
```

Every time the code advances an iterator, use the same helper:

```ts
          contextNext = tagNext("context", nextWithAbort(contextIterator.next(), signal));
```

```ts
      sdkNext = tagNext("sdk", nextWithAbort(sdkIterator.next(), signal));
```

In `finally`, call the runtime cleanup helper:

```ts
    context.closeEvents();
    await returnIteratorWithAbort(sdkIterator);
    await returnIteratorWithAbort(contextIterator);
```

- [ ] **Step 5: Run agent tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/agents.test.js
```

Expected: PASS for `agents.test.js`.

- [ ] **Step 6: Commit**

```bash
git add src/agents/types.ts src/agents/stream.ts tests/agents.test.ts
git commit -m "Support explicit cancellation in agent streams"
```

## Task 6: ResponseRunManager

**Files:**
- Create: `src/run-manager.ts`
- Modify: `src/index.ts`
- Test: `tests/run-manager.test.ts`

- [ ] **Step 1: Write run manager tests**

Create `tests/run-manager.test.ts`:

```ts
import test from "node:test";
import { strict as assert } from "node:assert";

import { ResponseRunManager } from "../src/run-manager.js";
import { StreamCancelledError } from "../src/stream-runtime.js";

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) {
    values.push(value);
  }
  return values;
}

test("subscriber cancellation detaches without stopping the run", async () => {
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const { run, subscription: subscriber } = await manager.startRunAndSubscribe({
    context: { userId: "user_1" },
    source: async function* () {
      yield "first";
      await released;
      yield "second";
    },
  });

  const iterator = subscriber.events[Symbol.asyncIterator]();

  assert.deepEqual(await iterator.next(), { done: false, value: "first" });
  await iterator.return?.();
  release();
  await run.completed;

  assert.equal(run.status, "completed");
});

test("initial subscriber receives synchronously yielded events", async () => {
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const { run, subscription } = await manager.startRunAndSubscribe({
    context: { userId: "user_1" },
    source: async function* () {
      yield "thread.created";
      yield "thread.item.done";
    },
  });

  assert.deepEqual(await collect(subscription.events), ["thread.created", "thread.item.done"]);
  await run.completed;
});

test("new subscribers can attach by run id and receive later events", async () => {
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const { run, subscription: first } = await manager.startRunAndSubscribe({
    context: { userId: "user_1" },
    source: async function* () {
      yield "first";
      await released;
      yield "second";
    },
  });

  const firstIterator = first.events[Symbol.asyncIterator]();
  assert.deepEqual(await firstIterator.next(), { done: false, value: "first" });
  await firstIterator.return?.();

  const second = await manager.subscribe({ runId: run.runId, context: { userId: "user_1" } });
  release();

  assert.deepEqual(await collect(second.events), ["second"]);
});

test("simultaneous runs require their own run ids", async () => {
  let releaseFirst!: () => void;
  let releaseSecond!: () => void;
  const firstReleased = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const secondReleased = new Promise<void>((resolve) => {
    releaseSecond = resolve;
  });
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const firstRun = await manager.startRun({
    context: { userId: "user_1" },
    source: async function* () {
      await firstReleased;
      yield "first-run";
    },
  });
  const secondRun = await manager.startRun({
    context: { userId: "user_1" },
    source: async function* () {
      await secondReleased;
      yield "second-run";
    },
  });

  const first = await manager.subscribe({ runId: firstRun.runId, context: { userId: "user_1" } });
  const second = await manager.subscribe({ runId: secondRun.runId, context: { userId: "user_1" } });
  releaseFirst();
  releaseSecond();

  assert.deepEqual(await collect(first.events), ["first-run"]);
  assert.deepEqual(await collect(second.events), ["second-run"]);
  assert.notEqual(firstRun.runId, secondRun.runId);
});

test("cancelRun is scoped and idempotent", async () => {
  let aborted = false;
  let returned = false;
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const run = await manager.startRun({
    context: { userId: "user_1" },
    source: ({ signal }) => ({
      [Symbol.asyncIterator](): AsyncIterator<string> {
        signal.addEventListener("abort", () => {
          aborted = true;
        });

        return {
          async next() {
            await new Promise<void>(() => {});
            return { done: true, value: undefined };
          },
          async return() {
            returned = true;
            throw new StreamCancelledError();
          },
        };
      },
    }),
  });

  assert.deepEqual(
    await manager.cancelRun({ runId: run.runId, context: { userId: "user_2" } }),
    { status: "forbidden" },
  );
  assert.equal(aborted, false);

  const result = await manager.cancelRun({ runId: run.runId, context: { userId: "user_1" } });
  assert.deepEqual(result, { status: "cancelled" });
  assert.equal(aborted, true);
  assert.equal(returned, true);

  assert.deepEqual(
    await manager.cancelRun({ runId: "missing", context: { userId: "user_1" } }),
    { status: "not_found" },
  );
});
```

- [ ] **Step 2: Run run manager tests to verify failure**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/run-manager.test.js
```

Expected: FAIL because `src/run-manager.ts` does not exist.

- [ ] **Step 3: Implement `ResponseRunManager`**

Create `src/run-manager.ts`:

```ts
import {
  StreamCancelledError,
  nextWithAbort,
  returnIterator,
  type ChatKitStreamRuntime,
} from "./stream-runtime.js";

export type DisconnectBehavior = "continue" | "cancel";

export type CancelRunResult =
  | { status: "cancelled" }
  | { status: "cancelling" }
  | { status: "already_finished" }
  | { status: "not_found" }
  | { status: "forbidden" };

export interface ResponseRunManagerOptions<TContext> {
  getRunScope?: (context: TContext) => string | Promise<string>;
}

export interface StartRunOptions<TContext, TEvent> {
  context: TContext;
  source: (runtime: ChatKitStreamRuntime) => AsyncIterable<TEvent>;
  supportsExplicitCancel?: boolean;
}

export interface SubscribeRunOptions<TContext> {
  runId: string;
  context: TContext;
}

export interface CancelRunOptions<TContext> {
  runId: string;
  context: TContext;
}

class AsyncEventQueue<T> implements AsyncIterable<T> {
  private readonly values: T[] = [];
  private readonly waiting: Array<(result: IteratorResult<T>) => void> = [];
  private closed = false;

  push(value: T): void {
    if (this.closed) return;
    const resolve = this.waiting.shift();
    if (resolve) {
      resolve({ done: false, value });
      return;
    }
    this.values.push(value);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const resolve of this.waiting.splice(0)) {
      resolve({ done: true, value: undefined });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        const value = this.values.shift();
        if (value !== undefined) return { done: false, value };
        if (this.closed) return { done: true, value: undefined };
        return new Promise((resolve) => this.waiting.push(resolve));
      },
      return: async () => {
        this.close();
        return { done: true, value: undefined };
      },
    };
  }
}

type RunStatus = "running" | "completed" | "failed" | "cancelled";

interface ResponseRunRecord<TContext, TEvent> {
  runId: string;
  scope: string | null;
  controller: AbortController;
  status: RunStatus;
  sourceIterator?: AsyncIterator<TEvent>;
  subscribers: Set<AsyncEventQueue<TEvent>>;
  completed: Promise<void>;
  resolveCompleted: () => void;
  rejectCompleted: (error: unknown) => void;
}

export interface ResponseRunHandle {
  runId: string;
  readonly status: RunStatus;
  completed: Promise<void>;
}

export interface ResponseRunSubscription<TEvent> {
  events: AsyncIterable<TEvent>;
}

export interface ResponseRunStart<TEvent> {
  run: ResponseRunHandle;
  subscription: ResponseRunSubscription<TEvent>;
}

export class ResponseRunManager<TContext = unknown, TEvent = Uint8Array> {
  private readonly runs = new Map<string, ResponseRunRecord<TContext, TEvent>>();

  constructor(private readonly options: ResponseRunManagerOptions<TContext> = {}) {}

  async startRun(options: StartRunOptions<TContext, TEvent>): Promise<ResponseRunHandle> {
    return (await this.startRunInternal(options, null)).run;
  }

  async startRunAndSubscribe(
    options: StartRunOptions<TContext, TEvent>,
  ): Promise<ResponseRunStart<TEvent>> {
    return this.startRunInternal(options, new AsyncEventQueue<TEvent>());
  }

  private async startRunInternal(
    options: StartRunOptions<TContext, TEvent>,
    initialSubscriber: AsyncEventQueue<TEvent> | null,
  ): Promise<ResponseRunStart<TEvent>> {
    const runId = `run_${crypto.randomUUID().replaceAll("-", "")}`;
    const controller = new AbortController();
    const scope = await this.scopeFor(options.context);
    let resolveCompleted!: () => void;
    let rejectCompleted!: (error: unknown) => void;
    const completed = new Promise<void>((resolve, reject) => {
      resolveCompleted = resolve;
      rejectCompleted = reject;
    });
    const record: ResponseRunRecord<TContext, TEvent> = {
      runId,
      scope,
      controller,
      status: "running",
      subscribers: initialSubscriber ? new Set([initialSubscriber]) : new Set(),
      completed,
      resolveCompleted,
      rejectCompleted,
    };

    void completed.catch(() => {});
    this.runs.set(runId, record);
    void this.drainRun(record, options.source, options.supportsExplicitCancel ?? true);

    const run: ResponseRunHandle = {
      runId,
      get status() {
        return record.status;
      },
      completed,
    };

    return {
      run,
      subscription: initialSubscriber
        ? this.subscriptionFor(record, initialSubscriber)
        : { events: this.closedIterable() },
    };
  }

  async subscribe(options: SubscribeRunOptions<TContext>): Promise<ResponseRunSubscription<TEvent>> {
    const record = this.runs.get(options.runId);
    if (!record || record.status !== "running") {
      return { events: this.closedIterable() };
    }

    if (!(await this.canAccess(record, options.context))) {
      return { events: this.closedIterable() };
    }

    const queue = new AsyncEventQueue<TEvent>();
    record.subscribers.add(queue);

    return this.subscriptionFor(record, queue);
  }

  private subscriptionFor(
    record: ResponseRunRecord<TContext, TEvent>,
    queue: AsyncEventQueue<TEvent>,
  ): ResponseRunSubscription<TEvent> {
    return {
      events: {
        [Symbol.asyncIterator]: () => {
          const iterator = queue[Symbol.asyncIterator]();
          return {
            next: () => iterator.next(),
            return: async () => {
              record.subscribers.delete(queue);
              await iterator.return?.();
              return { done: true, value: undefined };
            },
          };
        },
      },
    };
  }

  async cancelRun(options: CancelRunOptions<TContext>): Promise<CancelRunResult> {
    const record = this.runs.get(options.runId);
    if (!record) return { status: "not_found" };
    if (!(await this.canAccess(record, options.context))) return { status: "forbidden" };
    if (record.status === "completed" || record.status === "failed") {
      return { status: "already_finished" };
    }
    if (record.status === "cancelled") return { status: "cancelled" };

    record.status = "cancelled";
    record.controller.abort();
    this.closeSubscribers(record);
    await record.completed.catch(() => undefined);
    return { status: "cancelled" };
  }

  private async drainRun(
    record: ResponseRunRecord<TContext, TEvent>,
    source: (runtime: ChatKitStreamRuntime) => AsyncIterable<TEvent>,
    supportsExplicitCancel: boolean,
  ): Promise<void> {
    let terminalStatus: RunStatus = "completed";
    let terminalError: unknown;

    try {
      const runtime: ChatKitStreamRuntime = {
        signal: record.controller.signal,
        supportsExplicitCancel,
      };
      const iterator = source(runtime)[Symbol.asyncIterator]();
      record.sourceIterator = iterator;

      while (true) {
        const next = await nextWithAbort(iterator.next(), record.controller.signal);
        if (next.done) break;

        const event = next.value;
        for (const subscriber of record.subscribers) {
          subscriber.push(event);
        }
      }
      terminalStatus = record.status === "cancelled" ? "cancelled" : "completed";
    } catch (error) {
      if (error instanceof StreamCancelledError || record.controller.signal.aborted) {
        terminalStatus = "cancelled";
      } else {
        terminalStatus = "failed";
        terminalError = error;
      }
    } finally {
      if (record.sourceIterator && terminalStatus !== "completed") {
        try {
          await returnIterator(record.sourceIterator);
        } catch (error) {
          if (!(error instanceof StreamCancelledError || record.controller.signal.aborted)) {
            terminalStatus = "failed";
            terminalError = error;
          }
        }
      }

      record.sourceIterator = undefined;
      record.status = terminalStatus;
      this.closeSubscribers(record);
      this.runs.delete(record.runId);

      if (terminalStatus === "failed") {
        record.rejectCompleted(terminalError);
      } else {
        record.resolveCompleted();
      }
    }
  }

  private closeSubscribers(record: ResponseRunRecord<TContext, TEvent>): void {
    for (const subscriber of record.subscribers) {
      subscriber.close();
    }
    record.subscribers.clear();
  }

  private closedIterable(): AsyncIterable<TEvent> {
    return {
      [Symbol.asyncIterator](): AsyncIterator<TEvent> {
        return {
          async next() {
            return { done: true, value: undefined };
          },
        };
      },
    };
  }

  private async scopeFor(context: TContext): Promise<string | null> {
    return this.options.getRunScope ? await this.options.getRunScope(context) : null;
  }

  private async canAccess(
    record: ResponseRunRecord<TContext, TEvent>,
    context: TContext,
  ): Promise<boolean> {
    if (record.scope == null) return true;
    return record.scope === (await this.scopeFor(context));
  }
}
```

- [ ] **Step 4: Re-export run manager**

Update `src/index.ts`:

```ts
export * from "./run-manager.js";
```

- [ ] **Step 5: Run run manager tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/run-manager.test.js
```

Expected: PASS for `run-manager.test.js`.

- [ ] **Step 6: Commit**

```bash
git add src/run-manager.ts src/index.ts tests/run-manager.test.ts
git commit -m "Add in-process response run manager"
```

## Task 7: HTTP Handler Disconnect Semantics and Run Subscription

**Files:**
- Modify: `src/http.ts`
- Test: `tests/http.test.ts`

- [ ] **Step 1: Write HTTP lifecycle tests**

Update imports in `tests/http.test.ts`:

```ts
import { ResponseRunManager } from "../src/run-manager.js";
import { ChatKitServer, NonStreamingResult, StreamingEventResult } from "../src/server.js";
import { SQLiteStore } from "../src/sqlite-store.js";
import type { ChatKitStreamRuntime } from "../src/stream-runtime.js";
import type { Page, ThreadItem, ThreadMetadata } from "../src/types/core.js";
import type { Thread, ThreadStreamEvent } from "../src/types/server.js";
```

Update `RecordingServer` so the handler tests exercise `processRequest(...)`, not the legacy byte-stream `process(...)` path:

```ts
class RecordingServer extends ChatKitServer<RequestContext | undefined> {
  readonly calls: Array<{
    body: string;
    context: RequestContext | undefined;
    receivedArrayBuffer: boolean;
  }> = [];

  constructor(private readonly result: StreamingEventResult | NonStreamingResult) {
    super(new UnusedStore<RequestContext | undefined>());
  }

  override async *respond(): AsyncIterable<ThreadStreamEvent> {
    throw new Error("respond is not exercised by HTTP handler tests");
  }

  override async processRequest(
    request: ProcessInput,
    context: RequestContext | undefined,
  ): Promise<StreamingEventResult | NonStreamingResult> {
    const receivedArrayBuffer = request instanceof ArrayBuffer;
    const bytes =
      typeof request === "string"
        ? encoder.encode(request)
        : request instanceof ArrayBuffer
          ? new Uint8Array(request)
          : request;

    this.calls.push({ body: decoder.decode(bytes), context, receivedArrayBuffer });
    return this.result;
  }
}
```

Replace the old `streamingResult(...)` helper with an event-level helper:

```ts
function streamingEventResult(
  events: ThreadStreamEvent[],
  tail?: (() => void) | AsyncIterable<ThreadStreamEvent>,
): StreamingEventResult {
  return new StreamingEventResult(() => ({
    [Symbol.asyncIterator](): AsyncIterator<ThreadStreamEvent> {
      let index = 0;
      const tailIterator =
        tail && typeof tail !== "function" ? tail[Symbol.asyncIterator]() : null;

      return {
        async next(): Promise<IteratorResult<ThreadStreamEvent>> {
          if (index < events.length) {
            return { done: false, value: events[index++]! };
          }
          if (tailIterator) {
            return tailIterator.next();
          }
          return { done: true, value: undefined };
        },
        async return(): Promise<IteratorResult<ThreadStreamEvent>> {
          if (typeof tail === "function") {
            tail();
          }
          await tailIterator?.return?.();
          return { done: true, value: undefined };
        },
      };
    },
  }));
}
```

Update the existing streaming response test to use:

```ts
const server = new RecordingServer(
  streamingEventResult([{ type: "error", code: "custom", allow_retry: false }]),
);
```

and assert:

```ts
expect(await response.text()).toBe('data: {"type":"error","code":"custom","allow_retry":false}\n\n');
```

Add a real lifecycle server helper below `RecordingServer`:

```ts
class LifecycleServer extends ChatKitServer<RequestContext | undefined> {
  constructor(
    private readonly responder: (
      thread: ThreadMetadata,
      inputUserMessage: Extract<ThreadItem, { type: "user_message" }> | null,
      context: RequestContext | undefined,
      runtime: ChatKitStreamRuntime,
    ) => AsyncIterable<ThreadStreamEvent>,
  ) {
    super(
      new SQLiteStore<RequestContext | undefined>({
        path: ":memory:",
        getUserId: (context) => context?.userId ?? "anonymous",
      }),
    );
  }

  override respond(
    thread: ThreadMetadata,
    inputUserMessage: Extract<ThreadItem, { type: "user_message" }> | null,
    context: RequestContext | undefined,
    runtime: ChatKitStreamRuntime,
  ): AsyncIterable<ThreadStreamEvent> {
    return this.responder(thread, inputUserMessage, context, runtime);
  }
}
```

Add these tests inside `describe("createChatKitHandler", () => { ... })`:

```ts
  test("response body cancellation detaches while the run continues", async () => {
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    let markCompleted!: () => void;
    const completed = new Promise<void>((resolve) => {
      markCompleted = resolve;
    });
    const server = new RecordingServer(
      streamingEventResult([{ type: "error", code: "custom", allow_retry: false }], async function* () {
        await released;
        markCompleted();
        yield { type: "error", code: "custom", allow_retry: false };
      }),
    );
    const runManager = new ResponseRunManager<RequestContext | undefined, ThreadStreamEvent>();
    const handler = createChatKitHandler(server, { runManager });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({ type: "threads.create", params: {} }),
      }),
    );
    const reader = response.body!.getReader();

    expect(response.headers.get("x-chatkit-run-id")).toMatch(/^run_/);
    await reader.read();
    await reader.cancel();
    release();
    await completed;
  });

  test("disconnectBehavior cancel aborts the run", async () => {
    let markCancelled!: () => void;
    const cancelled = new Promise<void>((resolve) => {
      markCancelled = resolve;
    });
    const server = new RecordingServer(
      new StreamingEventResult((runtime) => ({
        [Symbol.asyncIterator](): AsyncIterator<ThreadStreamEvent> {
          let sent = false;
          runtime.signal.addEventListener("abort", () => {
            markCancelled();
          });

          return {
            async next() {
              if (!sent) {
                sent = true;
                return { done: false, value: { type: "error", code: "custom", allow_retry: false } };
              }
              await new Promise<void>(() => {});
              return { done: true, value: undefined };
            },
            async return() {
              markCancelled();
              return { done: true, value: undefined };
            },
          };
        },
      })),
    );
    const runManager = new ResponseRunManager<RequestContext | undefined, ThreadStreamEvent>();
    const handler = createChatKitHandler(server, {
      runManager,
      disconnectBehavior: "cancel",
    });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({ type: "threads.create", params: {} }),
      }),
    );
    const reader = response.body!.getReader();
    await reader.read();
    await reader.cancel();

    await cancelled;
  });

  test("explicit run manager cancellation persists partial state and hidden context", async () => {
    const context: RequestContext = { userId: "user_1", url: "https://example.com/chatkit" };
    let markPartialReady!: () => void;
    const partialReady = new Promise<void>((resolve) => {
      markPartialReady = resolve;
    });
    const server = new LifecycleServer(async function* (thread) {
      const assistant: Extract<ThreadItem, { type: "assistant_message" }> = {
        id: "msg_partial_http_cancel",
        type: "assistant_message",
        thread_id: thread.id,
        created_at: "2026-06-20T00:00:01.000Z",
        content: [],
      };
      yield { type: "thread.item.added", item: assistant };
      yield {
        type: "thread.item.updated",
        item_id: assistant.id,
        update: {
          type: "assistant_message.content_part.text_delta",
          content_index: 0,
          delta: "Partial before explicit cancel",
        },
      };
      markPartialReady();
      await new Promise<void>(() => {});
    });
    const runManager = new ResponseRunManager<RequestContext | undefined, ThreadStreamEvent>({
      getRunScope: (requestContext) => requestContext?.userId ?? "anonymous",
    });
    const handler = createChatKitHandler(server, {
      getContext: () => context,
      runManager,
      supportsExplicitCancel: true,
    });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({
          type: "threads.create",
          params: {
            input: {
              content: [{ type: "input_text", text: "Start" }],
              attachments: [],
              inference_options: {},
            },
          },
          metadata: {},
        }),
      }),
    );
    const runId = response.headers.get("x-chatkit-run-id");
    if (!runId) {
      throw new Error("Expected run id header");
    }
    await partialReady;
    await runManager.cancelRun({ runId, context });
    await response.body?.cancel();

    const list = (await server.process(
      JSON.stringify({ type: "threads.list", params: { limit: 10, order: "desc" }, metadata: {} }),
      context,
    )) as NonStreamingResult;
    const thread = (JSON.parse(decoder.decode(list.json)) as Page<Thread>).data[0]!;
    const fullThread = (await server.process(
      JSON.stringify({
        type: "threads.get_by_id",
        params: { thread_id: thread.id },
        metadata: {},
      }),
      context,
    )) as NonStreamingResult;
    const recovered = JSON.parse(decoder.decode(fullThread.json)) as Thread;
    expect(recovered.items.data.find((item) => item.id === "msg_partial_http_cancel")).toMatchObject({
      type: "assistant_message",
      content: [{ type: "output_text", text: "Partial before explicit cancel", annotations: [] }],
    });
    const hiddenContextItems = recovered.items.data.filter((item) => item.type === "sdk_hidden_context");
    expect(hiddenContextItems).toHaveLength(1);
    expect(hiddenContextItems[0]).toMatchObject({
      type: "sdk_hidden_context",
      content: "The user cancelled the stream. Stop responding to the prior request.",
    });
  });

  test("created thread is recoverable when first-turn response is cancelled before reading", async () => {
    const context: RequestContext = { userId: "user_1", url: "https://example.com/chatkit" };
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    let markCompleted!: () => void;
    const completed = new Promise<void>((resolve) => {
      markCompleted = resolve;
    });
    const server = new LifecycleServer(async function* (thread) {
      await released;
      yield {
        type: "thread.item.done",
        item: {
          id: "msg_late_assistant",
          type: "assistant_message",
          thread_id: thread.id,
          created_at: "2026-06-20T00:00:01.000Z",
          content: [{ type: "output_text", text: "Finished in background", annotations: [] }],
        },
      };
      markCompleted();
    });
    const handler = createChatKitHandler(server, {
      getContext: () => context,
    });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({
          type: "threads.create",
          params: {
            input: {
              content: [{ type: "input_text", text: "Start" }],
              attachments: [],
              inference_options: {},
            },
          },
          metadata: {},
        }),
      }),
    );
    await response.body!.getReader().cancel();
    release();
    await completed;

    const list = (await server.process(
      JSON.stringify({ type: "threads.list", params: { limit: 10, order: "desc" }, metadata: {} }),
      context,
    )) as NonStreamingResult;
    const page = JSON.parse(decoder.decode(list.json)) as Page<Thread>;
    const createdThread = page.data[0]!;

    const fullThread = (await server.process(
      JSON.stringify({
        type: "threads.get_by_id",
        params: { thread_id: createdThread.id },
        metadata: {},
      }),
      context,
    )) as NonStreamingResult;
    const recovered = JSON.parse(decoder.decode(fullThread.json)) as Thread;
    expect(recovered.items.data.find((item) => item.type === "user_message")).toMatchObject({
      type: "user_message",
      content: [{ type: "input_text", text: "Start" }],
    });
    expect(recovered.items.data.some((item) => item.id === "msg_late_assistant")).toBe(true);
    expect(recovered.items.data.some((item) => item.type === "sdk_hidden_context")).toBe(false);
  });
```

- [ ] **Step 2: Run HTTP tests to verify failure**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/http.test.js
```

Expected: FAIL because `createChatKitHandler` has no run manager or `disconnectBehavior` support.

- [ ] **Step 3: Implement handler options**

Replace `src/http.ts` with:

```ts
import { ResponseRunManager, type DisconnectBehavior } from "./run-manager.js";
import {
  NonStreamingResult,
  StreamingEventResult,
  type ChatKitServer,
} from "./server.js";
import type { ThreadStreamEvent } from "./types/server.js";

export interface ChatKitHandlerOptions<TContext> {
  getContext?: (request: Request) => TContext | Promise<TContext>;
  runManager?: ResponseRunManager<TContext, ThreadStreamEvent>;
  disconnectBehavior?: DisconnectBehavior;
  supportsExplicitCancel?: boolean;
}

export type ChatKitHandler = (request: Request) => Promise<Response>;

export function createChatKitHandler<TContext = undefined>(
  server: ChatKitServer<TContext>,
  options: ChatKitHandlerOptions<TContext> = {},
): ChatKitHandler {
  const runManager = options.runManager ?? new ResponseRunManager<TContext, ThreadStreamEvent>();
  const disconnectBehavior = options.disconnectBehavior ?? "continue";
  const supportsExplicitCancel = options.supportsExplicitCancel ?? false;

  return async (request) => {
    const context = options.getContext
      ? await options.getContext(request)
      : (undefined as TContext);
    const result = await server.processRequest(await request.arrayBuffer(), context);

    if (result instanceof NonStreamingResult) {
      return new Response(new Uint8Array(result.json), {
        headers: {
          "content-type": "application/json",
        },
      });
    }

    if (result instanceof StreamingEventResult) {
      const { run, subscription } = await runManager.startRunAndSubscribe({
        context,
        supportsExplicitCancel,
        source: (runtime) => result.stream(runtime),
      });

      return new Response(toReadableStream(server.serializeStreamingEventsForHandler(subscription.events), async () => {
        if (disconnectBehavior === "cancel") {
          await runManager.cancelRun({ runId: run.runId, context });
        }
      }), {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
          "x-chatkit-run-id": run.runId,
        },
      });
    }

    const _exhaustive: never = result;
    return _exhaustive;
  };
}

function toReadableStream(
  iterable: AsyncIterable<Uint8Array>,
  onCancel?: () => Promise<void>,
): ReadableStream<Uint8Array> {
  let iterator: AsyncIterator<Uint8Array> | undefined;

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      iterator ??= iterable[Symbol.asyncIterator]();
      const next = await iterator.next();

      if (next.done) {
        controller.close();
        return;
      }

      controller.enqueue(next.value);
    },
    async cancel() {
      iterator ??= iterable[Symbol.asyncIterator]();
      await iterator?.return?.();
      await onCancel?.();
    },
  });
}
```

In `src/server.ts`, expose a public handler serialization method:

```ts
  serializeStreamingEventsForHandler(
    events: AsyncIterable<ThreadStreamEvent>,
  ): AsyncIterable<Uint8Array> {
    return this.serializeStreamingEvents(events);
  }
```

- [ ] **Step 4: Run HTTP tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/http.test.js
```

Expected: PASS for `http.test.js`.

- [ ] **Step 5: Run package typecheck**

Run:

```bash
npm run typecheck
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/http.ts src/server.ts tests/http.test.ts
git commit -m "Continue response runs after disconnect"
```

## Task 8: Server-Level Runtime Integration Tests

**Files:**
- Modify: `tests/server.test.ts`
- Modify: `src/server.ts`

- [ ] **Step 1: Add `stream_options.allow_cancel` runtime gating test**

Add this test inside `describe("ChatKitServer", () => { ... })` after the existing stream cancellation tests:

```ts
  test("only advertises stream cancellation when explicit cancellation is supported", async () => {
    const server = new TestServer();
    const request = JSON.stringify({
      type: "threads.create",
      params: {
        input: {
          content: [{ type: "input_text", text: "Start" }],
          attachments: [],
          inference_options: {},
        },
      },
      metadata: {},
    });

    const defaultResult = (await server.process(request, defaultContext)) as StreamingResult;
    const defaultEvents = await decodeStream(defaultResult);
    expect(defaultEvents.find((event) => event.type === "stream_options")).toEqual({
      type: "stream_options",
      stream_options: { allow_cancel: false },
    });

    const explicitResult = (await server.process(request, defaultContext, {
      runtime: {
        signal: new AbortController().signal,
        supportsExplicitCancel: true,
      },
    })) as StreamingResult;
    const explicitEvents = await decodeStream(explicitResult);
    expect(explicitEvents.find((event) => event.type === "stream_options")).toEqual({
      type: "stream_options",
      stream_options: { allow_cancel: true },
    });
  });
```

- [ ] **Step 2: Add explicit cancellation server test**

Add this test after the previous test:

```ts
  test("explicit runtime cancellation persists partial assistant state and hidden context", async () => {
    const controller = new AbortController();
    const server = new TestServer(async function* (thread, _input, _context, runtime) {
      const assistant = { ...makeAssistantMessage(""), id: "msg_explicit_cancel", thread_id: thread.id };
      yield { type: "thread.item.added", item: assistant };
      yield {
        type: "thread.item.updated",
        item_id: assistant.id,
        update: {
          type: "assistant_message.content_part.text_delta",
          content_index: 0,
          delta: "Partial before cancel",
        },
      };
      controller.abort();
      if (runtime.signal.aborted) {
        throw new StreamCancelledError();
      }
    });

    const result = (await server.process(
      JSON.stringify({
        type: "threads.create",
        params: {
          input: {
            content: [{ type: "input_text", text: "Start" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
      {
        runtime: {
          signal: controller.signal,
          supportsExplicitCancel: true,
        },
      },
    )) as StreamingResult;

    await expect(decodeStream(result)).rejects.toBeInstanceOf(StreamCancelledError);
    const threads = await server.store.loadThreads(10, null, "desc", defaultContext);
    const thread = threads.data[0]!;
    const items = await server.store.loadThreadItems(thread.id, null, 10, "asc", defaultContext);
    expect(items.data.find((item) => item.id === "msg_explicit_cancel")).toMatchObject({
      type: "assistant_message",
      content: [{ type: "output_text", text: "Partial before cancel", annotations: [] }],
    });
    expect(items.data.find((item) => item.type === "sdk_hidden_context")).toMatchObject({
      type: "sdk_hidden_context",
      content: "The user cancelled the stream. Stop responding to the prior request.",
    });
  });
```

- [ ] **Step 3: Run server tests**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/server.test.js
```

Expected: PASS for `server.test.js`.

- [ ] **Step 4: Commit**

```bash
git add src/server.ts tests/server.test.ts
git commit -m "Cover explicit runtime cancellation"
```

## Task 9: Documentation, Parity Matrix, and Public Exports

**Files:**
- Modify: `README.md`
- Modify: `docs/parity/matrix.json`
- Modify: `src/index.ts`
- Test: `tests/parity-smoke.test.ts`

- [ ] **Step 1: Update README lifecycle guidance**

In `README.md`, add this section near the server example:

```md
### Stream disconnects and cancellation

By default, `createChatKitHandler` treats an HTTP/SSE disconnect as a lost
subscriber, not as user cancellation. This matters for mobile browsers: if the
device sleeps while a response is streaming, the in-process response run keeps
draining and persists the final thread items. When the client returns, reload
the thread or call ChatKit JS `fetchUpdates()` to recover the completed state.

Explicit user cancellation must use a separate app route or control that calls
`ResponseRunManager.cancelRun({ runId, context })`. Do not treat generic fetch
abort as user cancellation unless you configure `disconnectBehavior: "cancel"`.

The in-process run manager is not crash durable. Use application infrastructure
for restart recovery or cross-process workers.
```

Update the raw Node HTTP example so the `for await` loop cancels the response
body when `outgoing` closes:

```ts
    const response = await chatkitHandler(request);
    outgoing.writeHead(response.status, Object.fromEntries(response.headers));
    if (response.body) {
      const reader = response.body.getReader();
      outgoing.on("close", () => {
        void reader.cancel();
      });
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        outgoing.write(value);
      }
    }
```

- [ ] **Step 2: Update parity matrix row**

In `docs/parity/matrix.json`, update the `server-processing` notes to state:

```json
"notes": "Core request processing, persistence, explicit cancellation, retry, actions, transcription, and HTTP streaming are covered. Node.js intentionally improves on Python's transport-coupled cancellation by treating default SSE disconnect as subscriber detach while preserving Python-aligned explicit cancellation behavior."
```

- [ ] **Step 3: Add smoke assertion for new exports**

In `tests/parity-smoke.test.ts`, extend the existing package/public API smoke area or add this test:

```ts
test("exports run lifecycle helpers", async () => {
  const pkg = await import("../src/index.js");

  expect(typeof pkg.ResponseRunManager).toBe("function");
  expect(typeof pkg.StreamError).toBe("function");
  expect(typeof pkg.CustomStreamError).toBe("function");
  expect(typeof pkg.StreamCancelledError).toBe("function");
});
```

- [ ] **Step 4: Run smoke and scripts build**

Run:

```bash
npm run build:test && node --test .tmp/test/tests/parity-smoke.test.js && npm run build:scripts
```

Expected: PASS for `parity-smoke.test.js` and scripts build.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/parity/matrix.json src/index.ts tests/parity-smoke.test.ts
git commit -m "Document run lifecycle semantics"
```

## Task 10: Full Verification and Review

**Files:**
- Verify all changed files

- [ ] **Step 1: Run full parity verification**

Run:

```bash
npm run verify:parity
```

Expected: PASS with `tests 0 fail`, package smoke tests passing, pack check passing, and parity metadata printed.

- [ ] **Step 2: Inspect git status**

Run:

```bash
git status --short --branch
```

Expected: branch is ahead with no unstaged or untracked files.

- [ ] **Step 3: Request code review**

Dispatch `thermo-nuclear-code-quality-review` with:

```text
Review the implementation of docs/superpowers/specs/2026-06-20-chatkit-run-lifecycle-parity-design.md.
Focus on disconnect vs explicit cancellation semantics, run id safety, first-turn create recovery, sanitizer coverage, stream error parity, and tests.
Review git range from the plan start commit to HEAD.
Do not edit files.
```

- [ ] **Step 4: Address review findings**

For each review finding, verify it against the codebase before changing files. If the finding is valid, add or update the smallest focused test, run that test to see it fail, implement the fix, and rerun the focused test. If a finding is technically wrong, document the reason with file and test references before proceeding.

- [ ] **Step 5: Final verification**

Run:

```bash
npm run verify:parity
```

Expected: PASS.

- [ ] **Step 6: Final commit**

If Task 10 produced review-fix edits, commit them:

```bash
git add .
git commit -m "Harden run lifecycle implementation"
```

If Task 10 produced no edits, record in the final response that no review-fix commit was needed.
