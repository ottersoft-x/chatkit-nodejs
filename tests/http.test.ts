import { describe, test } from "node:test";

import { expect } from "./helpers/expect.js";

import { createChatKitHandler } from "../src/http.js";
import { ResponseRunManager } from "../src/run-manager.js";
import {
  ChatKitServer,
  NonStreamingResult,
  StreamingEventResult,
  StreamCancelledError,
} from "../src/server.js";
import { SQLiteStore } from "../src/sqlite-store.js";
import { BaseStore, type StoreItemType } from "../src/store.js";
import type { ChatKitStreamRuntime } from "../src/stream-runtime.js";
import type { Attachment, Page, ThreadItem, ThreadMetadata } from "../src/types/core.js";
import type { Thread, ThreadStreamEvent } from "../src/types/server.js";

interface RequestContext {
  userId: string;
  url: string;
}

type ProcessInput = string | Uint8Array | ArrayBuffer;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

class UnusedStore<TContext> extends BaseStore<TContext> {
  override async loadThread(_threadId: string, _context: TContext): Promise<ThreadMetadata> {
    throw new Error("loadThread is not exercised by HTTP handler tests");
  }

  override async saveThread(_thread: ThreadMetadata, _context: TContext): Promise<void> {
    throw new Error("saveThread is not exercised by HTTP handler tests");
  }

  override async loadThreadItems(
    _threadId: string,
    _after: string | null,
    _limit: number,
    _order: "asc" | "desc",
    _context: TContext,
  ): Promise<Page<ThreadItem>> {
    throw new Error("loadThreadItems is not exercised by HTTP handler tests");
  }

  override async saveAttachment(_attachment: Attachment, _context: TContext): Promise<void> {
    throw new Error("saveAttachment is not exercised by HTTP handler tests");
  }

  override async loadAttachment(_attachmentId: string, _context: TContext): Promise<Attachment> {
    throw new Error("loadAttachment is not exercised by HTTP handler tests");
  }

  override async deleteAttachment(_attachmentId: string, _context: TContext): Promise<void> {
    throw new Error("deleteAttachment is not exercised by HTTP handler tests");
  }

  override async loadThreads(
    _limit: number,
    _after: string | null,
    _order: "asc" | "desc",
    _context: TContext,
  ): Promise<Page<ThreadMetadata>> {
    throw new Error("loadThreads is not exercised by HTTP handler tests");
  }

  override async addThreadItem(
    _threadId: string,
    _item: ThreadItem,
    _context: TContext,
  ): Promise<void> {
    throw new Error("addThreadItem is not exercised by HTTP handler tests");
  }

  override async saveItem(_threadId: string, _item: ThreadItem, _context: TContext): Promise<void> {
    throw new Error("saveItem is not exercised by HTTP handler tests");
  }

  override async loadItem(
    _threadId: string,
    _itemId: string,
    _context: TContext,
  ): Promise<ThreadItem> {
    throw new Error("loadItem is not exercised by HTTP handler tests");
  }

  override async deleteThread(_threadId: string, _context: TContext): Promise<void> {
    throw new Error("deleteThread is not exercised by HTTP handler tests");
  }

  override async deleteThreadItem(
    _threadId: string,
    _itemId: string,
    _context: TContext,
  ): Promise<void> {
    throw new Error("deleteThreadItem is not exercised by HTTP handler tests");
  }

  override generateItemId(
    itemType: StoreItemType,
    thread: ThreadMetadata,
    context: TContext,
  ): string {
    return super.generateItemId(itemType, thread, context);
  }
}

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

function jsonResult(value: unknown): NonStreamingResult {
  return new NonStreamingResult(encoder.encode(JSON.stringify(value)));
}

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

function createThreadRequest(text: string): string {
  return JSON.stringify({
    type: "threads.create",
    params: {
      input: {
        content: [{ type: "input_text", text }],
        attachments: [],
        inference_options: {},
      },
    },
    metadata: {},
  });
}

function decodeJsonResult<T>(result: NonStreamingResult): T {
  return JSON.parse(decoder.decode(result.json)) as T;
}

async function processJson<T>(
  server: ChatKitServer<RequestContext | undefined>,
  body: unknown,
  context: RequestContext | undefined,
): Promise<T> {
  const result = await server.process(JSON.stringify(body), context);
  if (!(result instanceof NonStreamingResult)) {
    throw new Error("Expected a non-streaming result");
  }

  return decodeJsonResult<T>(result);
}

function assistantMessage(
  thread: ThreadMetadata,
  id: string,
  text: string,
): Extract<ThreadItem, { type: "assistant_message" }> {
  return {
    id,
    type: "assistant_message",
    thread_id: thread.id,
    created_at: new Date().toISOString(),
    content: [{ type: "output_text", text, annotations: [] }],
  };
}

async function waitFor<T>(promise: Promise<T>, message: string): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), 500);
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function readThread(
  server: ChatKitServer<RequestContext | undefined>,
  context: RequestContext | undefined,
): Promise<Thread> {
  const threads = await processJson<Page<Thread>>(
    server,
    { type: "threads.list", params: { order: "asc" }, metadata: {} },
    context,
  );

  expect(threads.data).toHaveLength(1);

  return processJson<Thread>(
    server,
    {
      type: "threads.get_by_id",
      params: { thread_id: threads.data[0]!.id },
      metadata: {},
    },
    context,
  );
}

describe("createChatKitHandler", () => {
  test("returns application/json for non-streaming results", async () => {
    const server = new RecordingServer(jsonResult({ ok: true }));
    const handler = createChatKitHandler(server);

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({ type: "threads.list", params: {} }),
      }),
    );

    expect(response.headers.get("content-type")).toBe("application/json");
    expect(await response.text()).toBe(JSON.stringify({ ok: true }));
    expect(server.calls).toEqual([
      {
        body: JSON.stringify({ type: "threads.list", params: {} }),
        context: undefined,
        receivedArrayBuffer: true,
      },
    ]);
  });

  test("returns text/event-stream for streaming results", async () => {
    const server = new RecordingServer(
      streamingEventResult([{ type: "error", code: "custom", allow_retry: false }]),
    );
    const handler = createChatKitHandler(server);

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({ type: "threads.create", params: {} }),
      }),
    );

    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(response.headers.get("cache-control")).toBe("no-cache");
    expect(await response.text()).toBe(
      'data: {"type":"error","code":"custom","allow_retry":false}\n\n',
    );
    expect(server.calls).toEqual([
      {
        body: JSON.stringify({ type: "threads.create", params: {} }),
        context: undefined,
        receivedArrayBuffer: true,
      },
    ]);
  });

  test("uses getContext to resolve per-request context", async () => {
    const server = new RecordingServer(jsonResult({ ok: true }));
    let getContextCalls = 0;
    const handler = createChatKitHandler(server, {
      getContext: async (request) => {
        getContextCalls++;
        await Promise.resolve();

        return {
          userId: request.headers.get("x-user-id") ?? "anonymous",
          url: request.url,
        };
      },
    });

    await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        headers: { "x-user-id": "user_123" },
        body: "context-body",
      }),
    );

    expect(getContextCalls).toBe(1);
    expect(server.calls).toEqual([
      {
        body: "context-body",
        context: {
          userId: "user_123",
          url: "https://example.com/chatkit",
        },
        receivedArrayBuffer: true,
      },
    ]);
  });

  test("response body cancellation detaches while the run continues", async () => {
    let releaseTail!: () => void;
    const tailReleased = new Promise<void>((resolve) => {
      releaseTail = resolve;
    });
    let markCompleted!: () => void;
    const completed = new Promise<void>((resolve) => {
      markCompleted = resolve;
    });
    const server = new RecordingServer(
      streamingEventResult(
        [{ type: "notice", level: "info", message: "first" }],
        (async function* (): AsyncIterable<ThreadStreamEvent> {
          await tailReleased;
          yield { type: "notice", level: "info", message: "second" };
          markCompleted();
        })(),
      ),
    );
    const runManager = new ResponseRunManager<RequestContext | undefined, ThreadStreamEvent>();
    const handler = createChatKitHandler(server, { runManager });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: JSON.stringify({ type: "threads.create", params: {} }),
      }),
    );

    expect(response.headers.get("x-chatkit-run-id") ?? "").toMatch(/^run_/);

    const reader = response.body!.getReader();
    const first = await reader.read();

    expect(first.done).toBe(false);
    expect(decoder.decode(first.value)).toBe(
      'data: {"type":"notice","level":"info","message":"first"}\n\n',
    );

    await reader.cancel();
    releaseTail();
    await waitFor(completed, "Expected run source to continue after response body cancellation");
  });

  test("disconnectBehavior cancel aborts the run", async () => {
    let observeCancellation!: () => void;
    const cancellationObserved = new Promise<void>((resolve) => {
      observeCancellation = resolve;
    });
    const server = new RecordingServer(
      new StreamingEventResult((runtime) => ({
        [Symbol.asyncIterator](): AsyncIterator<ThreadStreamEvent> {
          let sent = false;
          runtime.signal.addEventListener("abort", observeCancellation, { once: true });

          return {
            async next(): Promise<IteratorResult<ThreadStreamEvent>> {
              if (!sent) {
                sent = true;
                return {
                  done: false,
                  value: { type: "notice", level: "info", message: "first" },
                };
              }

              await new Promise<void>(() => {});
              return { done: true, value: undefined };
            },
            async return(): Promise<IteratorResult<ThreadStreamEvent>> {
              observeCancellation();
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
    const first = await reader.read();

    expect(first.done).toBe(false);
    expect(decoder.decode(first.value)).toBe(
      'data: {"type":"notice","level":"info","message":"first"}\n\n',
    );

    await reader.cancel();
    await waitFor(cancellationObserved, "Expected disconnectBehavior cancel to abort the run");
  });

  test("explicit run manager cancellation persists partial state and hidden context", async () => {
    let markPartialReady!: () => void;
    const partialReady = new Promise<void>((resolve) => {
      markPartialReady = resolve;
    });
    const server = new LifecycleServer(async function* (
      thread,
      _inputUserMessage,
      _context,
      runtime,
    ): AsyncIterable<ThreadStreamEvent> {
      const assistant = assistantMessage(thread, "msg_partial", "");
      yield { type: "thread.item.added", item: assistant };
      yield {
        type: "thread.item.updated",
        item_id: assistant.id,
        update: {
          type: "assistant_message.content_part.text_delta",
          content_index: 0,
          delta: "Partial answer",
        },
      };
      markPartialReady();

      await new Promise<never>((_, reject) => {
        runtime.signal.addEventListener(
          "abort",
          () => reject(new StreamCancelledError()),
          { once: true },
        );
      });
    });
    const runManager = new ResponseRunManager<RequestContext | undefined, ThreadStreamEvent>({
      getRunScope: (context) => context?.userId ?? "anonymous",
    });
    const context: RequestContext = { userId: "user_123", url: "https://example.com/chatkit" };
    const handler = createChatKitHandler(server, {
      getContext: (request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
      runManager,
      supportsExplicitCancel: true,
    });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body: createThreadRequest("Start"),
      }),
    );
    const runId = response.headers.get("x-chatkit-run-id");

    expect(runId ?? "").toMatch(/^run_/);

    await waitFor(partialReady, "Expected partial assistant state to be produced");
    expect(await runManager.cancelRun({ runId: runId!, context })).toEqual({ status: "cancelled" });
    await response.body!.cancel();

    const thread = await readThread(server, context);
    const assistant = thread.items.data.find((item) => item.id === "msg_partial");

    expect(thread.items.data.some((item) => item.type === "user_message")).toBe(true);
    expect(assistant).toMatchObject({
      type: "assistant_message",
      content: [{ type: "output_text", text: "Partial answer", annotations: [] }],
    });

    const storedItems = await server.store.loadThreadItems(thread.id, null, 20, "asc", context);
    const hiddenContextItems = storedItems.data.filter(
      (item): item is Extract<ThreadItem, { type: "sdk_hidden_context" }> =>
        item.type === "sdk_hidden_context",
    );
    expect(hiddenContextItems).toHaveLength(1);
    expect(hiddenContextItems[0]!.content).toBe(
      "The user cancelled the stream. Stop responding to the prior request.",
    );
  });

  test("created thread is recoverable when first-turn response is cancelled before reading", async () => {
    let releaseResponder!: () => void;
    const responderReleased = new Promise<void>((resolve) => {
      releaseResponder = resolve;
    });
    let markCompleted!: () => void;
    const completed = new Promise<void>((resolve) => {
      markCompleted = resolve;
    });
    const server = new LifecycleServer(async function* (
      thread,
    ): AsyncIterable<ThreadStreamEvent> {
      await responderReleased;
      yield { type: "thread.item.done", item: assistantMessage(thread, "msg_late", "Late answer") };
      markCompleted();
    });
    const context: RequestContext = { userId: "user_123", url: "https://example.com/chatkit" };
    const handler = createChatKitHandler(server, {
      getContext: (request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
    });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body: createThreadRequest("Start before sleep"),
      }),
    );

    expect(response.headers.get("x-chatkit-run-id") ?? "").toMatch(/^run_/);

    await response.body!.cancel();
    releaseResponder();
    await waitFor(completed, "Expected run to complete after response body cancellation");

    const thread = await readThread(server, context);
    const userMessage = thread.items.data.find((item) => item.type === "user_message");
    const assistant = thread.items.data.find((item) => item.id === "msg_late");

    expect(userMessage).toMatchObject({
      type: "user_message",
      content: [{ type: "input_text", text: "Start before sleep" }],
    });
    expect(assistant).toMatchObject({
      type: "assistant_message",
      content: [{ type: "output_text", text: "Late answer", annotations: [] }],
    });

    const storedItems = await server.store.loadThreadItems(thread.id, null, 20, "asc", context);
    expect(storedItems.data.some((item) => item.type === "sdk_hidden_context")).toBe(false);
  });

  test("source failures reject the response body instead of closing cleanly", async () => {
    const server = new RecordingServer(
      new StreamingEventResult(async function* (): AsyncIterable<ThreadStreamEvent> {
        yield { type: "notice", level: "info", message: "before failure" };
        throw new Error("source failed");
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

    await expect(response.text()).rejects.toThrow("source failed");
  });
});
