import { describe, test } from "node:test";

import { expect } from "./helpers/expect.js";

import {
  createChatKitHandler,
  createChatKitRunAttachHandler,
  createChatKitRunCancelHandler,
} from "../src/http.js";
import {
  ChatKitServer,
  NonStreamingResult,
  StreamingEventResult,
  type StreamingDescriptorMetadata,
} from "../src/server.js";
import type {
  AttachRunOptions,
  AttachRunResult,
  CancelRunOptions,
  CancelRunResult,
  RunCoordinator,
  RunDetachReason,
  RunSubscription,
  StartRunOptions,
  StartRunResult,
} from "../src/run-coordinator.js";
import { SQLiteStore } from "../src/sqlite-store.js";
import { BaseStore, type StoreItemType } from "../src/store.js";
import { StreamCancelledError, type ChatKitStreamRuntime } from "../src/stream-runtime.js";
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

class AsyncEventQueue<TEvent> {
  private readonly values: TEvent[] = [];
  private readonly waiters: Array<{
    resolve: (result: IteratorResult<TEvent>) => void;
    reject: (error: unknown) => void;
  }> = [];
  private closed = false;
  private error: unknown;

  push(event: TEvent): void {
    if (this.closed || this.error) {
      return;
    }

    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ done: false, value: event });
      return;
    }

    this.values.push(event);
  }

  close(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter.resolve({ done: true, value: undefined });
    }
  }

  fail(error: unknown): void {
    if (this.closed || this.error) {
      return;
    }

    this.error = error;
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(error);
    }
  }

  next(): Promise<IteratorResult<TEvent>> {
    if (this.values.length > 0) {
      return Promise.resolve({ done: false, value: this.values.shift()! });
    }

    if (this.error) {
      return Promise.reject(this.error);
    }

    if (this.closed) {
      return Promise.resolve({ done: true, value: undefined });
    }

    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }
}

class RecordingRunSubscription implements RunSubscription<ThreadStreamEvent> {
  readonly detachReasons: RunDetachReason[] = [];
  private readonly queue = new AsyncEventQueue<ThreadStreamEvent>();

  readonly events: AsyncIterable<ThreadStreamEvent> = {
    [Symbol.asyncIterator]: () => ({
      next: () => this.queue.next(),
      return: async () => {
        this.queue.close();
        return { done: true, value: undefined };
      },
    }),
  };

  push(event: ThreadStreamEvent): void {
    this.queue.push(event);
  }

  close(): void {
    this.queue.close();
  }

  fail(error: unknown): void {
    this.queue.fail(error);
  }

  async detach(reason: RunDetachReason): Promise<void> {
    this.detachReasons.push(reason);
    this.queue.close();
  }
}

class RecordingRunCoordinator<TContext>
  implements RunCoordinator<TContext, ThreadStreamEvent>
{
  readonly startCalls: Array<StartRunOptions<TContext, ThreadStreamEvent>> = [];
  readonly attachCalls: Array<AttachRunOptions<TContext>> = [];
  readonly cancelCalls: Array<CancelRunOptions<TContext>> = [];
  readonly subscriptions: RecordingRunSubscription[] = [];
  readonly drainPromises: Promise<void>[] = [];
  private nextRunNumber = 1;

  constructor(
    private readonly options: {
      startResult?: StartRunResult<ThreadStreamEvent>;
      attachResult?: AttachRunResult<ThreadStreamEvent>;
      cancelResult?: CancelRunResult;
      runtime?: () => ChatKitStreamRuntime;
    } = {},
  ) {}

  async startRun(
    options: StartRunOptions<TContext, ThreadStreamEvent>,
  ): Promise<StartRunResult<ThreadStreamEvent>> {
    this.startCalls.push(options);

    if (this.options.startResult) {
      return this.options.startResult;
    }

    const subscription = new RecordingRunSubscription();
    this.subscriptions.push(subscription);

    const drainPromise = this.drainSource(options, subscription);
    this.drainPromises.push(drainPromise);
    void drainPromise.catch(() => undefined);

    return {
      status: "started",
      runId: `run_recorded_${this.nextRunNumber++}`,
      subscription,
    };
  }

  async attachRun(options: AttachRunOptions<TContext>): Promise<AttachRunResult<ThreadStreamEvent>> {
    this.attachCalls.push(options);
    return this.options.attachResult ?? { status: "not_attachable", reason: "not_found" };
  }

  async cancelRun(options: CancelRunOptions<TContext>): Promise<CancelRunResult> {
    this.cancelCalls.push(options);
    return this.options.cancelResult ?? { status: "cancelled" };
  }

  private async drainSource(
    options: StartRunOptions<TContext, ThreadStreamEvent>,
    subscription: RecordingRunSubscription,
  ): Promise<void> {
    try {
      for await (const event of options.source(
        this.options.runtime ? this.options.runtime() : defaultRuntime(),
      )) {
        subscription.push(event);
      }
      subscription.close();
    } catch (error) {
      subscription.fail(error);
      throw error;
    }
  }
}

class ExplicitCancelRunCoordinator<TContext>
  implements RunCoordinator<TContext, ThreadStreamEvent>
{
  readonly startCalls: Array<StartRunOptions<TContext, ThreadStreamEvent>> = [];
  readonly attachCalls: Array<AttachRunOptions<TContext>> = [];
  readonly cancelCalls: Array<CancelRunOptions<TContext>> = [];
  readonly subscriptions: RecordingRunSubscription[] = [];
  readonly completedRuns: Promise<void>[] = [];
  private readonly controllers = new Map<string, AbortController>();
  private nextRunNumber = 1;

  async startRun(
    options: StartRunOptions<TContext, ThreadStreamEvent>,
  ): Promise<StartRunResult<ThreadStreamEvent>> {
    this.startCalls.push(options);
    const runId = `run_explicit_cancel_${this.nextRunNumber++}`;
    const controller = new AbortController();
    const subscription = new RecordingRunSubscription();
    this.controllers.set(runId, controller);
    this.subscriptions.push(subscription);
    const completed = this.drainSource(options, subscription, controller);
    this.completedRuns.push(completed);
    void completed.finally(() => {
      this.controllers.delete(runId);
    }).catch(() => undefined);

    return { status: "started", runId, subscription };
  }

  async attachRun(options: AttachRunOptions<TContext>): Promise<AttachRunResult<ThreadStreamEvent>> {
    this.attachCalls.push(options);
    return { status: "not_attachable", reason: "not_found" };
  }

  async cancelRun(options: CancelRunOptions<TContext>): Promise<CancelRunResult> {
    this.cancelCalls.push(options);
    const controller = this.controllers.get(options.runId);
    if (!controller) {
      return { status: "not_found" };
    }

    controller.abort();
    await this.completedRuns.at(-1);
    return { status: "cancelled" };
  }

  private async drainSource(
    options: StartRunOptions<TContext, ThreadStreamEvent>,
    subscription: RecordingRunSubscription,
    controller: AbortController,
  ): Promise<void> {
    try {
      for await (const event of options.source({
        signal: controller.signal,
        supportsExplicitCancel: true,
      })) {
        subscription.push(event);
      }
      subscription.close();
    } catch (error) {
      subscription.fail(error);
      if (!(error instanceof StreamCancelledError)) {
        throw error;
      }
    }
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
  descriptorMetadata: StreamingDescriptorMetadata = { requestType: "threads.create" },
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
  }), descriptorMetadata);
}

function defaultRuntime(): ChatKitStreamRuntime {
  return {
    signal: new AbortController().signal,
    supportsExplicitCancel: false,
  };
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

function parseSseFrames(text: string): unknown[] {
  return text
    .trim()
    .split("\n\n")
    .filter((frame) => frame.length > 0)
    .map((frame) => {
      expect(frame.startsWith("data: ")).toBe(true);
      return JSON.parse(frame.slice("data: ".length)) as unknown;
    });
}

function waitForSignalAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
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
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitHandler(server, { runCoordinator });

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
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitHandler(server, { runCoordinator });
    const body = createThreadRequest("Stream response");

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body,
      }),
    );

    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(response.headers.get("cache-control")).toBe("no-cache");
    expect(response.headers.get("x-chatkit-run-id")).toBe("run_recorded_1");
    expect(await response.text()).toBe(
      'data: {"type":"error","code":"custom","allow_retry":false}\n\n',
    );
    expect(server.calls).toEqual([
      {
        body,
        context: undefined,
        receivedArrayBuffer: true,
      },
    ]);
  });

  test("uses getContext to resolve per-request context", async () => {
    const server = new RecordingServer(jsonResult({ ok: true }));
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
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
      runCoordinator,
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

  test("processRequest derives streaming descriptor metadata from parsed request bodies", async () => {
    const server = new LifecycleServer(async function* (): AsyncIterable<ThreadStreamEvent> {});

    const createResult = await server.processRequest(createThreadRequest("Create descriptor"), undefined);

    expect(createResult).toBeInstanceOf(StreamingEventResult);
    expect((createResult as StreamingEventResult).descriptorMetadata).toEqual({
      requestType: "threads.create",
    });

    const structuredInputResult = await server.processRequest(
      JSON.stringify({
        type: "threads.add_structured_input",
        params: {
          thread_id: "thread_descriptor",
          item_id: "item_descriptor",
          input: { answers: {}, status: "answered" },
        },
        metadata: {},
      }),
      undefined,
    );

    expect(structuredInputResult).toBeInstanceOf(StreamingEventResult);
    expect((structuredInputResult as StreamingEventResult).descriptorMetadata).toEqual({
      requestType: "threads.add_structured_input",
      threadId: "thread_descriptor",
      itemId: "item_descriptor",
    });
  });

  test("streaming requests pass context, descriptor metadata, and raw request bytes to the coordinator", async () => {
    const body = JSON.stringify({
      type: "threads.add_structured_input",
      params: {
        thread_id: "thread_123",
        item_id: "item_456",
        input: { answers: {}, status: "answered" },
      },
      metadata: {},
    });
    const context: RequestContext = {
      userId: "user_123",
      url: "https://example.com/chatkit",
    };
    const server = new RecordingServer(
      streamingEventResult(
        [{ type: "notice", level: "info", message: "metadata" }],
        undefined,
        {
          requestType: "threads.add_structured_input",
          threadId: "thread_123",
          itemId: "item_456",
        },
      ),
    );
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitHandler(server, {
      getContext: (request: Request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
      runCoordinator,
    });

    const response = await handler(
      new Request(context.url, {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body,
      }),
    );

    await response.text();

    expect(runCoordinator.startCalls).toHaveLength(1);
    const startCall = runCoordinator.startCalls[0]!;
    expect(startCall.context).toEqual(context);
    expect(startCall.descriptor.requestType).toBe("threads.add_structured_input");
    expect(startCall.descriptor.threadId).toBe("thread_123");
    expect(startCall.descriptor.itemId).toBe("item_456");
    expect(Number.isNaN(Date.parse(startCall.descriptor.receivedAt))).toBe(false);
    expect(startCall.descriptor.rawRequest).toBeInstanceOf(Uint8Array);
    expect(Array.from(startCall.descriptor.rawRequest)).toEqual(Array.from(encoder.encode(body)));
  });

  test("streaming request without runCoordinator returns a configuration error", async () => {
    const server = new RecordingServer(
      streamingEventResult([{ type: "notice", level: "info", message: "unused" }]),
    );
    const handler = createChatKitHandler(server, {} as never);
    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: createThreadRequest("Missing coordinator"),
      }),
    );

    expect(response.status).toBe(500);
    expect(response.headers.get("content-type")).toBe("application/json");
    expect(await response.json()).toEqual({
      error: {
        code: "configuration_error",
        message: "createChatKitHandler requires runCoordinator for streaming requests.",
      },
    });
  });

  test("maps coordinator not_started reasons to HTTP status codes", async () => {
    const cases = [
      { reason: "forbidden" as const, status: 403 },
      { reason: "conflict" as const, status: 409 },
      { reason: "unavailable" as const, status: 503 },
    ];

    for (const { reason, status } of cases) {
      const server = new RecordingServer(
        streamingEventResult([{ type: "notice", level: "info", message: "unused" }]),
      );
      const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>({
        startResult: { status: "not_started", reason, message: `${reason} message` },
      });
      const handler = createChatKitHandler(server, { runCoordinator });

      const response = await handler(
        new Request("https://example.com/chatkit", {
          method: "POST",
          body: createThreadRequest(`Start ${reason}`),
        }),
      );

      expect(response.status).toBe(status);
      expect(response.headers.get("content-type")).toBe("application/json");
      expect(await response.json()).toEqual({
        error: {
          code: reason,
          message: `${reason} message`,
        },
      });
    }
  });

  test("response body cancellation detaches the subscription without cancelling the run", async () => {
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
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitHandler(server, { runCoordinator });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: createThreadRequest("Detach and continue"),
      }),
    );

    expect(response.headers.get("x-chatkit-run-id")).toBe("run_recorded_1");

    const reader = response.body!.getReader();
    const first = await reader.read();

    expect(first.done).toBe(false);
    expect(decoder.decode(first.value)).toBe(
      'data: {"type":"notice","level":"info","message":"first"}\n\n',
    );

    await reader.cancel();
    expect(runCoordinator.subscriptions[0]!.detachReasons).toEqual(["subscriber_cancelled"]);
    expect(runCoordinator.cancelCalls).toEqual([]);

    releaseTail();
    await waitFor(completed, "Expected run source to continue after response body cancellation");
  });

  test("app-owned coordinator can drain a first-turn run after subscriber detach and recover Store state", async () => {
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
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitHandler(server, {
      getContext: (request: Request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
      runCoordinator,
    });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body: createThreadRequest("Start before sleep"),
      }),
    );

    expect(response.headers.get("x-chatkit-run-id")).toBe("run_recorded_1");

    await response.body!.cancel();
    expect(runCoordinator.subscriptions[0]!.detachReasons).toEqual(["subscriber_cancelled"]);
    expect(runCoordinator.cancelCalls).toEqual([]);

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
      new StreamingEventResult(
        async function* (): AsyncIterable<ThreadStreamEvent> {
          yield { type: "notice", level: "info", message: "before failure" };
          throw new Error("source failed");
        },
        { requestType: "threads.create" },
      ),
    );
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitHandler(server, { runCoordinator });

    const response = await handler(
      new Request("https://example.com/chatkit", {
        method: "POST",
        body: createThreadRequest("Fail source"),
      }),
    );

    await expect(response.text()).rejects.toThrow("source failed");
  });
});

describe("createChatKitRunCancelHandler", () => {
  test("calls cancelRun with the request context", async () => {
    const context: RequestContext = { userId: "user_123", url: "https://example.com/chatkit/runs/cancel" };
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitRunCancelHandler({
      getContext: (request: Request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
      runCoordinator,
    });

    const response = await handler(
      new Request(context.url, {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body: JSON.stringify({ run_id: "run_123" }),
      }),
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: "cancelled" });
    expect(runCoordinator.cancelCalls).toEqual([{ runId: "run_123", context }]);
  });

  test("trims run_id before calling cancelRun", async () => {
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
    const handler = createChatKitRunCancelHandler({ runCoordinator });

    const response = await handler(
      new Request("https://example.com/chatkit/runs/cancel", {
        method: "POST",
        body: JSON.stringify({ run_id: " run_123 " }),
      }),
    );

    expect(response.status).toBe(200);
    expect(runCoordinator.cancelCalls).toEqual([{ runId: "run_123", context: undefined }]);
  });

  test("maps pending and finished cancel outcomes to structured 200 responses", async () => {
    const cases = [
      { status: "cancelling" as const },
      { status: "already_finished" as const },
    ];

    for (const result of cases) {
      const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>({
        cancelResult: result,
      });
      const handler = createChatKitRunCancelHandler({ runCoordinator });

      const response = await handler(
        new Request("https://example.com/chatkit/runs/cancel", {
          method: "POST",
          body: JSON.stringify({ run_id: "run_123" }),
        }),
      );

      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toBe("application/json");
      expect(await response.json()).toEqual(result);
    }
  });

  test("maps forbidden and missing runs to structured responses", async () => {
    const cases = [
      {
        result: { status: "forbidden" } satisfies CancelRunResult,
        status: 403,
        code: "forbidden",
        message: "Run access forbidden.",
      },
      {
        result: { status: "not_found" } satisfies CancelRunResult,
        status: 404,
        code: "not_found",
        message: "Run not found.",
      },
    ];

    for (const { result, status, code, message } of cases) {
      const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>({
        cancelResult: result,
      });
      const handler = createChatKitRunCancelHandler({ runCoordinator });

      const response = await handler(
        new Request("https://example.com/chatkit/runs/cancel", {
          method: "POST",
          body: JSON.stringify({ run_id: "run_123" }),
        }),
      );

      expect(response.status).toBe(status);
      expect(response.headers.get("content-type")).toBe("application/json");
      expect(await response.json()).toEqual({ error: { code, message } });
    }
  });

  test("rejects missing or invalid run_id without calling cancelRun", async () => {
    const invalidBodies = [
      "{",
      JSON.stringify({}),
      JSON.stringify({ run_id: null }),
      JSON.stringify({ run_id: 123 }),
      JSON.stringify({ run_id: "" }),
      JSON.stringify({ run_id: "   " }),
    ];

    for (const body of invalidBodies) {
      const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
      const handler = createChatKitRunCancelHandler({ runCoordinator });

      const response = await handler(
        new Request("https://example.com/chatkit/runs/cancel", {
          method: "POST",
          body,
        }),
      );

      expect(response.status).toBe(400);
      expect(response.headers.get("content-type")).toBe("application/json");
      expect(await response.json()).toEqual({
        error: {
          code: "invalid_request",
          message: "Expected JSON body with string run_id.",
        },
      });
      expect(runCoordinator.cancelCalls).toEqual([]);
    }
  });

  test("can abort an app-owned runtime while preserving cancellation Store behavior", async () => {
    let markPartialProduced!: () => void;
    const partialProduced = new Promise<void>((resolve) => {
      markPartialProduced = resolve;
    });
    const server = new LifecycleServer(async function* (
      thread,
      _inputUserMessage,
      _context,
      runtime,
    ): AsyncIterable<ThreadStreamEvent> {
      const assistant = { ...assistantMessage(thread, "msg_partial_cancel", ""), thread_id: thread.id };
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
      markPartialProduced();
      await waitForSignalAbort(runtime.signal);
      throw new StreamCancelledError();
    });
    const context: RequestContext = { userId: "user_123", url: "https://example.com/chatkit" };
    const runCoordinator = new ExplicitCancelRunCoordinator<RequestContext | undefined>();
    const streamingHandler = createChatKitHandler(server, {
      getContext: (request: Request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
      runCoordinator,
    });
    const cancelHandler = createChatKitRunCancelHandler({
      getContext: (request: Request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
      runCoordinator,
    });

    const streamResponse = await streamingHandler(
      new Request(context.url, {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body: createThreadRequest("Start cancellable run"),
      }),
    );
    const runId = streamResponse.headers.get("x-chatkit-run-id");

    expect(runId).toBe("run_explicit_cancel_1");
    await waitFor(partialProduced, "Expected partial assistant state before cancelling");

    const cancelResponse = await cancelHandler(
      new Request(context.url, {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body: JSON.stringify({ run_id: runId }),
      }),
    );

    expect(cancelResponse.status).toBe(200);
    expect(await cancelResponse.json()).toEqual({ status: "cancelled" });
    expect(runCoordinator.cancelCalls).toEqual([{ runId, context }]);

    const thread = await readThread(server, context);
    const storedItems = await server.store.loadThreadItems(thread.id, null, 20, "asc", context);

    expect(storedItems.data.find((item) => item.type === "user_message")).toMatchObject({
      type: "user_message",
      content: [{ type: "input_text", text: "Start cancellable run" }],
    });
    expect(storedItems.data.find((item) => item.id === "msg_partial_cancel")).toMatchObject({
      type: "assistant_message",
      content: [{ type: "output_text", text: "Partial before cancel", annotations: [] }],
    });
    expect(storedItems.data.find((item) => item.type === "sdk_hidden_context")).toMatchObject({
      type: "sdk_hidden_context",
      content: "The user cancelled the stream. Stop responding to the prior request.",
    });
  });
});

describe("createChatKitRunAttachHandler", () => {
  test("streams attached events and passes request context", async () => {
    const context: RequestContext = { userId: "user_123", url: "https://example.com/chatkit/runs/attach" };
    const subscription = new RecordingRunSubscription();
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>({
      attachResult: { status: "attached", runId: "run_attached_123", subscription },
    });
    const handler = createChatKitRunAttachHandler({
      getContext: (request: Request) => ({
        userId: request.headers.get("x-user-id") ?? "anonymous",
        url: request.url,
      }),
      runCoordinator,
    });

    const response = await handler(
      new Request(context.url, {
        method: "POST",
        headers: { "x-user-id": context.userId },
        body: JSON.stringify({ run_id: "run_123" }),
      }),
    );

    subscription.push({ type: "notice", level: "info", message: "attached" });
    subscription.close();

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(response.headers.get("cache-control")).toBe("no-cache");
    expect(response.headers.get("x-chatkit-run-id")).toBe("run_attached_123");
    expect(await response.text()).toBe(
      'data: {"type":"notice","level":"info","message":"attached"}\n\n',
    );
    expect(runCoordinator.attachCalls).toEqual([{ runId: "run_123", context }]);
  });

  test("maps every not-attachable outcome to a structured response", async () => {
    const cases = [
      { reason: "not_found" as const, status: 404 },
      { reason: "forbidden" as const, status: 403 },
      { reason: "finished" as const, status: 409 },
      { reason: "expired" as const, status: 410 },
      { reason: "unavailable" as const, status: 503, retryAfterMs: 250 },
    ];

    for (const { reason, status, retryAfterMs } of cases) {
      const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>({
        attachResult: {
          status: "not_attachable",
          reason,
          message: `${reason} message`,
          retryAfterMs,
        },
      });
      const handler = createChatKitRunAttachHandler({ runCoordinator });

      const response = await handler(
        new Request("https://example.com/chatkit/runs/attach", {
          method: "POST",
          body: JSON.stringify({ run_id: "run_123" }),
        }),
      );

      expect(response.status).toBe(status);
      expect(response.headers.get("content-type")).toBe("application/json");
      expect(await response.json()).toEqual({
        status: "not_attachable",
        reason,
        message: `${reason} message`,
        ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      });
    }
  });

  test("rejects missing or invalid run_id without calling attachRun", async () => {
    const invalidBodies = [
      "{",
      JSON.stringify({}),
      JSON.stringify({ run_id: null }),
      JSON.stringify({ run_id: 123 }),
      JSON.stringify({ run_id: "" }),
      JSON.stringify({ run_id: "   " }),
    ];

    for (const body of invalidBodies) {
      const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>();
      const handler = createChatKitRunAttachHandler({ runCoordinator });

      const response = await handler(
        new Request("https://example.com/chatkit/runs/attach", {
          method: "POST",
          body,
        }),
      );

      expect(response.status).toBe(400);
      expect(response.headers.get("content-type")).toBe("application/json");
      expect(await response.json()).toEqual({
        error: {
          code: "invalid_request",
          message: "Expected JSON body with string run_id.",
        },
      });
      expect(runCoordinator.attachCalls).toEqual([]);
    }
  });

  test("uses sanitizer-backed serialization for metadata-bearing events", async () => {
    const subscription = new RecordingRunSubscription();
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>({
      attachResult: { status: "attached", runId: "run_sanitize", subscription },
    });
    const handler = createChatKitRunAttachHandler({ runCoordinator });

    const response = await handler(
      new Request("https://example.com/chatkit/runs/attach", {
        method: "POST",
        body: JSON.stringify({ run_id: "run_sanitize" }),
      }),
    );
    const threadCreatedAt = new Date().toISOString();
    const itemCreatedAt = new Date().toISOString();
    const metadataBearingUserMessage = {
      id: "msg_sanitize",
      thread_id: "thread_sanitize",
      created_at: itemCreatedAt,
      type: "user_message" as const,
      content: [{ type: "input_text" as const, text: "See attachment" }],
      attachments: [
        {
          id: "att_sanitize",
          type: "file" as const,
          mime_type: "text/plain",
          name: "notes.txt",
          thread_id: "thread_sanitize",
          metadata: { secret: "remove me" },
        },
      ],
      inference_options: {},
    };

    subscription.push({
      type: "thread.created",
      thread: {
        id: "thread_sanitize",
        created_at: threadCreatedAt,
        status: { type: "active" },
        items: { data: [metadataBearingUserMessage], has_more: false, after: null },
      } as unknown as Thread,
    });
    subscription.push({
      type: "thread.item.done",
      item: metadataBearingUserMessage,
    });
    subscription.close();

    const text = await response.text();

    expect(text).not.toContain("metadata");
    expect(parseSseFrames(text)).toEqual([
      {
        type: "thread.created",
        thread: {
          id: "thread_sanitize",
          created_at: threadCreatedAt,
          status: { type: "active" },
          items: {
            data: [
              {
                id: "msg_sanitize",
                thread_id: "thread_sanitize",
                created_at: itemCreatedAt,
                type: "user_message",
                content: [{ type: "input_text", text: "See attachment" }],
                attachments: [
                  {
                    id: "att_sanitize",
                    type: "file",
                    mime_type: "text/plain",
                    name: "notes.txt",
                    thread_id: "thread_sanitize",
                  },
                ],
                inference_options: {},
              },
            ],
            has_more: false,
            after: null,
          },
        },
      },
      {
        type: "thread.item.done",
        item: {
          id: "msg_sanitize",
          thread_id: "thread_sanitize",
          created_at: itemCreatedAt,
          type: "user_message",
          content: [{ type: "input_text", text: "See attachment" }],
          attachments: [
            {
              id: "att_sanitize",
              type: "file",
              mime_type: "text/plain",
              name: "notes.txt",
              thread_id: "thread_sanitize",
            },
          ],
          inference_options: {},
        },
      },
    ]);
  });

  test("detaches the subscription when the attached response body is cancelled", async () => {
    const subscription = new RecordingRunSubscription();
    const runCoordinator = new RecordingRunCoordinator<RequestContext | undefined>({
      attachResult: { status: "attached", runId: "run_detach", subscription },
    });
    const handler = createChatKitRunAttachHandler({ runCoordinator });

    const response = await handler(
      new Request("https://example.com/chatkit/runs/attach", {
        method: "POST",
        body: JSON.stringify({ run_id: "run_detach" }),
      }),
    );

    subscription.push({ type: "notice", level: "info", message: "first" });

    const reader = response.body!.getReader();
    const first = await reader.read();

    expect(first.done).toBe(false);
    expect(decoder.decode(first.value)).toBe(
      'data: {"type":"notice","level":"info","message":"first"}\n\n',
    );

    await reader.cancel();

    expect(subscription.detachReasons).toEqual(["subscriber_cancelled"]);
  });
});
