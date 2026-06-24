import { describe, test } from "node:test";

import { expect } from "./helpers/expect.js";

import { AgentContext, streamAgentResponse } from "../src/agents/index.js";
import { createChatKitHandler } from "../src/http.js";
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
import { ChatKitServer, StreamingResult } from "../src/server.js";
import { SQLiteStore } from "../src/sqlite-store.js";
import { defaultChatKitStreamRuntime } from "../src/stream-runtime.js";
import type { ThreadItem, ThreadMetadata } from "../src/types/core.js";
import { ThreadStreamEventSchema, type ThreadStreamEvent } from "../src/types/server.js";

interface RequestContext {
  userId: string;
}

const now = "2026-05-28T00:00:00.000Z";
const requestContext: RequestContext = { userId: "user_parity" };

type UserMessageItem = Extract<ThreadItem, { type: "user_message" }>;

class InlineRunSubscription implements RunSubscription<ThreadStreamEvent> {
  constructor(readonly events: AsyncIterable<ThreadStreamEvent>) {}

  async detach(_reason: RunDetachReason): Promise<void> {}
}

class InlineRunCoordinator implements RunCoordinator<RequestContext, ThreadStreamEvent> {
  async startRun(
    options: StartRunOptions<RequestContext, ThreadStreamEvent>,
  ): Promise<StartRunResult<ThreadStreamEvent>> {
    return {
      status: "started",
      runId: "run_parity",
      subscription: new InlineRunSubscription(options.source(defaultChatKitStreamRuntime())),
    };
  }

  async attachRun(
    _options: AttachRunOptions<RequestContext>,
  ): Promise<AttachRunResult<ThreadStreamEvent>> {
    return { status: "not_attachable", reason: "not_found" };
  }

  async cancelRun(_options: CancelRunOptions<RequestContext>): Promise<CancelRunResult> {
    return { status: "not_found" };
  }
}

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

function parseSseFrames(text: string): unknown[] {
  return text
    .split("\n\n")
    .filter(Boolean)
    .map((frame) => {
      expect(frame.startsWith("data: ")).toBe(true);
      return JSON.parse(frame.slice("data: ".length));
    });
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
            title: "",
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

  test("emits parseable ChatKit SSE frames from the HTTP handler", async () => {
    const server = new AgentSmokeServer();
    const handler = createChatKitHandler(server, {
      getContext: () => requestContext,
      runCoordinator: new InlineRunCoordinator(),
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

    expect(response.headers.get("content-type")).toBe("text/event-stream");
    const frames = parseSseFrames(await response.text());
    const events = frames.map((frame) => ThreadStreamEventSchema.parse(frame));

    expect(events[0]?.type).toBe("thread.created");
    expect(events.some((event) => event.type === "thread.item.done")).toBe(true);
    expect(events.some((event) => event.type === "thread.item.updated")).toBe(true);
  });

  test("exports run lifecycle helpers", async () => {
    const pkg = await import("../src/index.js");

    expect("createChatKitRunCancelHandler" in pkg).toBe(false);
    expect("createChatKitRunAttachHandler" in pkg).toBe(false);
    expect(("Response" + "RunManager") in pkg).toBe(false);
    expect(typeof pkg.StreamError).toBe("function");
    expect(typeof pkg.CustomStreamError).toBe("function");
    expect(typeof pkg.StreamCancelledError).toBe("function");
  });
});
