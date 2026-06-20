import test from "node:test";
import { strict as assert } from "node:assert";

import {
  sanitizeClientPayload,
  sanitizeThreadItem,
  sanitizeThreadStreamEvent,
} from "../src/response-sanitizer.js";
import type {
  ClientPage,
  ClientSyncCustomActionResponse,
  ClientThread,
  ClientThreadItem,
  ClientThreadStreamEvent,
  SyncCustomActionResponsePayloadInput,
  ThreadItemPagePayloadInput,
  ThreadItemPayloadInput,
  ThreadStreamEventPayloadInput,
} from "../src/response-sanitizer.js";
import type { Attachment, Thread, ThreadItem, ThreadStreamEvent } from "../src/index.js";
import type { SyncCustomActionResponse } from "../src/types/server.js";

function expectType<T>(_value: T): void {}

const attachment: Attachment = {
  id: "atc_secret",
  type: "file",
  name: "secret.txt",
  mime_type: "text/plain",
  metadata: { source: "internal" },
};

const uploadAttachment: Attachment = {
  id: "atc_upload",
  type: "file",
  name: "upload.txt",
  mime_type: "text/plain",
  metadata: { source: "internal" },
  upload_descriptor: {
    url: "https://example.com/upload",
    method: "POST",
    headers: { authorization: "secret" },
  },
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
  metadata: { topic: "support" },
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

test("sanitizeThreadItem deep clones retained attachment fields", () => {
  const message: Extract<ThreadItem, { type: "user_message" }> = {
    ...userMessage,
    attachments: [uploadAttachment],
  };

  const sanitized = sanitizeThreadItem(message);

  if (sanitized.type !== "user_message") {
    throw new Error("Expected user message");
  }

  assert.equal("metadata" in sanitized.attachments[0]!, false);
  sanitized.attachments[0]!.upload_descriptor!.headers.authorization = "changed";
  assert.equal(uploadAttachment.upload_descriptor!.headers.authorization, "secret");
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

  assert.deepEqual(sanitized.thread.metadata, { topic: "support" });
  assert.equal("metadata" in item.attachments[0]!, false);
  const originalItem = threadResponse.items.data[0];
  if (!originalItem || originalItem.type !== "user_message") {
    throw new Error("Expected original user message");
  }
  assert.deepEqual(originalItem.attachments[0]!.metadata, { source: "internal" });
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

test("sanitizeClientPayload handles pages without after cursor", () => {
  const sanitized = sanitizeClientPayload({
    data: [userMessage],
    has_more: false,
  });

  const item = sanitized.data[0];
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.equal("metadata" in item.attachments[0]!, false);
});

test("sanitizeClientPayload handles pages with undefined after cursor", () => {
  const sanitized = sanitizeClientPayload({
    data: [userMessage],
    has_more: false,
    after: undefined,
  });

  const item = sanitized.data[0];
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.equal("metadata" in item.attachments[0]!, false);
});

test("sanitizeClientPayload handles pages with omitted has_more", () => {
  const input = {
    data: [userMessage],
    metadata: { keep: true },
  };
  const sanitized = sanitizeClientPayload(input);

  expectType<ClientPage<typeof input, ClientThreadItem>>(sanitized);
  assert.equal(sanitized.has_more, false);
  assert.deepEqual(sanitized.metadata, { keep: true });
  const item = sanitized.data[0];
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.equal("metadata" in item.attachments[0]!, false);
});

test("sanitizeClientPayload uses parsed item defaults before sanitizing", () => {
  const input: ThreadItemPayloadInput = {
    id: "msg_defaulted",
    type: "user_message",
    thread_id: "thr_test",
    created_at: "2026-06-20T00:00:00.000Z",
    content: [{ type: "input_text", text: "hello" }],
  };
  const sanitized = sanitizeClientPayload(input);

  expectType<ClientThreadItem>(sanitized);
  assert.equal(sanitized.type, "user_message");
  assert.deepEqual(sanitized.attachments, []);
  assert.deepEqual(sanitized.inference_options, {});
});

test("sanitizeClientPayload uses parsed item defaults inside pages", () => {
  const input: ThreadItemPagePayloadInput = {
    data: [
      {
        id: "msg_defaulted",
        type: "user_message",
        thread_id: "thr_test",
        created_at: "2026-06-20T00:00:00.000Z",
        content: [{ type: "input_text", text: "hello" }],
      },
    ],
    has_more: false,
  };
  const sanitized = sanitizeClientPayload(input);

  expectType<ClientPage<ThreadItem, ClientThreadItem>>(sanitized);
  const item = sanitized.data[0];
  assert.equal(item?.type, "user_message");
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.deepEqual(item.attachments, []);
  assert.deepEqual(item.inference_options, {});
});

test("sanitizeClientPayload uses parsed item defaults inside events", () => {
  const input: ThreadStreamEventPayloadInput = {
    type: "thread.item.done",
    item: {
      id: "msg_defaulted",
      type: "user_message",
      thread_id: "thr_test",
      created_at: "2026-06-20T00:00:00.000Z",
      content: [{ type: "input_text", text: "hello" }],
    },
  };
  const sanitized = sanitizeClientPayload(input);

  expectType<ClientThreadStreamEvent>(sanitized);
  assert.equal(sanitized.type, "thread.item.done");
  if (sanitized.type !== "thread.item.done" || sanitized.item.type !== "user_message") {
    throw new Error("Expected user message event");
  }
  assert.deepEqual(sanitized.item.attachments, []);
  assert.deepEqual(sanitized.item.inference_options, {});
});

test("sanitizeClientPayload strips metadata from sync custom action response items", () => {
  const input: SyncCustomActionResponsePayloadInput & { custom: boolean } = {
    updated_item: userMessage,
    custom: true,
  };
  const sanitized = sanitizeClientPayload(input);

  expectType<ClientSyncCustomActionResponse>(sanitized);
  // @ts-expect-error Sync custom action response output is canonical and does not promise extra fields.
  sanitized.custom;
  const item = sanitized.updated_item;
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.equal("metadata" in item.attachments[0]!, false);
});

test("sanitizeClientPayload returns canonical sync custom action response for public type", () => {
  const input: SyncCustomActionResponse = {
    updated_item: userMessage,
  };
  const sanitized = sanitizeClientPayload(input);

  expectType<ClientSyncCustomActionResponse>(sanitized);
  const item = sanitized.updated_item;
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.equal("metadata" in item.attachments[0]!, false);
});

test("sanitizeClientPayload preserves nested thread item page fields", () => {
  const value = {
    ...threadResponse,
    items: {
      ...threadResponse.items,
      metadata: { keep: true },
    },
  };
  const sanitized = sanitizeClientPayload(value);
  const item = sanitized.items.data[0];

  assert.deepEqual((sanitized.items as typeof sanitized.items & { metadata?: unknown }).metadata, {
    keep: true,
  });
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.equal("metadata" in item.attachments[0]!, false);
});

test("sanitizeClientPayload preserves nested event thread item page fields", () => {
  const value = {
    type: "thread.created",
    thread: {
      ...threadResponse,
      items: {
        ...threadResponse.items,
        metadata: { keep: true },
      },
    },
  };
  const sanitized = sanitizeClientPayload(value);

  assert.equal(sanitized.type, "thread.created");
  if (sanitized.type !== "thread.created") {
    throw new Error("Expected thread.created event");
  }
  assert.deepEqual((sanitized.thread.items as typeof sanitized.thread.items & { metadata?: unknown }).metadata, {
    keep: true,
  });
});

test("sanitizeClientPayload preserves nested thread item page fields inside thread pages", () => {
  const value = {
    data: [
      {
        ...threadResponse,
        items: {
          ...threadResponse.items,
          metadata: { keep: true },
        },
      },
    ],
    has_more: false,
    metadata: { outer: true },
  };
  const sanitized = sanitizeClientPayload(value);

  expectType<ClientPage<typeof value, ClientThread>>(sanitized);
  assert.deepEqual(sanitized.metadata, { outer: true });
  const thread = sanitized.data[0];
  if (!thread) {
    throw new Error("Expected thread");
  }
  const item = thread.items.data[0];

  assert.deepEqual((thread.items as typeof thread.items & { metadata?: unknown }).metadata, {
    keep: true,
  });
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.equal("metadata" in item.attachments[0]!, false);
});

test("sanitizeClientPayload preserves attachment metadata on unsupported page types", () => {
  const value = {
    data: [attachment],
    has_more: false,
  };

  assert.deepEqual(sanitizeClientPayload(value), value);
});

test("sanitizeClientPayload preserves page-level fields on empty pages", () => {
  const value = {
    data: [],
    metadata: { keep: true },
  };

  assert.deepEqual(sanitizeClientPayload(value), value);
});

test("sanitizeClientPayload leaves invalid typed records unchanged", () => {
  assert.deepEqual(sanitizeClientPayload({ type: "user_message", metadata: { keep: true } }), {
    type: "user_message",
    metadata: { keep: true },
  });
  assert.deepEqual(sanitizeClientPayload({ type: "thread.created", metadata: { keep: true } }), {
    type: "thread.created",
    metadata: { keep: true },
  });
});

test("sanitizeClientPayload does not strip unrelated attachment-shaped records", () => {
  const value = { id: "not_attachment", mime_type: "text/plain", metadata: { keep: true } };

  assert.deepEqual(sanitizeClientPayload(value), value);
});

test("sanitizeClientPayload leaves invalid page-like payloads unchanged", () => {
  const value = {
    data: [{ type: "user_message", metadata: { keep: true } }],
    has_more: false,
  };

  assert.deepEqual(sanitizeClientPayload(value), value);
});

test("sanitizeClientPayload follows JSON serialization semantics for unknown payloads", () => {
  const input = {
    metadata: { keep: true },
    omitted: undefined,
    callback() {
      return "ignored";
    },
  };
  const sanitized = sanitizeClientPayload(input);

  expectType<typeof input>(sanitized);
  assert.deepEqual(sanitized, { metadata: { keep: true } });
});

test("sanitizeClientPayload keeps broad unrelated record type", () => {
  const input: Record<string, unknown> = { metadata: { keep: true } };
  const sanitized = sanitizeClientPayload(input);

  expectType<Record<string, unknown>>(sanitized);
  assert.deepEqual(sanitized, input);
});

test("sanitizeClientPayload keeps invalid updated item record type", () => {
  const input = { updated_item: "invalid", custom: true };
  const sanitized = sanitizeClientPayload(input);

  expectType<typeof input>(sanitized);
  assert.deepEqual(sanitized, input);
});
