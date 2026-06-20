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

  assert.equal("metadata" in sanitized.data[0]!.attachments[0]!, false);
});

test("sanitizeClientPayload handles pages with undefined after cursor", () => {
  const sanitized = sanitizeClientPayload({
    data: [userMessage],
    has_more: false,
    after: undefined,
  });

  assert.equal("metadata" in sanitized.data[0]!.attachments[0]!, false);
});

test("sanitizeClientPayload uses parsed item defaults before sanitizing", () => {
  const sanitized = sanitizeClientPayload({
    id: "msg_defaulted",
    type: "user_message",
    thread_id: "thr_test",
    created_at: "2026-06-20T00:00:00.000Z",
    content: [{ type: "input_text", text: "hello" }],
  }) as Extract<ThreadItem, { type: "user_message" }>;

  assert.equal(sanitized.type, "user_message");
  assert.deepEqual(sanitized.attachments, []);
  assert.deepEqual(sanitized.inference_options, {});
});

test("sanitizeClientPayload uses parsed item defaults inside pages", () => {
  const sanitized = sanitizeClientPayload({
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
  }) as unknown as { data: Array<Extract<ThreadItem, { type: "user_message" }>> };

  const item = sanitized.data[0];
  assert.equal(item?.type, "user_message");
  if (!item || item.type !== "user_message") {
    throw new Error("Expected user message");
  }
  assert.deepEqual(item.attachments, []);
  assert.deepEqual(item.inference_options, {});
});

test("sanitizeClientPayload uses parsed item defaults inside events", () => {
  const sanitized = sanitizeClientPayload({
    type: "thread.item.done",
    item: {
      id: "msg_defaulted",
      type: "user_message",
      thread_id: "thr_test",
      created_at: "2026-06-20T00:00:00.000Z",
      content: [{ type: "input_text", text: "hello" }],
    },
  }) as Extract<ThreadStreamEvent, { type: "thread.item.done" }>;

  assert.equal(sanitized.type, "thread.item.done");
  if (sanitized.type !== "thread.item.done" || sanitized.item.type !== "user_message") {
    throw new Error("Expected user message event");
  }
  assert.deepEqual(sanitized.item.attachments, []);
  assert.deepEqual(sanitized.item.inference_options, {});
});

test("sanitizeClientPayload preserves attachment metadata on unsupported page types", () => {
  const value = {
    data: [attachment],
    has_more: false,
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
  const sanitized = sanitizeClientPayload({
    metadata: { keep: true },
    omitted: undefined,
    callback() {
      return "ignored";
    },
  });

  assert.deepEqual(sanitized, { metadata: { keep: true } });
});
