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
    "after" in value
  );
}

function isAttachment(value: unknown): value is Attachment {
  return isRecord(value) && typeof value.id === "string" && typeof value.mime_type === "string";
}

export function sanitizeAttachment<TAttachment extends Attachment>(
  attachment: TAttachment,
): TAttachment {
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
  } as TThread;
}

export function sanitizePage<T>(page: Page<T>, sanitizeData: (value: T) => T): Page<T> {
  return {
    ...page,
    data: page.data.map((item) => sanitizeData(item)),
  };
}

export function sanitizeThreadStreamEvent<TEvent extends ThreadStreamEvent>(
  event: TEvent,
): TEvent {
  if (
    event.type === "thread.item.added" ||
    event.type === "thread.item.done" ||
    event.type === "thread.item.replaced"
  ) {
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
    return sanitizeThreadItem(value as unknown as ThreadItem) as T;
  }

  if (isRecord(value) && "items" in value && isPage(value.items)) {
    return sanitizeThreadResponse(value as unknown as Thread) as T;
  }

  if (isRecord(value) && typeof value.type === "string") {
    return sanitizeThreadStreamEvent(value as unknown as ThreadStreamEvent) as T;
  }

  return structuredClone(value);
}
