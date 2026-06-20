import { omitUndefinedDeep } from "./serialization.js";
import { AttachmentSchema, PageSchema, ThreadItemSchema, type Attachment, type Page, type ThreadItem } from "./types/core.js";
import { ThreadSchema, ThreadStreamEventSchema, type Thread, type ThreadStreamEvent } from "./types/server.js";

const ThreadItemPageSchema = PageSchema(ThreadItemSchema);
const ThreadPageSchema = PageSchema(ThreadSchema);

type UserMessageItem = Extract<ThreadItem, { type: "user_message" }>;
type ItemBearingEvent = Extract<
  ThreadStreamEvent,
  { type: "thread.item.added" | "thread.item.done" | "thread.item.replaced" }
>;
type ThreadBearingEvent = Extract<
  ThreadStreamEvent,
  { type: "thread.created" | "thread.updated" }
>;

export type ClientAttachment<TAttachment extends Attachment = Attachment> =
  TAttachment extends Attachment ? Omit<TAttachment, "metadata"> : never;

export type ClientThreadItem<TItem extends ThreadItem = ThreadItem> =
  TItem extends UserMessageItem
    ? Omit<TItem, "attachments"> & { attachments: ClientAttachment<TItem["attachments"][number]>[] }
    : TItem;

export type ClientPage<TItem, TClientItem = TItem> = Omit<Page<TItem>, "data"> & {
  data: TClientItem[];
};

export type ClientThread<TThread extends Thread = Thread> = Omit<TThread, "items"> & {
  items: ClientPage<ThreadItem, ClientThreadItem>;
};

export type ClientThreadStreamEvent<TEvent extends ThreadStreamEvent = ThreadStreamEvent> =
  TEvent extends ItemBearingEvent
    ? Omit<TEvent, "item"> & { item: ClientThreadItem<TEvent["item"]> }
    : TEvent extends ThreadBearingEvent
      ? Omit<TEvent, "thread"> & { thread: ClientThread<TEvent["thread"]> }
      : TEvent;

export type ClientPayload<T> =
  T extends Attachment
    ? ClientAttachment<T>
    : T extends ThreadItem
      ? ClientThreadItem<T>
      : T extends Thread
        ? ClientThread<T>
        : T extends ThreadStreamEvent
          ? ClientThreadStreamEvent<T>
          : T extends Page<infer TItem>
            ? ClientPage<TItem, ClientPayload<TItem>>
            : T;

function jsonClone<T>(value: T): T {
  const json = JSON.stringify(omitUndefinedDeep(value));
  if (json === undefined) {
    return undefined as T;
  }
  return JSON.parse(json) as T;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPageRecord(value: unknown): value is Page<unknown> {
  return (
    isRecord(value) &&
    Array.isArray(value.data) &&
    typeof value.has_more === "boolean" &&
    (!("after" in value) || typeof value.after === "string" || value.after === null)
  );
}

export function sanitizeAttachment<TAttachment extends Attachment>(
  attachment: TAttachment,
): ClientAttachment<TAttachment> {
  const { metadata: _metadata, ...rest } = jsonClone(attachment);
  return rest as ClientAttachment<TAttachment>;
}

export function sanitizeThreadItem<TItem extends ThreadItem>(item: TItem): ClientThreadItem<TItem> {
  if (item.type !== "user_message") {
    return jsonClone(item) as ClientThreadItem<TItem>;
  }

  const clone = jsonClone(item as UserMessageItem);
  return {
    ...clone,
    attachments: clone.attachments.map((attachment) => sanitizeAttachment(attachment)),
  } as ClientThreadItem<TItem>;
}

export function sanitizeThreadResponse<TThread extends Thread>(thread: TThread): ClientThread<TThread> {
  const clone = jsonClone(thread);
  return {
    ...clone,
    items: sanitizePage(thread.items, sanitizeThreadItem),
  } as ClientThread<TThread>;
}

export function sanitizePage<TItem, TClientItem>(
  page: Page<TItem>,
  sanitizeData: (value: TItem) => TClientItem,
): ClientPage<TItem, TClientItem> {
  return {
    ...jsonClone(page),
    data: page.data.map((item) => sanitizeData(item)),
  };
}

export function sanitizeThreadStreamEvent<TEvent extends ThreadStreamEvent>(
  event: TEvent,
): ClientThreadStreamEvent<TEvent> {
  if (
    event.type === "thread.item.added" ||
    event.type === "thread.item.done" ||
    event.type === "thread.item.replaced"
  ) {
    return { ...jsonClone(event), item: sanitizeThreadItem(event.item) } as ClientThreadStreamEvent<TEvent>;
  }

  if (event.type === "thread.created" || event.type === "thread.updated") {
    return { ...jsonClone(event), thread: sanitizeThreadResponse(event.thread) } as ClientThreadStreamEvent<TEvent>;
  }

  return jsonClone(event) as ClientThreadStreamEvent<TEvent>;
}

export function sanitizeClientPayload<T>(value: T): ClientPayload<T> {
  const attachment = AttachmentSchema.safeParse(value);
  if (attachment.success) {
    return sanitizeAttachment(value as Attachment) as ClientPayload<T>;
  }

  const threadItemPage = isPageRecord(value) ? ThreadItemPageSchema.safeParse(value) : null;
  if (threadItemPage?.success) {
    return sanitizePage(value as Page<ThreadItem>, sanitizeThreadItem) as ClientPayload<T>;
  }

  const threadPage = isPageRecord(value) ? ThreadPageSchema.safeParse(value) : null;
  if (threadPage?.success) {
    return sanitizePage(value as Page<Thread>, sanitizeThreadResponse) as ClientPayload<T>;
  }

  const threadItem = ThreadItemSchema.safeParse(value);
  if (threadItem.success) {
    return sanitizeThreadItem(value as ThreadItem) as ClientPayload<T>;
  }

  const thread = ThreadSchema.safeParse(value);
  if (thread.success) {
    return sanitizeThreadResponse(value as Thread) as ClientPayload<T>;
  }

  const event = ThreadStreamEventSchema.safeParse(value);
  if (event.success) {
    return sanitizeThreadStreamEvent(value as ThreadStreamEvent) as ClientPayload<T>;
  }

  return jsonClone(value) as ClientPayload<T>;
}
