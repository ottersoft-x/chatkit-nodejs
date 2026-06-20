import { omitUndefinedDeep } from "./serialization.js";
import { AttachmentSchema, PageSchema, ThreadItemSchema, type Attachment, type Page, type ThreadItem } from "./types/core.js";
import {
  SyncCustomActionResponseSchema,
  ThreadSchema,
  ThreadStreamEventSchema,
  type SyncCustomActionResponse,
  type Thread,
  type ThreadStreamEvent,
} from "./types/server.js";
import type { z } from "zod";

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
export type AttachmentPayloadInput = z.input<typeof AttachmentSchema>;
export type ThreadItemPayloadInput = z.input<typeof ThreadItemSchema>;
export type ThreadPayloadInput = z.input<typeof ThreadSchema>;
export type ThreadStreamEventPayloadInput = z.input<typeof ThreadStreamEventSchema>;
export type SyncCustomActionResponsePayloadInput = z.input<typeof SyncCustomActionResponseSchema>;
export type ThreadItemPagePayloadInput = {
  data: ThreadItemPayloadInput[];
  has_more?: boolean | undefined;
  after?: string | null | undefined;
};
export type ThreadPagePayloadInput = {
  data: ThreadPayloadInput[];
  has_more?: boolean | undefined;
  after?: string | null | undefined;
};
type PagePayloadLike = {
  data: readonly unknown[];
  has_more?: boolean | undefined;
};

export type ClientAttachment<TAttachment extends Attachment = Attachment> =
  TAttachment extends Attachment ? Omit<TAttachment, "metadata"> : never;

export type ClientThreadItem<TItem extends ThreadItem = ThreadItem> =
  TItem extends UserMessageItem
    ? Omit<TItem, "attachments"> & { attachments: ClientAttachment<TItem["attachments"][number]>[] }
    : TItem;

export type ClientPage<TPageOrItem, TClientItem = TPageOrItem> =
  TPageOrItem extends PagePayloadLike
    ? Omit<TPageOrItem, "data" | "has_more"> & { data: TClientItem[]; has_more: boolean }
    : Omit<Page<TPageOrItem>, "data"> & { data: TClientItem[] };

export type ClientThread<TThread extends Thread = Thread> = Omit<TThread, "items"> & {
  items: ClientPage<TThread["items"], ClientThreadItem>;
};

export type ClientThreadStreamEvent<TEvent extends ThreadStreamEvent = ThreadStreamEvent> =
  TEvent extends ItemBearingEvent
    ? Omit<TEvent, "item"> & { item: ClientThreadItem<TEvent["item"]> }
    : TEvent extends ThreadBearingEvent
      ? Omit<TEvent, "thread"> & { thread: ClientThread<TEvent["thread"]> }
      : TEvent;

export type ClientSyncCustomActionResponse = {
  updated_item?: ClientThreadItem | null;
};

export type ClientPayload<T> =
  T extends AttachmentPayloadInput
    ? ClientAttachment
    : T extends ThreadItemPayloadInput
      ? ClientThreadItem
      : T extends ThreadPayloadInput
        ? ClientThread
        : T extends ThreadStreamEventPayloadInput
          ? ClientThreadStreamEvent
          : T extends SyncCustomActionResponsePayloadInput
            ? ClientSyncCustomActionResponse
            : T extends { data: readonly (infer TItem)[]; has_more?: boolean | undefined; after?: string | null | undefined }
              ? [TItem] extends [never]
                ? T
                : T extends ThreadItemPagePayloadInput
                  ? ClientPage<T, ClientThreadItem>
                  : T extends ThreadPagePayloadInput
                    ? ClientPage<T, ClientThread>
                    : T
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

function isPageRecord(
  value: unknown,
): value is { data: unknown[]; has_more?: boolean | undefined; after?: string | null | undefined } & Record<
  string,
  unknown
> {
  return (
    isRecord(value) &&
    Array.isArray(value.data) &&
    (!("has_more" in value) || typeof value.has_more === "boolean" || value.has_more === undefined) &&
    (!("after" in value) ||
      typeof value.after === "string" ||
      value.after === null ||
      value.after === undefined)
  );
}

function mergeParsedPage<TItem>(original: unknown, parsed: Page<TItem>): Page<TItem> {
  if (!isPageRecord(original)) {
    return parsed;
  }

  return {
    ...jsonClone(original as Page<unknown> & Record<string, unknown>),
    ...parsed,
  };
}

function mergeParsedThread(original: unknown, parsed: Thread): Thread {
  if (!isRecord(original) || !isPageRecord(original.items)) {
    return parsed;
  }

  return {
    ...parsed,
    items: mergeParsedPage(original.items, parsed.items),
  };
}

function mergeParsedThreadPage(original: unknown, parsed: Page<Thread>): Page<Thread> {
  const mergedPage = mergeParsedPage(original, parsed);
  if (!isPageRecord(original)) {
    return mergedPage;
  }

  return {
    ...mergedPage,
    data: parsed.data.map((thread, index) => mergeParsedThread(original.data[index], thread)),
  };
}

function mergeParsedThreadStreamEvent(
  original: unknown,
  parsed: ThreadStreamEvent,
): ThreadStreamEvent {
  if (!isRecord(original)) {
    return parsed;
  }

  if ((parsed.type === "thread.created" || parsed.type === "thread.updated") && isRecord(original.thread)) {
    return {
      ...parsed,
      thread: mergeParsedThread(original.thread, parsed.thread),
    };
  }

  return parsed;
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
  } as unknown as ClientThread<TThread>;
}

export function sanitizePage<TPage extends Page<unknown>, TClientItem>(
  page: TPage,
  sanitizeData: (value: TPage["data"][number]) => TClientItem,
): ClientPage<TPage, TClientItem> {
  return {
    ...jsonClone(page as TPage & Record<string, unknown>),
    data: page.data.map((item) => sanitizeData(item)),
  } as unknown as ClientPage<TPage, TClientItem>;
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

export function sanitizeSyncCustomActionResponse<TResponse extends SyncCustomActionResponse>(
  response: TResponse,
): ClientSyncCustomActionResponse {
  const clone = jsonClone(response as TResponse & Record<string, unknown>);
  if (!response.updated_item) {
    return clone as ClientSyncCustomActionResponse;
  }

  return {
    ...clone,
    updated_item: sanitizeThreadItem(response.updated_item),
  } as ClientSyncCustomActionResponse;
}

export function sanitizeClientPayload<T>(value: T): ClientPayload<T> {
  const attachment = AttachmentSchema.safeParse(value);
  if (attachment.success) {
    return sanitizeAttachment(attachment.data) as ClientPayload<T>;
  }

  const threadItemPage = isPageRecord(value) ? ThreadItemPageSchema.safeParse(value) : null;
  if (threadItemPage?.success) {
    return sanitizePage(mergeParsedPage(value, threadItemPage.data), sanitizeThreadItem) as ClientPayload<T>;
  }

  const threadPage = isPageRecord(value) ? ThreadPageSchema.safeParse(value) : null;
  if (threadPage?.success) {
    return sanitizePage(mergeParsedThreadPage(value, threadPage.data), sanitizeThreadResponse) as ClientPayload<T>;
  }

  const threadItem = ThreadItemSchema.safeParse(value);
  if (threadItem.success) {
    return sanitizeThreadItem(threadItem.data) as ClientPayload<T>;
  }

  const thread = ThreadSchema.safeParse(value);
  if (thread.success) {
    return sanitizeThreadResponse(mergeParsedThread(value, thread.data)) as ClientPayload<T>;
  }

  const event = ThreadStreamEventSchema.safeParse(value);
  if (event.success) {
    return sanitizeThreadStreamEvent(mergeParsedThreadStreamEvent(value, event.data)) as ClientPayload<T>;
  }

  const syncCustomActionResponse =
    isRecord(value) && "updated_item" in value ? SyncCustomActionResponseSchema.safeParse(value) : null;
  if (syncCustomActionResponse?.success) {
    return sanitizeSyncCustomActionResponse({
      ...jsonClone(value as Record<string, unknown>),
      ...syncCustomActionResponse.data,
    } as SyncCustomActionResponse & Record<string, unknown>) as ClientPayload<T>;
  }

  return jsonClone(value) as ClientPayload<T>;
}
