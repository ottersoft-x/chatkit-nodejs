import type { StoreItemType } from "../store";
import { defaultGenerateId } from "../store";
import type { ThreadItem, ThreadMetadata } from "../types/core";
import type { ThreadStreamEvent } from "../types/server";
import { diffWidget } from "./diff";
import { serializeWidget } from "./serialization";
import type { WidgetRoot } from "./types";

export interface StreamWidgetOptions {
  copyText?: string | null;
  generateId?: (itemType: StoreItemType) => string;
  now?: () => string;
}

type ResolvedStreamWidgetOptions = Required<Pick<StreamWidgetOptions, "generateId" | "now">> &
  Pick<StreamWidgetOptions, "copyText">;

function isAsyncIterable(value: unknown): value is AsyncIterable<WidgetRoot> {
  return (
    value != null &&
    typeof (value as Partial<AsyncIterable<WidgetRoot>>)[Symbol.asyncIterator] === "function"
  );
}

function makeWidgetItem(
  thread: ThreadMetadata,
  itemId: string,
  widget: WidgetRoot,
  options: ResolvedStreamWidgetOptions,
): ThreadItem {
  return {
    id: itemId,
    type: "widget",
    thread_id: thread.id,
    created_at: options.now(),
    widget: serializeWidget(widget),
    copy_text: options.copyText ?? undefined,
  };
}

export async function* streamWidget(
  thread: ThreadMetadata,
  widgetOrAsyncIterable: WidgetRoot | AsyncIterable<WidgetRoot>,
  options: StreamWidgetOptions = {},
): AsyncIterable<ThreadStreamEvent> {
  const resolvedOptions: ResolvedStreamWidgetOptions = {
    generateId: options.generateId ?? defaultGenerateId,
    now: options.now ?? (() => new Date().toISOString()),
    copyText: options.copyText,
  };
  const itemId = resolvedOptions.generateId("message");

  if (!isAsyncIterable(widgetOrAsyncIterable)) {
    yield {
      type: "thread.item.done",
      item: makeWidgetItem(thread, itemId, widgetOrAsyncIterable, resolvedOptions),
    };
    return;
  }

  const iterator = widgetOrAsyncIterable[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) {
    throw new Error("streamWidget async iterable must yield an initial widget.");
  }

  let lastState = first.value;
  yield {
    type: "thread.item.added",
    item: makeWidgetItem(thread, itemId, lastState, resolvedOptions),
  };

  for (;;) {
    const next = await iterator.next();
    if (next.done) break;

    for (const update of diffWidget(lastState, next.value)) {
      yield {
        type: "thread.item.updated",
        item_id: itemId,
        update,
      };
    }
    lastState = next.value;
  }

  yield {
    type: "thread.item.done",
    item: makeWidgetItem(thread, itemId, lastState, resolvedOptions),
  };
}
