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

  signal.addEventListener = ((
    type: string,
    listener: EventListenerOrEventListenerObject,
    options?: boolean | AddEventListenerOptions,
  ) => {
    if (type === "abort") abortListeners++;
    return originalAdd(type, listener, options);
  }) as typeof signal.addEventListener;
  signal.removeEventListener = ((
    type: string,
    listener: EventListenerOrEventListenerObject,
    options?: boolean | EventListenerOptions,
  ) => {
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
