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

test("nextWithAbort removes abort listener after operation rejects", async () => {
  const controller = new AbortController();
  const signal = controller.signal;
  const originalAdd = signal.addEventListener.bind(signal);
  const originalRemove = signal.removeEventListener.bind(signal);
  let abortListeners = 0;
  const expected = new Error("next failed");

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

  await assert.rejects(
    nextWithAbort(Promise.reject<IteratorResult<string>>(expected), signal),
    expected,
  );

  assert.equal(abortListeners, 0);
});

test("nextWithAbort observes late next rejection when signal is already aborted", async () => {
  const controller = new AbortController();
  controller.abort();
  const unhandled: unknown[] = [];
  const onUnhandledRejection = (reason: unknown) => {
    unhandled.push(reason);
  };

  process.on("unhandledRejection", onUnhandledRejection);
  try {
    const next = new Promise<IteratorResult<string>>((_, reject) => {
      setImmediate(() => reject(new Error("late next rejection")));
    });

    await assert.rejects(nextWithAbort(next, controller.signal), StreamCancelledError);
    await new Promise<void>((resolve) => setImmediate(resolve));
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.deepEqual(unhandled, []);
  } finally {
    process.off("unhandledRejection", onUnhandledRejection);
  }
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

test("returnIterator propagates iterator return errors", async () => {
  const expected = new Error("return failed");

  await assert.rejects(
    returnIterator({
      async next() {
        return { done: true, value: undefined };
      },
      async return() {
        throw expected;
      },
    }),
    expected,
  );
});
