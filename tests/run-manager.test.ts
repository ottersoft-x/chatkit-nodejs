import test from "node:test";
import { strict as assert } from "node:assert";

import { ResponseRunManager } from "../src/run-manager.js";
import { StreamCancelledError } from "../src/stream-runtime.js";

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) {
    values.push(value);
  }
  return values;
}

test("subscriber cancellation detaches without stopping the run", async () => {
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const { run, subscription: subscriber } = await manager.startRunAndSubscribe({
    context: { userId: "user_1" },
    source: async function* () {
      yield "first";
      await released;
      yield "second";
    },
  });

  const iterator = subscriber.events[Symbol.asyncIterator]();

  assert.deepEqual(await iterator.next(), { done: false, value: "first" });
  await iterator.return?.();
  release();
  await run.completed;

  assert.equal(run.status, "completed");
});

test("initial subscriber receives synchronously yielded events", async () => {
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const { run, subscription } = await manager.startRunAndSubscribe({
    context: { userId: "user_1" },
    source: async function* () {
      yield "thread.created";
      yield "thread.item.done";
    },
  });

  assert.deepEqual(await collect(subscription.events), ["thread.created", "thread.item.done"]);
  await run.completed;
});

test("new subscribers can attach by run id and receive later events", async () => {
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const { run, subscription: first } = await manager.startRunAndSubscribe({
    context: { userId: "user_1" },
    source: async function* () {
      yield "first";
      await released;
      yield "second";
    },
  });

  const firstIterator = first.events[Symbol.asyncIterator]();
  assert.deepEqual(await firstIterator.next(), { done: false, value: "first" });
  await firstIterator.return?.();

  const second = await manager.subscribe({ runId: run.runId, context: { userId: "user_1" } });
  release();

  assert.deepEqual(await collect(second.events), ["second"]);
});

test("simultaneous runs require their own run ids", async () => {
  let releaseFirst!: () => void;
  let releaseSecond!: () => void;
  const firstReleased = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const secondReleased = new Promise<void>((resolve) => {
    releaseSecond = resolve;
  });
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const firstRun = await manager.startRun({
    context: { userId: "user_1" },
    source: async function* () {
      await firstReleased;
      yield "first-run";
    },
  });
  const secondRun = await manager.startRun({
    context: { userId: "user_1" },
    source: async function* () {
      await secondReleased;
      yield "second-run";
    },
  });

  const first = await manager.subscribe({ runId: firstRun.runId, context: { userId: "user_1" } });
  const second = await manager.subscribe({ runId: secondRun.runId, context: { userId: "user_1" } });
  releaseFirst();
  releaseSecond();

  assert.deepEqual(await collect(first.events), ["first-run"]);
  assert.deepEqual(await collect(second.events), ["second-run"]);
  assert.notEqual(firstRun.runId, secondRun.runId);
});

test("cancelRun is scoped and idempotent", async () => {
  let aborted = false;
  let returned = false;
  const manager = new ResponseRunManager<{ userId: string }, string>({
    getRunScope: (context) => context.userId,
  });
  const run = await manager.startRun({
    context: { userId: "user_1" },
    source: ({ signal }) => ({
      [Symbol.asyncIterator](): AsyncIterator<string> {
        signal.addEventListener("abort", () => {
          aborted = true;
        });

        return {
          async next() {
            await new Promise<void>(() => {});
            return { done: true, value: undefined };
          },
          async return() {
            returned = true;
            throw new StreamCancelledError();
          },
        };
      },
    }),
  });

  assert.deepEqual(
    await manager.cancelRun({ runId: run.runId, context: { userId: "user_2" } }),
    { status: "forbidden" },
  );
  assert.equal(aborted, false);

  const result = await manager.cancelRun({ runId: run.runId, context: { userId: "user_1" } });
  assert.deepEqual(result, { status: "cancelled" });
  assert.equal(aborted, true);
  assert.equal(returned, true);

  assert.deepEqual(
    await manager.cancelRun({ runId: "missing", context: { userId: "user_1" } }),
    { status: "not_found" },
  );
});

test("cancelRun does not wait for async generator return behind pending next", async () => {
  const manager = new ResponseRunManager<undefined, string>();
  const run = await manager.startRun({
    context: undefined,
    source: async function* () {
      await new Promise<void>(() => {});
      yield "late";
    },
  });

  const result = await Promise.race([
    manager.cancelRun({ runId: run.runId, context: undefined }),
    new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 50)),
  ]);

  assert.deepEqual(result, { status: "cancelled" });
  assert.equal(run.status, "cancelled");
});
