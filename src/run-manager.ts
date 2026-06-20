import {
  StreamCancelledError,
  nextWithAbort,
  returnIterator,
  type ChatKitStreamRuntime,
} from "./stream-runtime.js";

export type DisconnectBehavior = "continue" | "cancel";

export type CancelRunResult =
  | { status: "cancelled" }
  | { status: "cancelling" }
  | { status: "already_finished" }
  | { status: "not_found" }
  | { status: "forbidden" };

export interface ResponseRunManagerOptions<TContext> {
  getRunScope?: (context: TContext) => string | Promise<string>;
}

export interface StartRunOptions<TContext, TEvent> {
  context: TContext;
  source: (runtime: ChatKitStreamRuntime) => AsyncIterable<TEvent>;
  supportsExplicitCancel?: boolean;
}

export interface SubscribeRunOptions<TContext> {
  runId: string;
  context: TContext;
}

export interface CancelRunOptions<TContext> {
  runId: string;
  context: TContext;
}

class AsyncEventQueue<T> implements AsyncIterable<T> {
  private readonly values: Array<IteratorYieldResult<T>> = [];
  private readonly waiting: Array<(result: IteratorResult<T>) => void> = [];
  private closed = false;

  push(value: T): void {
    if (this.closed) {
      return;
    }

    const resolve = this.waiting.shift();
    if (resolve) {
      resolve({ done: false, value });
      return;
    }

    this.values.push({ done: false, value });
  }

  close(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    for (const resolve of this.waiting.splice(0)) {
      resolve({ done: true, value: undefined });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        const result = this.values.shift();
        if (result) {
          return result;
        }

        if (this.closed) {
          return { done: true, value: undefined };
        }

        return new Promise((resolve) => {
          this.waiting.push(resolve);
        });
      },
      return: async () => {
        this.close();
        return { done: true, value: undefined };
      },
    };
  }
}

type RunStatus = "running" | "completed" | "failed" | "cancelled";

interface ResponseRunRecord<TEvent> {
  runId: string;
  scope: string | null;
  controller: AbortController;
  status: RunStatus;
  sourceIterator?: AsyncIterator<TEvent>;
  subscribers: Set<AsyncEventQueue<TEvent>>;
  completed: Promise<void>;
  resolveCompleted: () => void;
  rejectCompleted: (error: unknown) => void;
}

export interface ResponseRunHandle {
  runId: string;
  readonly status: RunStatus;
  completed: Promise<void>;
}

export interface ResponseRunSubscription<TEvent> {
  events: AsyncIterable<TEvent>;
}

export interface ResponseRunStart<TEvent> {
  run: ResponseRunHandle;
  subscription: ResponseRunSubscription<TEvent>;
}

export class ResponseRunManager<TContext = unknown, TEvent = Uint8Array> {
  private readonly runs = new Map<string, ResponseRunRecord<TEvent>>();

  constructor(private readonly options: ResponseRunManagerOptions<TContext> = {}) {}

  async startRun(options: StartRunOptions<TContext, TEvent>): Promise<ResponseRunHandle> {
    return (await this.startRunInternal(options, null)).run;
  }

  async startRunAndSubscribe(
    options: StartRunOptions<TContext, TEvent>,
  ): Promise<ResponseRunStart<TEvent>> {
    return this.startRunInternal(options, new AsyncEventQueue<TEvent>());
  }

  async subscribe(options: SubscribeRunOptions<TContext>): Promise<ResponseRunSubscription<TEvent>> {
    const record = this.runs.get(options.runId);
    if (!record || record.status !== "running") {
      return { events: this.closedIterable() };
    }

    if (!(await this.canAccess(record, options.context))) {
      return { events: this.closedIterable() };
    }

    const queue = new AsyncEventQueue<TEvent>();
    record.subscribers.add(queue);

    return this.subscriptionFor(record, queue);
  }

  async cancelRun(options: CancelRunOptions<TContext>): Promise<CancelRunResult> {
    const record = this.runs.get(options.runId);
    if (!record) {
      return { status: "not_found" };
    }

    if (!(await this.canAccess(record, options.context))) {
      return { status: "forbidden" };
    }

    if (record.status === "completed" || record.status === "failed") {
      return { status: "already_finished" };
    }

    if (record.status === "cancelled") {
      await record.completed.catch(() => undefined);
      return { status: "cancelled" };
    }

    record.status = "cancelled";
    record.controller.abort();
    this.closeSubscribers(record);
    await record.completed.catch(() => undefined);

    return { status: "cancelled" };
  }

  private async startRunInternal(
    options: StartRunOptions<TContext, TEvent>,
    initialSubscriber: AsyncEventQueue<TEvent> | null,
  ): Promise<ResponseRunStart<TEvent>> {
    const runId = `run_${crypto.randomUUID().replaceAll("-", "")}`;
    const controller = new AbortController();
    const scope = await this.scopeFor(options.context);
    let resolveCompleted!: () => void;
    let rejectCompleted!: (error: unknown) => void;
    const completed = new Promise<void>((resolve, reject) => {
      resolveCompleted = resolve;
      rejectCompleted = reject;
    });
    const record: ResponseRunRecord<TEvent> = {
      runId,
      scope,
      controller,
      status: "running",
      subscribers: initialSubscriber ? new Set([initialSubscriber]) : new Set(),
      completed,
      resolveCompleted,
      rejectCompleted,
    };
    const run: ResponseRunHandle = {
      runId,
      get status() {
        return record.status;
      },
      completed,
    };

    void completed.catch(() => {});
    this.runs.set(runId, record);
    void this.drainRun(record, options.source, options.supportsExplicitCancel ?? true);

    return {
      run,
      subscription: initialSubscriber
        ? this.subscriptionFor(record, initialSubscriber)
        : { events: this.closedIterable() },
    };
  }

  private subscriptionFor(
    record: ResponseRunRecord<TEvent>,
    queue: AsyncEventQueue<TEvent>,
  ): ResponseRunSubscription<TEvent> {
    return {
      events: {
        [Symbol.asyncIterator]: () => {
          const iterator = queue[Symbol.asyncIterator]();
          return {
            next: () => iterator.next(),
            return: async () => {
              record.subscribers.delete(queue);
              await iterator.return?.();
              return { done: true, value: undefined };
            },
          };
        },
      },
    };
  }

  private async drainRun(
    record: ResponseRunRecord<TEvent>,
    source: (runtime: ChatKitStreamRuntime) => AsyncIterable<TEvent>,
    supportsExplicitCancel: boolean,
  ): Promise<void> {
    let terminalStatus: RunStatus = "completed";
    let terminalError: unknown;

    try {
      const runtime: ChatKitStreamRuntime = {
        signal: record.controller.signal,
        supportsExplicitCancel,
      };
      const iterator = source(runtime)[Symbol.asyncIterator]();
      record.sourceIterator = iterator;

      while (true) {
        const next = await nextWithAbort(iterator.next(), record.controller.signal);
        if (next.done) {
          break;
        }

        for (const subscriber of record.subscribers) {
          subscriber.push(next.value);
        }
      }

      terminalStatus = record.status === "cancelled" ? "cancelled" : "completed";
    } catch (error) {
      if (error instanceof StreamCancelledError || record.controller.signal.aborted) {
        terminalStatus = "cancelled";
      } else {
        terminalStatus = "failed";
        terminalError = error;
      }
    } finally {
      if (record.sourceIterator && terminalStatus !== "completed") {
        const sourceReturn = returnIterator(record.sourceIterator);
        if (terminalStatus === "cancelled" || record.controller.signal.aborted) {
          void sourceReturn.catch(() => undefined);
        } else {
          try {
            await sourceReturn;
          } catch (error) {
            if (!(error instanceof StreamCancelledError || record.controller.signal.aborted)) {
              terminalStatus = "failed";
              terminalError = error;
            }
          }
        }
      }

      record.sourceIterator = undefined;
      record.status = terminalStatus;
      this.closeSubscribers(record);
      this.runs.delete(record.runId);

      if (terminalStatus === "failed") {
        record.rejectCompleted(terminalError);
      } else {
        record.resolveCompleted();
      }
    }
  }

  private closeSubscribers(record: ResponseRunRecord<TEvent>): void {
    for (const subscriber of record.subscribers) {
      subscriber.close();
    }
    record.subscribers.clear();
  }

  private closedIterable(): AsyncIterable<TEvent> {
    return {
      [Symbol.asyncIterator](): AsyncIterator<TEvent> {
        return {
          async next() {
            return { done: true, value: undefined };
          },
        };
      },
    };
  }

  private async scopeFor(context: TContext): Promise<string | null> {
    return this.options.getRunScope ? await this.options.getRunScope(context) : null;
  }

  private async canAccess(record: ResponseRunRecord<TEvent>, context: TContext): Promise<boolean> {
    if (record.scope == null) {
      return true;
    }

    return record.scope === (await this.scopeFor(context));
  }
}
