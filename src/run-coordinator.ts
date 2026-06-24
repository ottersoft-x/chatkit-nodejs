import type { ChatKitStreamRuntime } from "./stream-runtime.js";

export type RunStatus = "running" | "completed" | "failed" | "cancelled" | "expired";

export type RunDetachReason = "response_closed" | "request_aborted" | "subscriber_cancelled";

export interface RunStartDescriptor {
  requestType: string;
  threadId?: string;
  itemId?: string;
  receivedAt: string;
  rawRequest: Uint8Array;
}

export interface StartRunOptions<TContext, TEvent> {
  context: TContext;
  descriptor: RunStartDescriptor;
  source: (runtime: ChatKitStreamRuntime) => AsyncIterable<TEvent>;
}

export interface RunSubscription<TEvent> {
  events: AsyncIterable<TEvent>;
  detach(reason: RunDetachReason): Promise<void>;
}

export type StartRunResult<TEvent> =
  | {
      status: "started";
      runId: string;
      subscription: RunSubscription<TEvent>;
    }
  | {
      status: "not_started";
      reason: "forbidden" | "conflict" | "unavailable";
      message?: string;
    };

export interface AttachRunOptions<TContext> {
  runId: string;
  context: TContext;
}

export type AttachRunResult<TEvent> =
  | {
      status: "attached";
      runId: string;
      subscription: RunSubscription<TEvent>;
    }
  | {
      status: "not_attachable";
      reason: "not_found" | "forbidden" | "finished" | "expired" | "unavailable";
      message?: string;
      retryAfterMs?: number;
    };

export interface CancelRunOptions<TContext> {
  runId: string;
  context: TContext;
}

export type CancelRunResult =
  | { status: "cancelled" }
  | { status: "cancelling" }
  | { status: "already_finished" }
  | { status: "not_found" }
  | { status: "forbidden" };

export interface RunCoordinator<TContext, TEvent> {
  startRun(options: StartRunOptions<TContext, TEvent>): Promise<StartRunResult<TEvent>>;
  attachRun(options: AttachRunOptions<TContext>): Promise<AttachRunResult<TEvent>>;
  cancelRun(options: CancelRunOptions<TContext>): Promise<CancelRunResult>;
}
