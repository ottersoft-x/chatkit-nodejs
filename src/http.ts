import { ChatKitServer, NonStreamingResult, StreamingEventResult } from "./server.js";
import type { RunCoordinator } from "./run-coordinator.js";
import { decodeJsonBytes } from "./serialization.js";
import { serializeThreadStreamEventToSse } from "./stream-serialization.js";
import type { ThreadStreamEvent } from "./types/server.js";

export interface RunCoordinatorHandlerOptions<TContext> {
  getContext?: (request: Request) => TContext | Promise<TContext>;
  runCoordinator: RunCoordinator<TContext, ThreadStreamEvent>;
}

export interface ChatKitHandlerOptions<TContext> extends RunCoordinatorHandlerOptions<TContext> {}

export type ChatKitHandler = (request: Request) => Promise<Response>;

export function createChatKitHandler<TContext = undefined>(
  server: ChatKitServer<TContext>,
  options: ChatKitHandlerOptions<TContext>,
): ChatKitHandler {
  const handlerOptions = options as Partial<ChatKitHandlerOptions<TContext>> | undefined;

  return async (request) => {
    const receivedAt = new Date().toISOString();
    const context = handlerOptions?.getContext
      ? await handlerOptions.getContext(request)
      : (undefined as TContext);
    const rawRequestBuffer = await request.arrayBuffer();
    const rawRequestBytes = new Uint8Array(rawRequestBuffer);
    const result = await server.processRequest(rawRequestBuffer, context);

    if (result instanceof NonStreamingResult) {
      return new Response(new Uint8Array(result.json), {
        headers: {
          "content-type": "application/json",
        },
      });
    }

    if (result instanceof StreamingEventResult) {
      const runCoordinator = handlerOptions?.runCoordinator;

      if (!runCoordinator) {
        return jsonErrorResponse(
          500,
          "configuration_error",
          "createChatKitHandler requires runCoordinator for streaming requests.",
        );
      }

      const startResult = await runCoordinator.startRun({
        context,
        descriptor: {
          ...result.descriptorMetadata,
          receivedAt,
          rawRequest: new Uint8Array(rawRequestBytes),
        },
        source: (runtime) => result.stream(runtime),
      });

      if (startResult.status === "not_started") {
        return jsonErrorResponse(
          notStartedStatusCode(startResult.reason),
          startResult.reason,
          startResult.message ?? startResult.reason,
        );
      }

      return new Response(
        toReadableStream(startResult.subscription.events, {
          serializeEvent: (event) => server.serializeStreamingEventForHandler(event),
          onCancel: () => startResult.subscription.detach("subscriber_cancelled"),
        }),
        {
          headers: {
            "content-type": "text/event-stream",
            "cache-control": "no-cache",
            "x-chatkit-run-id": startResult.runId,
          },
        },
      );
    }

    const _exhaustive: never = result;
    return _exhaustive;
  };
}

export function createChatKitRunCancelHandler<TContext = undefined>(
  options: RunCoordinatorHandlerOptions<TContext>,
): ChatKitHandler {
  return async (request) => {
    const parsed = await parseRunIdRequest(request);
    if (!parsed.ok) {
      return parsed.response;
    }

    const context = options.getContext
      ? await options.getContext(request)
      : (undefined as TContext);
    const result = await options.runCoordinator.cancelRun({
      runId: parsed.runId,
      context,
    });

    switch (result.status) {
      case "cancelled":
      case "cancelling":
      case "already_finished":
        return jsonResponse(200, { status: result.status });
      case "not_found":
        return jsonErrorResponse(404, "not_found", "Run not found.");
      case "forbidden":
        return jsonErrorResponse(403, "forbidden", "Run access forbidden.");
    }
  };
}

export function createChatKitRunAttachHandler<TContext = undefined>(
  options: RunCoordinatorHandlerOptions<TContext>,
): ChatKitHandler {
  return async (request) => {
    const parsed = await parseRunIdRequest(request);
    if (!parsed.ok) {
      return parsed.response;
    }

    const context = options.getContext
      ? await options.getContext(request)
      : (undefined as TContext);
    const result = await options.runCoordinator.attachRun({
      runId: parsed.runId,
      context,
    });

    if (result.status === "attached") {
      return new Response(
        toReadableStream(result.subscription.events, {
          serializeEvent: serializeThreadStreamEventToSse,
          onCancel: () => result.subscription.detach("subscriber_cancelled"),
        }),
        {
          headers: {
            "content-type": "text/event-stream",
            "cache-control": "no-cache",
            "x-chatkit-run-id": result.runId,
          },
        },
      );
    }

    return jsonResponse(
      notAttachableStatusCode(result.reason),
      notAttachableResponseBody(result),
    );
  };
}

type ParsedRunIdRequest =
  | { ok: true; runId: string }
  | { ok: false; response: Response };

async function parseRunIdRequest(request: Request): Promise<ParsedRunIdRequest> {
  let value: unknown;

  try {
    value = decodeJsonBytes(await request.arrayBuffer());
  } catch {
    return invalidRunIdResponse();
  }

  if (!isRecord(value) || typeof value.run_id !== "string") {
    return invalidRunIdResponse();
  }

  const runId = value.run_id.trim();
  if (runId.length === 0) {
    return invalidRunIdResponse();
  }

  return { ok: true, runId };
}

function invalidRunIdResponse(): ParsedRunIdRequest {
  return {
    ok: false,
    response: jsonErrorResponse(
      400,
      "invalid_request",
      "Expected JSON body with string run_id.",
    ),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function jsonResponse(status: number, value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: {
      "content-type": "application/json",
    },
  });
}

function jsonErrorResponse(status: number, code: string, message: string): Response {
  return new Response(JSON.stringify({ error: { code, message } }), {
    status,
    headers: {
      "content-type": "application/json",
    },
  });
}

function notAttachableStatusCode(
  reason: "not_found" | "forbidden" | "finished" | "expired" | "unavailable",
): number {
  switch (reason) {
    case "not_found":
      return 404;
    case "forbidden":
      return 403;
    case "finished":
      return 409;
    case "expired":
      return 410;
    case "unavailable":
      return 503;
  }
}

function notAttachableMessage(
  reason: "not_found" | "forbidden" | "finished" | "expired" | "unavailable",
): string {
  switch (reason) {
    case "not_found":
      return "Run not found.";
    case "forbidden":
      return "Run access forbidden.";
    case "finished":
      return "Run has already finished.";
    case "expired":
      return "Run has expired.";
    case "unavailable":
      return "Run is unavailable.";
  }
}

function notAttachableResponseBody(result: {
  status: "not_attachable";
  reason: "not_found" | "forbidden" | "finished" | "expired" | "unavailable";
  message?: string;
  retryAfterMs?: number;
}): Record<string, unknown> {
  return {
    status: result.status,
    reason: result.reason,
    message: result.message ?? notAttachableMessage(result.reason),
    ...(result.retryAfterMs === undefined ? {} : { retryAfterMs: result.retryAfterMs }),
  };
}

function notStartedStatusCode(reason: "forbidden" | "conflict" | "unavailable"): number {
  switch (reason) {
    case "forbidden":
      return 403;
    case "conflict":
      return 409;
    case "unavailable":
      return 503;
  }
}

interface ReadableStreamOptions {
  onCancel?: () => Promise<void>;
  serializeEvent: (event: ThreadStreamEvent) => Uint8Array;
}

function toReadableStream(
  iterable: AsyncIterable<ThreadStreamEvent>,
  options: ReadableStreamOptions,
): ReadableStream<Uint8Array> {
  const iterator = iterable[Symbol.asyncIterator]();

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const next = await iterator.next();

      if (next.done) {
        controller.close();
        return;
      }

      controller.enqueue(options.serializeEvent(next.value));
    },
    async cancel() {
      const returned = iterator.return?.();
      try {
        await options.onCancel?.();
      } finally {
        await returned;
      }
    },
  });
}
