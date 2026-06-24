import { ChatKitServer, NonStreamingResult, StreamingEventResult } from "./server.js";
import type { RunCoordinator } from "./run-coordinator.js";
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

function jsonErrorResponse(status: number, code: string, message: string): Response {
  return new Response(JSON.stringify({ error: { code, message } }), {
    status,
    headers: {
      "content-type": "application/json",
    },
  });
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
