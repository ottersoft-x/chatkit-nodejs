import { ResponseRunManager, type DisconnectBehavior } from "./run-manager.js";
import { ChatKitServer, NonStreamingResult, StreamingEventResult } from "./server.js";
import type { ThreadStreamEvent } from "./types/server.js";

export interface ChatKitHandlerOptions<TContext> {
  getContext?: (request: Request) => TContext | Promise<TContext>;
  runManager?: ResponseRunManager<TContext, ThreadStreamEvent>;
  disconnectBehavior?: DisconnectBehavior;
  supportsExplicitCancel?: boolean;
}

export type ChatKitHandler = (request: Request) => Promise<Response>;

export function createChatKitHandler<TContext = undefined>(
  server: ChatKitServer<TContext>,
  options: ChatKitHandlerOptions<TContext> = {},
): ChatKitHandler {
  const runManager =
    options.runManager ?? new ResponseRunManager<TContext, ThreadStreamEvent>();
  const disconnectBehavior = options.disconnectBehavior ?? "continue";
  const supportsExplicitCancel = options.supportsExplicitCancel ?? false;

  return async (request) => {
    const context = options.getContext
      ? await options.getContext(request)
      : (undefined as TContext);
    const result = await server.processRequest(await request.arrayBuffer(), context);

    if (result instanceof NonStreamingResult) {
      return new Response(new Uint8Array(result.json), {
        headers: {
          "content-type": "application/json",
        },
      });
    }

    if (result instanceof StreamingEventResult) {
      const { run, subscription } = await runManager.startRunAndSubscribe({
        context,
        supportsExplicitCancel,
        source: (runtime) => result.stream(runtime),
      });

      return new Response(
        toReadableStream(server.serializeStreamingEventsForHandler(subscription.events), {
          completed: run.completed,
          onCancel: async () => {
            if (disconnectBehavior === "cancel") {
              await runManager.cancelRun({ runId: run.runId, context });
            }
          },
        }),
        {
          headers: {
            "content-type": "text/event-stream",
            "cache-control": "no-cache",
            "x-chatkit-run-id": run.runId,
          },
        },
      );
    }

    const _exhaustive: never = result;
    return _exhaustive;
  };
}

interface ReadableStreamOptions {
  completed?: Promise<void>;
  onCancel?: () => Promise<void>;
}

function toReadableStream(
  iterable: AsyncIterable<Uint8Array>,
  options: ReadableStreamOptions = {},
): ReadableStream<Uint8Array> {
  const iterator = iterable[Symbol.asyncIterator]();

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const next = await iterator.next();

      if (next.done) {
        await options.completed;
        controller.close();
        return;
      }

      controller.enqueue(next.value);
    },
    async cancel() {
      try {
        await iterator.return?.();
      } finally {
        await options.onCancel?.();
      }
    },
  });
}
