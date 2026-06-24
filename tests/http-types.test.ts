import { createChatKitHandler } from "../src/index.js";
import type { ChatKitServer } from "../src/server.js";
import type { RunCoordinator as ExportedRunCoordinator } from "../src/index.js";
import type {
  AttachRunOptions,
  AttachRunResult,
  CancelRunOptions,
  CancelRunResult,
  RunCoordinator,
  RunDetachReason,
  RunStartDescriptor,
  RunStatus,
  RunSubscription,
  StartRunOptions,
  StartRunResult,
} from "../src/run-coordinator.js";
import type { ThreadStreamEvent } from "../src/types/server.js";
import { ResponseRunManager } from "../src/run-manager.js";

interface RequestContext {
  userId: string;
}

function assertHttpHandlerOptions(
  server: ChatKitServer<RequestContext>,
  runCoordinator: RunCoordinator<RequestContext, ThreadStreamEvent>,
): void {
  createChatKitHandler(server, {
    getContext: () => ({ userId: "user_1" }),
    runCoordinator,
  });

  // @ts-expect-error createChatKitHandler requires explicit handler options.
  createChatKitHandler(server);

  // @ts-expect-error runCoordinator is required when handler options are provided.
  createChatKitHandler(server, {
    getContext: () => ({ userId: "user_1" }),
  });

  createChatKitHandler(server, {
    getContext: () => ({ userId: "user_1" }),
    runCoordinator,
    // @ts-expect-error runManager was removed from public HTTP handler options.
    runManager: new ResponseRunManager<RequestContext, ThreadStreamEvent>(),
  });

  createChatKitHandler(server, {
    getContext: () => ({ userId: "user_1" }),
    runCoordinator,
    // @ts-expect-error disconnectBehavior was removed from public HTTP handler options.
    disconnectBehavior: "cancel",
  });
}

type PublicRunCoordinatorTypes =
  | AttachRunOptions<RequestContext>
  | AttachRunResult<ThreadStreamEvent>
  | CancelRunOptions<RequestContext>
  | CancelRunResult
  | ExportedRunCoordinator<RequestContext, ThreadStreamEvent>
  | RunCoordinator<RequestContext, ThreadStreamEvent>
  | RunDetachReason
  | RunStartDescriptor
  | RunStatus
  | RunSubscription<ThreadStreamEvent>
  | StartRunOptions<RequestContext, ThreadStreamEvent>
  | StartRunResult<ThreadStreamEvent>;

export type { PublicRunCoordinatorTypes };
