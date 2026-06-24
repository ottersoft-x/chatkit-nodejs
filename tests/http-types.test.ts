import { createChatKitHandler } from "../src/index.js";
import type { ChatKitServer } from "../src/server.js";
import type {
  AttachRunOptions as RootAttachRunOptions,
  AttachRunResult as RootAttachRunResult,
  RunCoordinator as RootRunCoordinator,
  RunDetachReason as RootRunDetachReason,
  RunStartDescriptor as RootRunStartDescriptor,
  RunStatus as RootRunStatus,
  RunSubscription as RootRunSubscription,
  StartRunResult as RootStartRunResult,
} from "../src/index.js";
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
  | RunCoordinator<RequestContext, ThreadStreamEvent>
  | RunDetachReason
  | RunStartDescriptor
  | RunStatus
  | RunSubscription<ThreadStreamEvent>
  | StartRunOptions<RequestContext, ThreadStreamEvent>
  | StartRunResult<ThreadStreamEvent>;

type RootRunCoordinatorTypes =
  | RootAttachRunOptions<RequestContext>
  | RootAttachRunResult<ThreadStreamEvent>
  | RootRunCoordinator<RequestContext, ThreadStreamEvent>
  | RootRunDetachReason
  | RootRunStartDescriptor
  | RootRunStatus
  | RootRunSubscription<ThreadStreamEvent>
  | RootStartRunResult<ThreadStreamEvent>;

export type { PublicRunCoordinatorTypes, RootRunCoordinatorTypes };
