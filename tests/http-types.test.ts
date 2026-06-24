import { createChatKitHandler } from "../src/index.js";
import type { ChatKitServer } from "../src/server.js";
import type {
  AttachRunOptions as RootAttachRunOptions,
  AttachRunResult as RootAttachRunResult,
  CancelRunOptions as RootCancelRunOptions,
  CancelRunResult as RootCancelRunResult,
  RunCoordinator as RootRunCoordinator,
  RunCoordinatorAttachRunOptions as RootRunCoordinatorAttachRunOptions,
  RunCoordinatorAttachRunResult as RootRunCoordinatorAttachRunResult,
  RunCoordinatorCancelRunOptions as RootRunCoordinatorCancelRunOptions,
  RunCoordinatorCancelRunResult as RootRunCoordinatorCancelRunResult,
  RunCoordinatorStartRunOptions as RootRunCoordinatorStartRunOptions,
  RunCoordinatorStartRunResult as RootRunCoordinatorStartRunResult,
  RunDetachReason as RootRunDetachReason,
  RunStartDescriptor as RootRunStartDescriptor,
  RunStatus as RootRunStatus,
  RunSubscription as RootRunSubscription,
  StartRunOptions as RootStartRunOptions,
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
    // @ts-expect-error Legacy manager option is not part of public HTTP handler options.
    run\u004danager: {},
  });

  createChatKitHandler(server, {
    getContext: () => ({ userId: "user_1" }),
    runCoordinator,
    // @ts-expect-error Legacy stream close policy is not part of public HTTP handler options.
    disconnect\u0042ehavior: "cancel",
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
  | RootCancelRunOptions<RequestContext>
  | RootCancelRunResult
  | RootRunCoordinator<RequestContext, ThreadStreamEvent>
  | RootRunCoordinatorAttachRunOptions<RequestContext>
  | RootRunCoordinatorAttachRunResult<ThreadStreamEvent>
  | RootRunCoordinatorCancelRunOptions<RequestContext>
  | RootRunCoordinatorCancelRunResult
  | RootRunCoordinatorStartRunOptions<RequestContext, ThreadStreamEvent>
  | RootRunCoordinatorStartRunResult<ThreadStreamEvent>
  | RootRunDetachReason
  | RootRunStartDescriptor
  | RootRunStatus
  | RootRunSubscription<ThreadStreamEvent>
  | RootStartRunOptions<RequestContext, ThreadStreamEvent>
  | RootStartRunResult<ThreadStreamEvent>;

type RootRunCoordinatorStartDescriptor =
  RootRunCoordinatorStartRunOptions<RequestContext, ThreadStreamEvent>["descriptor"];

export type {
  PublicRunCoordinatorTypes,
  RootRunCoordinatorStartDescriptor,
  RootRunCoordinatorTypes,
};
