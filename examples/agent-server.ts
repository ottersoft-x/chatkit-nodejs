import { Agent, run } from "@openai/agents";
import {
  AgentContext,
  ChatKitServer,
  SQLiteStore,
  createChatKitHandler,
  simpleToAgentInput,
  streamAgentResponse,
  type ThreadItem,
  type ThreadMetadata,
  type ThreadStreamEvent,
} from "chatkit-bun";

export interface RequestContext {
  userId: string;
}

type UserMessageItem = Extract<ThreadItem, { type: "user_message" }>;

export const supportAgent = new Agent({
  name: "Support Agent",
  instructions: "Help the user with concise, accurate answers.",
});

function requestContext(request: Request): RequestContext {
  return {
    userId: request.headers.get("x-user-id") ?? "anonymous",
  };
}

function threadPreviousResponseId(thread: ThreadMetadata): string | null {
  const value = thread.metadata.previous_response_id;
  return typeof value === "string" ? value : null;
}

export class AppChatKitServer extends ChatKitServer<RequestContext> {
  constructor(
    readonly sqlitePath = Bun.env.CHATKIT_SQLITE_PATH ?? "chatkit.sqlite",
  ) {
    super(
      new SQLiteStore<RequestContext>({
        path: sqlitePath,
        getUserId: (context) => context.userId,
      }),
    );
  }

  override async *respond(
    thread: ThreadMetadata,
    _inputUserMessage: UserMessageItem | null,
    context: RequestContext,
  ): AsyncIterable<ThreadStreamEvent> {
    const page = await this.store.loadThreadItems(thread.id, null, 50, "asc", context);
    const input = await simpleToAgentInput(page.data);
    const agentContext = new AgentContext({
      thread,
      store: this.store,
      context,
      previousResponseId: threadPreviousResponseId(thread),
    });
    const result = await run(supportAgent, input, {
      stream: true,
      previousResponseId: agentContext.previousResponseId ?? undefined,
    });

    yield* streamAgentResponse(agentContext, result);
  }
}

export function createAppHandler(server = new AppChatKitServer()) {
  return createChatKitHandler(server, {
    getContext: requestContext,
  });
}

export interface AppBunServerOptions {
  port?: number;
  server?: AppChatKitServer;
}

export function createAppBunServer(options: AppBunServerOptions = {}) {
  const handler = createAppHandler(options.server);

  return Bun.serve({
    port: options.port ?? Number(Bun.env.PORT ?? 3000),
    routes: {
      "/health": new Response("ok"),
      "/chatkit": {
        POST: handler,
      },
    },
  });
}
