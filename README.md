# chatkit-nodejs

`chatkit-nodejs` is a Node.js server bridge for ChatKit-style thread APIs. It is
a TypeScript/Node.js port derived from OpenAI's Apache-2.0 licensed
`openai-chatkit` Python package.

- ChatKit request processing and SSE response helpers.
- SQLite-backed thread and item storage using Node's built-in `node:sqlite`.
- Widget serialization and streaming helpers.
- `@openai/agents` stream conversion helpers for Node.js servers.

## Transparency

This package has been developed heavily with AI assistance using
[Superpowers](https://github.com/obra/superpowers), an agentic skills framework
and software development methodology.

## Development

Install in a Node.js app:

```bash
npm install chatkit-nodejs
```

Install dependencies:

```bash
npm install
```

Run typecheck, tests, package smoke checks, and pack inspection:

```bash
npm run verify
```

The package publishes compiled ESM JavaScript and TypeScript declarations under
`dist/`. Node.js `>=24.15.0` is required.

## Node.js Agent Server Example

Use `ChatKitServer` to bridge ChatKit requests to an `@openai/agents` workflow.
This example streams an intake agent first, passes its summary to an isolated
research agent that does not receive the prior chat history, then passes both
outputs to the final answer agent. Each stage emits workflow updates so the
frontend can show what is happening:

```ts
import { Agent, run } from "@openai/agents";
import { createServer, type ServerResponse } from "node:http";
import { Readable } from "node:stream";
import {
  AgentContext,
  ChatKitServer,
  SQLiteStore,
  createChatKitHandler,
  createChatKitRunCancelHandler,
  defaultChatKitStreamRuntime,
  simpleToAgentInput,
  streamAgentResponse,
  type RunCoordinator,
  type ThreadItem,
  type ThreadMetadata,
  type ThreadStreamEvent,
} from "chatkit-nodejs";
import { createAppRunCoordinator } from "./run-coordinator.js";

interface RequestContext {
  userId: string;
}

type UserMessageItem = Extract<ThreadItem, { type: "user_message" }>;

const intakeAgent = new Agent({
  name: "Intake Agent",
  instructions:
    "Read the conversation and summarize the user's goal, constraints, and any missing context.",
});

const answerAgent = new Agent({
  name: "Answer Agent",
  instructions:
    "Use the intake summary and research notes to produce a concise, helpful final answer for the user.",
});

const researchAgent = new Agent({
  name: "Research Agent",
  instructions:
    "You receive only a task summary, not the conversation history. Return focused research notes.",
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

class AppChatKitServer extends ChatKitServer<RequestContext> {
  constructor(readonly sqlitePath = process.env.CHATKIT_SQLITE_PATH ?? "chatkit.sqlite") {
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
    const previousResponseId = threadPreviousResponseId(thread);

    const intakeContext = new AgentContext({
      thread,
      store: this.store,
      context,
      previousResponseId,
    });
    intakeContext.addWorkflowTask({
      type: "custom",
      title: "Reviewing the request",
      content: "The intake agent is identifying the user's goal and constraints.",
      status_indicator: "loading",
    });

    const intakeRun = await run(intakeAgent, input, {
      stream: true,
      previousResponseId: previousResponseId ?? undefined,
    });

    yield* streamAgentResponse(intakeContext, intakeRun);
    await intakeRun.completed;

    const intakeSummary = String(intakeRun.finalOutput ?? "No intake summary was produced.");
    const researchContext = new AgentContext({
      thread,
      store: this.store,
      context,
    });
    researchContext.addWorkflowTask({
      type: "custom",
      title: "Checking isolated context",
      content: "The research agent is working from the intake summary only.",
      status_indicator: "loading",
    });

    const researchRun = await run(
      researchAgent,
      `Research this request using only this summary:\n\n${intakeSummary}`,
      { stream: true },
    );

    yield* streamAgentResponse(researchContext, researchRun);
    await researchRun.completed;

    const researchNotes = String(researchRun.finalOutput ?? "No research notes were produced.");
    const answerContext = new AgentContext({
      thread,
      store: this.store,
      context,
      previousResponseId: intakeRun.lastResponseId ?? previousResponseId,
    });
    answerContext.addWorkflowTask({
      type: "custom",
      title: "Drafting the answer",
      content: "The answer agent is combining the intake summary and isolated research notes.",
      status_indicator: "loading",
    });

    const answerRun = await run(
      answerAgent,
      `Use this intake summary and research notes.

Intake summary:
${intakeSummary}

Research notes:
${researchNotes}`,
      {
        stream: true,
        previousResponseId: answerContext.previousResponseId ?? undefined,
      },
    );

    yield* streamAgentResponse(answerContext, answerRun);
  }
}

const appChatKitServer = new AppChatKitServer();
const runCoordinator: RunCoordinator<RequestContext, ThreadStreamEvent> =
  createAppRunCoordinator<RequestContext>({
    createRuntime: defaultChatKitStreamRuntime,
  });

const chatkitOptions = {
  getContext: requestContext,
  runCoordinator,
};

const chatkitHandler = createChatKitHandler(appChatKitServer, chatkitOptions);
const cancelRunHandler = createChatKitRunCancelHandler(chatkitOptions);

async function sendResponse(outgoing: ServerResponse, response: Response): Promise<void> {
  outgoing.writeHead(response.status, Object.fromEntries(response.headers));

  if (!response.body) {
    outgoing.end();
    return;
  }

  const reader = response.body.getReader();
  outgoing.on("close", () => {
    void reader.cancel();
  });

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    outgoing.write(value);
  }

  outgoing.end();
}

const server = createServer(async (incoming, outgoing) => {
  const origin = `http://${incoming.headers.host ?? "localhost"}`;
  const url = new URL(incoming.url ?? "/", origin);
  const request = new Request(url, {
    method: incoming.method,
    headers: incoming.headers as HeadersInit,
    body: incoming.method === "GET" || incoming.method === "HEAD" ? undefined : Readable.toWeb(incoming),
    duplex: "half",
  } as RequestInit & { duplex: "half" });

  if (request.method === "GET" && url.pathname === "/health") {
    outgoing.writeHead(200, { "content-type": "text/plain" });
    outgoing.end("ok");
    return;
  }

  if (request.method === "POST" && url.pathname === "/chatkit") {
    await sendResponse(outgoing, await chatkitHandler(request));
    return;
  }

  if (request.method === "POST" && url.pathname === "/chatkit/runs/cancel") {
    await sendResponse(outgoing, await cancelRunHandler(request));
    return;
  }

  outgoing.writeHead(404, { "content-type": "text/plain" });
  outgoing.end("not found");
});

const port = Number(process.env.PORT ?? 3000);
server.listen(port, () => {
  console.log(`ChatKit server listening on http://localhost:${port}`);
});
```

The `createAppRunCoordinator` helper imported above is application code, not a
`chatkit-nodejs` export. A production coordinator should persist run ownership,
execute or enqueue the stream source, fan out events to subscribers, support
replay as needed, and enforce your app's authorization model. A minimal skeleton
is:

```ts
// ./run-coordinator.ts
import {
  type ChatKitStreamRuntime,
  type RunCoordinator,
  type ThreadStreamEvent,
} from "chatkit-nodejs";

interface AppRunCoordinatorOptions {
  createRuntime(): ChatKitStreamRuntime;
}

export function createAppRunCoordinator<TContext>(
  _options: AppRunCoordinatorOptions,
): RunCoordinator<TContext, ThreadStreamEvent> {
  return {
    async startRun(_run) {
      // Persist run state, execute or enqueue _run.source, and return a
      // subscription backed by your app's fanout/replay infrastructure.
      throw new Error("Implement durable application run coordination.");
    },
    async attachRun(_run) {
      return { status: "not_attachable", reason: "unavailable" };
    },
    async cancelRun(_run) {
      return { status: "not_found" };
    },
  };
}
```

The server listens on `PORT` or `3000` and exposes `POST /chatkit`, plus an
optional `POST /chatkit/runs/cancel` route when your coordinator supports
explicit cancellation. Protect the cancel route with your app auth. This demo
uses `x-user-id` as the per-request user id, falling back to `anonymous`.

### Run lifecycle, cancellation, and Vercel hosting

`createChatKitHandler(...)` requires a `runCoordinator` for streaming requests.
The coordinator owns run start, authorization, durable state, live fanout,
replay, and cancellation semantics. The handler supplies the parsed stream
source and returns the coordinator's subscription as SSE.

An HTTP/SSE close is a subscriber detach, not backend cancellation. Browser
refresh, network changes, mobile sleep, or a fetch abort cause the current
subscription to detach; they do not call `RunCoordinator.cancelRun(...)`.
Clients should recover completed state through normal thread/item fetches or
ChatKit JS `fetchUpdates()`. If your app supports live reconnects, call
`RunCoordinator.attachRun(...)` from a streaming `ChatKitServer.action(...)`
override so reconnect events flow through ChatKit's normal stream handling.

The stable response header for the active run is `x-chatkit-run-id`. For
cross-origin browser apps, expose it with
`Access-Control-Expose-Headers: x-chatkit-run-id` if client code needs to read
it. Explicit cancellation must use an app route or control that calls
`RunCoordinator.cancelRun(...)`;
`createChatKitRunCancelHandler(...)` provides the default JSON route shape for
that mapping. Use the same context derivation and authorization boundary for
the ChatKit handler, cancel route, and any custom action that attaches to a run.

On Vercel, deploy the consumer app with Node `24.x` because this package
requires Node.js `>=24.15.0`. Use the Node.js runtime for ChatKit handlers unless
the entire coordinator stack and its dependencies are proven Edge-compatible.
Streaming responses count against the function's maximum duration, and
`waitUntil()` or Next.js `after()` can continue work only within that timeout.
Durable continuation after response close, timeout, crash, deployment
replacement, or scale-to-zero requires application infrastructure such as a
database, pub/sub, queue, Vercel Workflows, or another worker system.
