# chatkit-bun

`chatkit-bun` is a Bun-native server bridge for ChatKit-style thread APIs. It includes:

- ChatKit request processing and SSE response helpers.
- SQLite-backed thread and item storage.
- Widget serialization and streaming helpers.
- `@openai/agents` stream conversion helpers for Bun servers.

## Development

Install dependencies:

```bash
bun install
```

Run typecheck and tests:

```bash
bun run verify
```

The package is source-distributed for Bun apps. Its package entrypoint is `src/index.ts`, so a private Git install does not need a build step or committed `dist` output.

## Bun Agent Server Example

The example at `examples/agent-server.ts` shows the intended downstream shape: a Bun server imports `chatkit-bun` and `@openai/agents`, subclasses `ChatKitServer`, and bridges an Agents SDK stream back to ChatKit.

```ts
import { Agent, run } from "@openai/agents";
import {
  AgentContext,
  ChatKitServer,
  SQLiteStore,
  createChatKitHandler,
  simpleToAgentInput,
  streamAgentResponse,
} from "chatkit-bun";
```

Install from a private GitHub URL in a Bun app:

```json
{
  "dependencies": {
    "@openai/agents": "^0.11.6",
    "chatkit-bun": "git+ssh://git@github.com/ottersoft-x/chatkit-bun.git#main"
  }
}
```

The example is reference code, not an auto-starting server entrypoint. In a consuming app, import the pattern and start your own server from your app's entrypoint:

```ts
import { createAppBunServer } from "./chatkit-server";

const server = createAppBunServer();
console.log(`ChatKit server listening on ${server.url}`);
```

The server factory listens on `PORT` or `3000` and exposes `POST /chatkit`. It uses `x-user-id` as the per-request user id, falling back to `anonymous`.
