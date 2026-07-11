import { sanitizeThreadStreamEvent } from "./response-sanitizer.js";
import type { ThreadStreamEvent } from "./types/server.js";

const sseEncoder = new TextEncoder();

export function serializeThreadStreamEventToSse(event: ThreadStreamEvent): Uint8Array {
  return sseEncoder.encode(`data: ${JSON.stringify(sanitizeThreadStreamEvent(event))}\n\n`);
}
