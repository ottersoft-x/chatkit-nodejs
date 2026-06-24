import { sanitizeClientPayload } from "./response-sanitizer.js";
import { encodeJsonBytes } from "./serialization.js";
import type { ThreadStreamEvent } from "./types/server.js";

const sseEncoder = new TextEncoder();
const jsonDecoder = new TextDecoder();

export function serializeThreadStreamEventToSse(event: ThreadStreamEvent): Uint8Array {
  const json = jsonDecoder.decode(encodeJsonBytes(sanitizeClientPayload(event)));
  return sseEncoder.encode(`data: ${json}\n\n`);
}
