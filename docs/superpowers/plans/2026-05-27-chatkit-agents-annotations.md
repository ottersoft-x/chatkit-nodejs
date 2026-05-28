# ChatKit Agents Annotations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add citation annotation conversion to the Agents stream bridge, including a converter hook for application-specific source metadata.

**Architecture:** Create a focused `src/agents/annotations.ts` module for citation conversion, then thread a converter option through `streamAgentResponse(...)`. Keep `stream.ts` responsible for ordering and event validation while delegating raw annotation payload conversion to the converter.

**Tech Stack:** Bun, TypeScript, Zod-validated ChatKit schemas, `bun:test`, existing `@openai/agents` fixture-shaped streams.

---

## Scope Check

This plan implements only the approved annotations/citations slice: default citation mapping, custom converter hooks, streaming annotation updates, final message annotations, exports, and verification. It does not add generated images, workflows, guardrails, widgets, input conversion, or broader refusal/content-part parity.

## File Structure

- Create: `src/agents/annotations.ts` for `ResponseStreamConverter`, `defaultResponseStreamConverter`, and annotation helpers.
- Modify: `src/agents/types.ts` for `StreamAgentResponseOptions`.
- Modify: `src/agents/stream.ts` to use the converter for streaming and final annotations.
- Modify: `src/agents/index.ts` to export the converter API.
- Modify: `tests/agents.test.ts` for converter, streaming annotation, final annotation, and custom converter coverage.
- Modify: `tests/exports.test.ts` for package-root export coverage.

## Task 1: Add Default Annotation Converter

**Files:**
- Create: `src/agents/annotations.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write failing converter tests**

In `tests/agents.test.ts`, add:

```ts
import { ResponseStreamConverter, defaultResponseStreamConverter } from "../src/agents/annotations";
```

Add this block before `describe("AgentContext", () => {`:

```ts
describe("ResponseStreamConverter", () => {
  test("converts default citation annotations", () => {
    const converter = new ResponseStreamConverter();

    expect(converter.convertAnnotation({
      type: "file_citation",
      file_id: "file_123",
      filename: "report.pdf",
      index: 12,
    })).toEqual({
      source: { type: "file", filename: "report.pdf", title: "report.pdf" },
      index: 12,
    });

    expect(converter.convertAnnotation({
      type: "container_file_citation",
      container_id: "container_1",
      file_id: "file_123",
      filename: "container.txt",
      start_index: 1,
      end_index: 9,
    })).toEqual({
      source: { type: "file", filename: "container.txt", title: "container.txt" },
      index: 9,
    });

    expect(converter.convertAnnotation({
      type: "url_citation",
      url: "https://example.com/report",
      title: "Example Report",
      start_index: 3,
      end_index: 15,
    })).toEqual({
      source: {
        type: "url",
        url: "https://example.com/report",
        title: "Example Report",
      },
      index: 15,
    });
  });

  test("drops invalid or unsupported citation annotations", () => {
    const converter = new ResponseStreamConverter();

    expect(converter.convertAnnotation({
      type: "file_citation",
      file_id: "file_123",
      filename: "",
      index: 0,
    })).toBeNull();
    expect(converter.convertAnnotation({
      type: "container_file_citation",
      container_id: "container_1",
      file_id: "file_123",
      filename: "",
      end_index: 4,
    })).toBeNull();
    expect(converter.convertAnnotation({
      type: "url_citation",
      url: "https://example.com",
      end_index: 4,
    })).toBeNull();
    expect(converter.convertAnnotation({ type: "unknown" })).toBeNull();
    expect(converter.convertAnnotation(null)).toBeNull();
  });

  test("exports a shared default converter instance", () => {
    expect(defaultResponseStreamConverter).toBeInstanceOf(ResponseStreamConverter);
  });
});
```

- [ ] **Step 2: Run the converter tests to verify they fail**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because `src/agents/annotations.ts` does not exist.

- [ ] **Step 3: Add the converter implementation**

Create `src/agents/annotations.ts`:

```ts
import type { Annotation } from "../types/core";

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function nonEmptyStringValue(value: unknown): string | null {
  const text = stringValue(value);
  return text && text.length > 0 ? text : null;
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) ? value : null;
}

export class ResponseStreamConverter {
  convertAnnotation(annotation: unknown): Annotation | null {
    if (!isRecord(annotation)) {
      return null;
    }

    switch (annotation.type) {
      case "file_citation":
        return this.fileCitationToAnnotation(annotation);
      case "container_file_citation":
        return this.containerFileCitationToAnnotation(annotation);
      case "url_citation":
        return this.urlCitationToAnnotation(annotation);
      default:
        return null;
    }
  }

  fileCitationToAnnotation(annotation: unknown): Annotation | null {
    if (!isRecord(annotation)) {
      return null;
    }

    const filename = nonEmptyStringValue(annotation.filename);
    if (!filename) {
      return null;
    }

    return {
      source: { type: "file", filename, title: filename },
      index: numberValue(annotation.index),
    };
  }

  containerFileCitationToAnnotation(annotation: unknown): Annotation | null {
    if (!isRecord(annotation)) {
      return null;
    }

    const filename = nonEmptyStringValue(annotation.filename);
    if (!filename) {
      return null;
    }

    return {
      source: { type: "file", filename, title: filename },
      index: numberValue(annotation.end_index),
    };
  }

  urlCitationToAnnotation(annotation: unknown): Annotation | null {
    if (!isRecord(annotation)) {
      return null;
    }

    const url = nonEmptyStringValue(annotation.url);
    const title = nonEmptyStringValue(annotation.title);
    if (!url || !title) {
      return null;
    }

    return {
      source: { type: "url", url, title },
      index: numberValue(annotation.end_index),
    };
  }
}

export const defaultResponseStreamConverter = new ResponseStreamConverter();
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 5: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 6: Commit converter API**

Run:

```bash
git add src/agents/annotations.ts tests/agents.test.ts
git commit -m "Add Agents response stream converter"
```

Expected: Commit succeeds.

## Task 2: Convert Streaming Annotation Events

**Files:**
- Modify: `src/agents/types.ts`
- Modify: `src/agents/stream.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write failing streaming annotation tests**

In `tests/agents.test.ts`, update the annotations import to include `Annotation`:

```ts
import type { Annotation, Attachment, Page, ThreadItem, ThreadMetadata } from "../src/types/core";
```

Add these tests inside `describe("streamAgentResponse", () => {` before the unknown event test:

```ts
  test("emits compacted streaming annotation added events", async () => {
    const agentContext = createContext();
    const events = await collect(
      streamAgentResponse(
        agentContext,
        streamedRun([
          rawResponse({
            type: "response.output_text.annotation.added",
            item_id: "msg_1",
            content_index: 0,
            annotation_index: 0,
            annotation: {
              type: "file_citation",
              file_id: "file_invalid",
              filename: "",
              index: 0,
            },
          }),
          rawResponse({
            type: "response.output_text.annotation.added",
            item_id: "msg_1",
            content_index: 0,
            annotation_index: 1,
            annotation: {
              type: "container_file_citation",
              container_id: "container_1",
              file_id: "file_123",
              filename: "container.txt",
              start_index: 0,
              end_index: 3,
            },
          }),
          rawResponse({
            type: "response.output_text.annotation.added",
            item_id: "msg_1",
            content_index: 0,
            annotation_index: 2,
            annotation: {
              type: "url_citation",
              url: "https://example.com",
              title: "Example",
              start_index: 1,
              end_index: 5,
            },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.updated",
        item_id: "msg_1",
        update: {
          type: "assistant_message.content_part.annotation_added",
          content_index: 0,
          annotation_index: 0,
          annotation: {
            source: { type: "file", filename: "container.txt", title: "container.txt" },
            index: 3,
          },
        },
      },
      {
        type: "thread.item.updated",
        item_id: "msg_1",
        update: {
          type: "assistant_message.content_part.annotation_added",
          content_index: 0,
          annotation_index: 1,
          annotation: {
            source: { type: "url", url: "https://example.com", title: "Example" },
            index: 5,
          },
        },
      },
    ]);
  });

  test("uses custom converters for streaming annotation events", async () => {
    class CustomConverter extends ResponseStreamConverter {
      readonly calls: string[] = [];

      override fileCitationToAnnotation(_annotation: unknown): Annotation | null {
        this.calls.push("file");
        return {
          source: {
            type: "file",
            filename: "custom.pdf",
            title: "Custom Report",
            description: "Custom citation metadata",
          },
          index: 111,
        };
      }
    }

    const converter = new CustomConverter();
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawModel({
            type: "model",
            event: {
              type: "response.output_text.annotation.added",
              item_id: "msg_1",
              content_index: 0,
              annotation_index: 0,
              annotation: {
                type: "file_citation",
                file_id: "file_123",
                filename: "report.pdf",
                index: 0,
              },
            },
          }),
        ]),
        { converter },
      ),
    );

    expect(converter.calls).toEqual(["file"]);
    expect(events).toEqual([
      {
        type: "thread.item.updated",
        item_id: "msg_1",
        update: {
          type: "assistant_message.content_part.annotation_added",
          content_index: 0,
          annotation_index: 0,
          annotation: {
            source: {
              type: "file",
              filename: "custom.pdf",
              title: "Custom Report",
              description: "Custom citation metadata",
            },
            index: 111,
          },
        },
      },
    ]);
  });
```

- [ ] **Step 2: Run agents tests to verify they fail**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because `streamAgentResponse(...)` does not accept a converter option and does not handle annotation-added events.

- [ ] **Step 3: Add the options type**

Modify `src/agents/types.ts`:

```ts
import type { ResponseStreamConverter } from "./annotations";
```

Add:

```ts
export interface StreamAgentResponseOptions {
  converter?: ResponseStreamConverter;
}
```

- [ ] **Step 4: Add annotation state and stream conversion**

Modify `src/agents/stream.ts`:

1. Add imports:

```ts
import { defaultResponseStreamConverter } from "./annotations";
import type { ResponseStreamConverter } from "./annotations";
import type { AgentStreamInput, StreamAgentResponseOptions, ToolCallMetadata } from "./types";
```

2. Add annotation counts to `AssistantTextState`:

```ts
annotationCountByPart: Map<string, number>;
```

3. Add helper:

```ts
function nextAnnotationIndex(state: AssistantTextState, itemId: string, contentIndex: number): number {
  const key = partKey(itemId, contentIndex);
  const index = state.annotationCountByPart.get(key) ?? 0;
  state.annotationCountByPart.set(key, index + 1);
  return index;
}
```

4. Update `convertSdkEvent(...)` to accept `converter: ResponseStreamConverter`.

5. Add this switch case:

```ts
    case "response.output_text.annotation.added": {
      const itemId = stringValue(rawData.item_id) ?? state.activeItemId;
      if (!itemId) {
        return [];
      }

      const annotation = converter.convertAnnotation(rawData.annotation);
      if (!annotation) {
        return [];
      }

      const contentIndex = numberValue(rawData.content_index) ?? 0;

      return [
        {
          type: "thread.item.updated",
          item_id: itemId,
          update: {
            type: "assistant_message.content_part.annotation_added",
            content_index: contentIndex,
            annotation_index: nextAnnotationIndex(state, itemId, contentIndex),
            annotation,
          },
        },
      ];
    }
```

6. Update `streamAgentResponse(...)` signature and state:

```ts
export async function* streamAgentResponse<TContext>(
  context: AgentContext<TContext>,
  streamedRun: AgentStreamInput | AsyncIterable<unknown>,
  options: StreamAgentResponseOptions = {},
): AsyncIterable<ThreadStreamEvent> {
  const converter = options.converter ?? defaultResponseStreamConverter;
  const state: AssistantTextState = {
    activeItemId: null,
    textByPart: new Map(),
    annotationCountByPart: new Map(),
  };
```

7. Pass `converter` into `convertSdkEvent(...)`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 6: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 7: Commit streaming annotations**

Run:

```bash
git add src/agents/types.ts src/agents/stream.ts tests/agents.test.ts
git commit -m "Convert Agents streaming annotations"
```

Expected: Commit succeeds.

## Task 3: Convert Final Message Content Annotations

**Files:**
- Modify: `src/agents/annotations.ts`
- Modify: `src/agents/stream.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write failing final content tests**

Add these tests inside `describe("streamAgentResponse", () => {`:

```ts
  test("includes converted annotations in final response output items", async () => {
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawResponse({
            type: "response.output_item.done",
            item: {
              type: "message",
              id: "msg_1",
              content: [
                {
                  type: "output_text",
                  text: "Hello!",
                  annotations: [
                    {
                      type: "url_citation",
                      url: "https://example.com",
                      title: "Example",
                      start_index: 0,
                      end_index: 6,
                    },
                  ],
                },
              ],
            },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.done",
        item: {
          id: "msg_1",
          thread_id: "thr_1",
          created_at: now,
          type: "assistant_message",
          content: [
            {
              type: "output_text",
              text: "Hello!",
              annotations: [
                {
                  source: { type: "url", url: "https://example.com", title: "Example" },
                  index: 6,
                },
              ],
            },
          ],
        },
      },
    ]);
  });

  test("includes converted annotations in normalized response_done outputs", async () => {
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawModel({ type: "output_text_delta", delta: "Hello!" }),
          rawModel({
            type: "response_done",
            response: {
              id: "resp_1",
              output: [
                {
                  type: "message",
                  id: "msg_real",
                  role: "assistant",
                  status: "completed",
                  content: [
                    {
                      type: "output_text",
                      text: "Hello!",
                      annotations: [
                        {
                          type: "file_citation",
                          file_id: "file_123",
                          filename: "report.pdf",
                          index: 4,
                        },
                      ],
                    },
                  ],
                },
              ],
              usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
            },
          }),
        ]),
      ),
    );

    expect(events.at(-1)).toEqual({
      type: "thread.item.done",
      item: {
        id: "message_generated",
        thread_id: "thr_1",
        created_at: now,
        type: "assistant_message",
        content: [
          {
            type: "output_text",
            text: "Hello!",
            annotations: [
              {
                source: { type: "file", filename: "report.pdf", title: "report.pdf" },
                index: 4,
              },
            ],
          },
        ],
      },
    });
  });
```

- [ ] **Step 2: Run agents tests to verify they fail**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because final content still emits empty annotation arrays.

- [ ] **Step 3: Add final content conversion helper**

In `src/agents/annotations.ts`, add:

```ts
export interface ConvertedTextContent {
  type: "output_text";
  text: string;
  annotations: Annotation[];
}

export function convertTextContentPart(
  part: unknown,
  converter: ResponseStreamConverter,
): ConvertedTextContent | null {
  if (!isRecord(part) || part.type !== "output_text") {
    return null;
  }

  const text = stringValue(part.text);
  if (text === null) {
    return null;
  }

  const annotations = Array.isArray(part.annotations)
    ? part.annotations.flatMap((annotation) => {
        const converted = converter.convertAnnotation(annotation);
        return converted ? [converted] : [];
      })
    : [];

  return { type: "output_text", text, annotations };
}
```

- [ ] **Step 4: Use the helper from stream conversion**

Modify `src/agents/stream.ts`:

1. Import `convertTextContentPart`.

2. Change `assistantContentFromItem(...)` to accept `converter: ResponseStreamConverter`.

3. Replace its raw content mapping with:

```ts
  const content = rawContent.flatMap((part) => {
    const converted = convertTextContentPart(part, converter);
    return converted ? [converted] : [];
  });
```

4. Update all `assistantContentFromItem(...)` call sites to pass `converter`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 6: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 7: Commit final annotation conversion**

Run:

```bash
git add src/agents/annotations.ts src/agents/stream.ts tests/agents.test.ts
git commit -m "Convert Agents final content annotations"
```

Expected: Commit succeeds.

## Task 4: Export Converter API And Verify

**Files:**
- Modify: `src/agents/index.ts`
- Modify: `tests/exports.test.ts`

- [ ] **Step 1: Write failing root export assertions**

Modify `tests/exports.test.ts`:

1. Add these imports to the existing import block from `../src`:

```ts
  ResponseStreamConverter,
  defaultResponseStreamConverter,
```

2. Add these assertions near the existing agents assertions:

```ts
    expect(typeof ResponseStreamConverter).toBe("function");
    expect(defaultResponseStreamConverter).toBeInstanceOf(ResponseStreamConverter);
```

- [ ] **Step 2: Run export test to verify it fails**

Run:

```bash
bun test tests/exports.test.ts
```

Expected: FAIL because `src/agents/index.ts` does not export the converter API.

- [ ] **Step 3: Export the converter API**

Modify `src/agents/index.ts`:

```ts
export { ResponseStreamConverter, defaultResponseStreamConverter } from "./annotations";
export { AgentContext, ClientToolCall } from "./context";
export { streamAgentResponse } from "./stream";
export type { AgentContextOptions, AgentStreamInput, StreamAgentResponseOptions } from "./types";
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
bun test tests/exports.test.ts tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:

```bash
bun run verify
```

Expected: PASS.

- [ ] **Step 6: Commit exports**

Run:

```bash
git add src/agents/index.ts tests/exports.test.ts
git commit -m "Export Agents annotation converter"
```

Expected: Commit succeeds.

## Final Verification

After all tasks complete, run:

```bash
bun run verify
git status --short --branch
```

Expected:

- TypeScript typecheck passes.
- All Bun tests pass.
- Git status is clean on the implementation branch.

## Implementation Notes

- Keep converter methods synchronous in this slice.
- Treat raw SDK annotation payloads as `unknown`.
- Validate outgoing stream events through the existing `ThreadStreamEventSchema.parse(...)` path.
- Let custom converter exceptions propagate through the stream iterator.
- Drop invalid annotations with `null` and compact emitted annotation indices.
- Preserve existing assistant text, client tool, merge fairness, cancellation, and normalized response tests.
