# ChatKit Agents Generated Images Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add generated image stream conversion to the Agents bridge, including default data URLs, custom base64-to-URL conversion, partial image progress updates, and server persistence coverage.

**Architecture:** Extend the existing `ResponseStreamConverter` rather than adding a second extension point. Keep raw SDK event unwrapping and event validation in `src/agents/stream.ts`, track one active generated image item there, and add the narrow server update support needed for `generated_image.updated` persistence.

**Tech Stack:** Bun, TypeScript, Zod-validated ChatKit schemas, `bun:test`, existing fixture-shaped `@openai/agents` streams.

---

## Scope Check

This plan implements only the approved generated image stream slice:

- default base64 image result conversion to data URLs
- custom base64-to-URL converter hook
- `partialImages` progress normalization
- `response.output_item.added` and `response.output_item.done` image generation call conversion
- `response.image_generation_call.partial_image` progress updates
- server lifecycle persistence for generated image partial/final streams

It does not add reasoning/workflow conversion, generated image input replay, guardrail rollback, widgets, or multiple simultaneous generated image call state.

## File Structure

- Modify: `src/agents/annotations.ts`
  - Adds `ResponseStreamConverterOptions`, constructor options, `base64ImageToUrl(...)`, and `partialImageIndexToProgress(...)`.
- Modify: `src/agents/index.ts`
  - Exports the new converter options type.
- Modify: `src/agents/stream.ts`
  - Tracks an active generated image item, converts image generation added/final events, and converts partial image updates.
- Modify: `src/server.ts`
  - Applies `generated_image.updated` to pending generated image items and preserves pending image state when a done item has no final image.
- Modify: `tests/agents.test.ts`
  - Adds converter, image added/final, custom converter, and partial progress tests.
- Modify: `tests/server.test.ts`
  - Adds generated image lifecycle persistence regression.
- Modify: `tests/exports.test.ts`
  - Adds a compile-time package-root type export check for `ResponseStreamConverterOptions`.

## Task 1: Extend Response Stream Converter For Images

**Files:**
- Modify: `src/agents/annotations.ts`
- Modify: `src/agents/index.ts`
- Modify: `tests/agents.test.ts`
- Modify: `tests/exports.test.ts`

- [ ] **Step 1: Write failing converter tests**

In `tests/agents.test.ts`, add these tests inside `describe("ResponseStreamConverter", () => {` after the default converter instance test:

```ts
  test("converts base64 images to data URLs by default", async () => {
    const converter = new ResponseStreamConverter();

    await expect(converter.base64ImageToUrl("img_call_1", "dGVzdA==", null)).resolves.toBe(
      "data:image/png;base64,dGVzdA==",
    );
  });

  test("normalizes partial image progress from configured partial image count", () => {
    expect(new ResponseStreamConverter().partialImageIndexToProgress(1)).toBe(0);
    expect(new ResponseStreamConverter({ partialImages: 0 }).partialImageIndexToProgress(1)).toBe(0);
    expect(new ResponseStreamConverter({ partialImages: 3 }).partialImageIndexToProgress(1)).toBe(1 / 3);
    expect(new ResponseStreamConverter({ partialImages: 3 }).partialImageIndexToProgress(5)).toBe(1);
  });
```

In `tests/exports.test.ts`, add this type import:

```ts
import type { ResponseStreamConverterOptions } from "../src";
```

Add this helper above `describe("public exports", () => {`:

```ts
function assertResponseStreamConverterOptions(_options: ResponseStreamConverterOptions): void {}
```

Add this assertion inside the existing export test after the converter assertions:

```ts
    assertResponseStreamConverterOptions({ partialImages: 3 });
```

- [ ] **Step 2: Run focused tests to verify they fail**

Run:

```bash
bun test tests/agents.test.ts tests/exports.test.ts
```

Expected: FAIL because `ResponseStreamConverter` does not accept constructor options and does not define `base64ImageToUrl(...)` or `partialImageIndexToProgress(...)`.

- [ ] **Step 3: Run typecheck to verify the type export fails**

Run:

```bash
bun run typecheck
```

Expected: FAIL because `ResponseStreamConverterOptions` is not exported from the package root.

- [ ] **Step 4: Add converter image methods and options**

Modify the top of `src/agents/annotations.ts` after the helper functions:

```ts
export interface ResponseStreamConverterOptions {
  partialImages?: number | null;
}
```

Update the start of `ResponseStreamConverter`:

```ts
export class ResponseStreamConverter {
  private readonly partialImages: number | null;

  constructor(options: ResponseStreamConverterOptions = {}) {
    this.partialImages = options.partialImages ?? null;
  }

  base64ImageToUrl(
    _imageId: string,
    base64Image: string,
    _partialImageIndex: number | null = null,
  ): string | Promise<string> {
    return `data:image/png;base64,${base64Image}`;
  }

  partialImageIndexToProgress(partialImageIndex: number): number {
    if (this.partialImages === null || this.partialImages <= 0) {
      return 0;
    }

    return Math.min(1, partialImageIndex / this.partialImages);
  }

  convertAnnotation(annotation: unknown): Annotation | null {
```

Keep all existing annotation methods unchanged after this insertion.

- [ ] **Step 5: Export the options type**

Modify `src/agents/index.ts`:

```ts
export { ResponseStreamConverter, defaultResponseStreamConverter } from "./annotations";
export type { ResponseStreamConverterOptions } from "./annotations";
export { AgentContext, ClientToolCall } from "./context";
export { streamAgentResponse } from "./stream";
export type { AgentContextOptions, AgentStreamInput, StreamAgentResponseOptions } from "./types";
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts tests/exports.test.ts
```

Expected: PASS.

- [ ] **Step 7: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 8: Commit converter image API**

Run:

```bash
git add src/agents/annotations.ts src/agents/index.ts tests/agents.test.ts tests/exports.test.ts
git commit -m "Extend Agents converter for images"
```

Expected: Commit succeeds.

## Task 2: Convert Image Generation Added And Final Events

**Files:**
- Modify: `src/agents/stream.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write failing image generation stream tests**

Add these tests inside `describe("streamAgentResponse", () => {` before the unknown SDK event test:

```ts
  test("maps image generation added and done events", async () => {
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: { type: "image_generation_call", id: "img_call_1" },
          }),
          rawResponse({
            type: "response.output_item.done",
            item: { type: "image_generation_call", id: "img_call_1", result: "dGVzdA==" },
          }),
        ]),
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "message_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "generated_image",
          image: null,
        },
      },
      {
        type: "thread.item.done",
        item: {
          id: "message_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "generated_image",
          image: {
            id: "img_call_1",
            url: "data:image/png;base64,dGVzdA==",
          },
        },
      },
    ]);
  });

  test("uses custom converters for final image generation URLs", async () => {
    class CustomConverter extends ResponseStreamConverter {
      readonly calls: Array<[string, string, number | null]> = [];

      override async base64ImageToUrl(
        imageId: string,
        base64Image: string,
        partialImageIndex: number | null = null,
      ): Promise<string> {
        this.calls.push([imageId, base64Image, partialImageIndex]);
        return `https://example.com/${imageId}.png`;
      }
    }

    const converter = new CustomConverter();
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: { type: "image_generation_call", id: "img_call_1" },
          }),
          rawResponse({
            type: "response.output_item.done",
            item: { type: "image_generation_call", id: "img_call_1", result: "dGVzdA==" },
          }),
        ]),
        { converter },
      ),
    );

    expect(converter.calls).toEqual([["img_call_1", "dGVzdA==", null]]);
    expect(events.at(-1)).toEqual({
      type: "thread.item.done",
      item: {
        id: "message_generated",
        thread_id: "thr_1",
        created_at: now,
        type: "generated_image",
        image: { id: "img_call_1", url: "https://example.com/img_call_1.png" },
      },
    });
  });

  test("ignores final image generation events without an active image item or result", async () => {
    await expect(
      collect(
        streamAgentResponse(
          createContext(),
          streamedRun([
            rawResponse({
              type: "response.output_item.done",
              item: { type: "image_generation_call", id: "img_call_1", result: "dGVzdA==" },
            }),
            rawResponse({
              type: "response.output_item.added",
              item: { type: "image_generation_call", id: "img_call_2" },
            }),
            rawResponse({
              type: "response.output_item.done",
              item: { type: "image_generation_call", id: "img_call_2" },
            }),
          ]),
        ),
      ),
    ).resolves.toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "message_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "generated_image",
          image: null,
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

Expected: FAIL because `streamAgentResponse(...)` ignores image generation calls and `convertSdkEvent(...)` is synchronous.

- [ ] **Step 3: Add generated image stream state and helpers**

Modify imports at the top of `src/agents/stream.ts`:

```ts
import type { AssistantMessageContent, ThreadItem } from "../types/core";
```

Add this type near the other interfaces:

```ts
type GeneratedImageItem = Extract<ThreadItem, { type: "generated_image" }>;
```

Add generated image state to `AssistantTextState`:

```ts
  generatedImageItem: GeneratedImageItem | null;
```

Add this helper after `assistantItem(...)`:

```ts
function generatedImageItem<TContext>(
  context: AgentContext<TContext>,
  itemId: string,
  image: GeneratedImageItem["image"],
): GeneratedImageItem {
  return {
    id: itemId,
    thread_id: context.thread.id,
    created_at: context.createdAt(),
    type: "generated_image",
    image,
  };
}
```

- [ ] **Step 4: Make SDK event conversion async**

Change the `convertSdkEvent(...)` signature:

```ts
async function convertSdkEvent<TContext>(
  context: AgentContext<TContext>,
  state: AssistantTextState,
  event: unknown,
  converter: ResponseStreamConverter,
): Promise<ThreadStreamEvent[]> {
```

Update the loop in `streamAgentResponse(...)`:

```ts
      for (const event of await convertSdkEvent(context, state, next.result.value, converter)) {
        yield ThreadStreamEventSchema.parse(event);
      }
```

Initialize the new state field:

```ts
  const state: AssistantTextState = {
    activeItemId: null,
    textByPart: new Map(),
    annotationCountByPart: new Map(),
    generatedImageItem: null,
  };
```

- [ ] **Step 5: Convert image generation added and final events**

In the `response.output_item.added` case in `src/agents/stream.ts`, after the message branch and before returning `[]`, add:

```ts
      if (item.type === "image_generation_call") {
        const itemId = context.store.generateItemId("message", context.thread, context.context);
        const generated = generatedImageItem(context, itemId, null);
        state.generatedImageItem = generated;

        return [
          {
            type: "thread.item.added",
            item: generated,
          },
        ];
      }
```

In the `response.output_item.done` case, after the message branch and before returning `[]`, add:

```ts
      if (item.type === "image_generation_call") {
        const imageId = stringValue(item.id);
        const result = stringValue(item.result);

        if (!state.generatedImageItem || !imageId || !result) {
          return [];
        }

        const image = {
          id: imageId,
          url: await converter.base64ImageToUrl(imageId, result, null),
        };
        const doneItem = { ...state.generatedImageItem, image };
        state.generatedImageItem = null;

        return [
          {
            type: "thread.item.done",
            item: doneItem,
          },
        ];
      }
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 7: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS.

- [ ] **Step 8: Commit image added/final conversion**

Run:

```bash
git add src/agents/stream.ts tests/agents.test.ts
git commit -m "Convert Agents generated image results"
```

Expected: Commit succeeds.

## Task 3: Convert Partial Image Progress Events

**Files:**
- Modify: `src/agents/stream.ts`
- Modify: `tests/agents.test.ts`

- [ ] **Step 1: Write failing partial image tests**

Add these tests inside `describe("streamAgentResponse", () => {` before the unknown SDK event test:

```ts
  test("maps partial image progress updates", async () => {
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawResponse({
            type: "response.output_item.added",
            item: { type: "image_generation_call", id: "img_call_1" },
          }),
          rawResponse({
            type: "response.image_generation_call.partial_image",
            item_id: "img_call_1",
            partial_image_b64: "cGFydGlhbA==",
            partial_image_index: 1,
          }),
          rawResponse({
            type: "response.output_item.done",
            item: { type: "image_generation_call", id: "img_call_1", result: "ZmluYWw=" },
          }),
        ]),
        { converter: new ResponseStreamConverter({ partialImages: 3 }) },
      ),
    );

    expect(events).toEqual([
      {
        type: "thread.item.added",
        item: {
          id: "message_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "generated_image",
          image: null,
        },
      },
      {
        type: "thread.item.updated",
        item_id: "message_generated",
        update: {
          type: "generated_image.updated",
          image: {
            id: "img_call_1",
            url: "data:image/png;base64,cGFydGlhbA==",
          },
          progress: 1 / 3,
        },
      },
      {
        type: "thread.item.done",
        item: {
          id: "message_generated",
          thread_id: "thr_1",
          created_at: now,
          type: "generated_image",
          image: {
            id: "img_call_1",
            url: "data:image/png;base64,ZmluYWw=",
          },
        },
      },
    ]);
  });

  test("uses custom converters for partial image updates and nested provider events", async () => {
    class CustomConverter extends ResponseStreamConverter {
      readonly calls: Array<[string, string, number | null]> = [];

      override base64ImageToUrl(
        imageId: string,
        base64Image: string,
        partialImageIndex: number | null = null,
      ): string {
        this.calls.push([imageId, base64Image, partialImageIndex]);
        const suffix = partialImageIndex === null ? "final" : `partial-${partialImageIndex}`;
        return `https://example.com/${imageId}/${suffix}.png`;
      }
    }

    const converter = new CustomConverter({ partialImages: 4 });
    const events = await collect(
      streamAgentResponse(
        createContext(),
        streamedRun([
          rawModel({
            type: "model",
            event: {
              type: "response.output_item.added",
              item: { type: "image_generation_call", id: "img_call_1" },
            },
          }),
          rawModel({
            type: "model",
            event: {
              type: "response.image_generation_call.partial_image",
              item_id: "img_call_1",
              partial_image_b64: "cGFydGlhbA==",
              partial_image_index: 2,
            },
          }),
          rawModel({
            type: "model",
            event: {
              type: "response.output_item.done",
              item: { type: "image_generation_call", id: "img_call_1", result: "ZmluYWw=" },
            },
          }),
        ]),
        { converter },
      ),
    );

    expect(converter.calls).toEqual([
      ["img_call_1", "cGFydGlhbA==", 2],
      ["img_call_1", "ZmluYWw=", null],
    ]);
    expect(events[1]).toEqual({
      type: "thread.item.updated",
      item_id: "message_generated",
      update: {
        type: "generated_image.updated",
        image: {
          id: "img_call_1",
          url: "https://example.com/img_call_1/partial-2.png",
        },
        progress: 0.5,
      },
    });
    expect(events.at(-1)).toEqual({
      type: "thread.item.done",
      item: {
        id: "message_generated",
        thread_id: "thr_1",
        created_at: now,
        type: "generated_image",
        image: {
          id: "img_call_1",
          url: "https://example.com/img_call_1/final.png",
        },
      },
    });
  });

  test("ignores partial image updates without an active generated image item", async () => {
    await expect(
      collect(
        streamAgentResponse(
          createContext(),
          streamedRun([
            rawResponse({
              type: "response.image_generation_call.partial_image",
              item_id: "img_call_1",
              partial_image_b64: "cGFydGlhbA==",
              partial_image_index: 1,
            }),
          ]),
        ),
      ),
    ).resolves.toEqual([]);
  });
```

- [ ] **Step 2: Run agents tests to verify they fail**

Run:

```bash
bun test tests/agents.test.ts
```

Expected: FAIL because partial image events are ignored.

- [ ] **Step 3: Add partial image conversion**

In `src/agents/stream.ts`, add this switch case before `response.output_text.annotation.added`:

```ts
    case "response.image_generation_call.partial_image": {
      const imageId = stringValue(rawData.item_id);
      const base64Image = stringValue(rawData.partial_image_b64);
      const partialImageIndex = numberValue(rawData.partial_image_index);

      if (!state.generatedImageItem || !imageId || !base64Image || partialImageIndex === null) {
        return [];
      }

      const image = {
        id: imageId,
        url: await converter.base64ImageToUrl(imageId, base64Image, partialImageIndex),
      };
      state.generatedImageItem = { ...state.generatedImageItem, image };

      return [
        {
          type: "thread.item.updated",
          item_id: state.generatedImageItem.id,
          update: {
            type: "generated_image.updated",
            image,
            progress: converter.partialImageIndexToProgress(partialImageIndex),
          },
        },
      ];
    }
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

- [ ] **Step 6: Commit partial image conversion**

Run:

```bash
git add src/agents/stream.ts tests/agents.test.ts
git commit -m "Convert Agents partial image progress"
```

Expected: Commit succeeds.

## Task 4: Persist Generated Image Updates And Verify

**Files:**
- Modify: `src/server.ts`
- Modify: `tests/server.test.ts`

- [ ] **Step 1: Write failing server lifecycle test**

Add these tests inside `describe("ChatKitServer", () => {` near the existing assistant annotation lifecycle tests:

```ts
  test("persists generated image final state after partial updates", async () => {
    const server = new TestServer(async function* (thread) {
      const generatedImage = {
        id: "msg_generated_image",
        thread_id: thread.id,
        created_at: "2026-05-27T00:00:02.000Z",
        type: "generated_image" as const,
        image: null,
      };

      yield { type: "thread.item.added", item: generatedImage };
      yield {
        type: "thread.item.updated",
        item_id: generatedImage.id,
        update: {
          type: "generated_image.updated",
          image: {
            id: "img_call_1",
            url: "data:image/png;base64,cGFydGlhbA==",
          },
          progress: 0.5,
        },
      };
      yield {
        type: "thread.item.done",
        item: {
          ...generatedImage,
          image: {
            id: "img_call_1",
            url: "data:image/png;base64,ZmluYWw=",
          },
        },
      };
    });
    const thread = makeThread();
    await server.store.saveThread(thread, defaultContext);

    const result = (await server.process(
      JSON.stringify({
        type: "threads.add_user_message",
        params: {
          thread_id: thread.id,
          input: {
            content: [{ type: "input_text", text: "Generate an image" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
    )) as StreamingResult;

    await decodeStream(result);
    await expect(server.store.loadItem(thread.id, "msg_generated_image", defaultContext)).resolves.toMatchObject({
      type: "generated_image",
      image: {
        id: "img_call_1",
        url: "data:image/png;base64,ZmluYWw=",
      },
    });
  });

  test("preserves pending generated image state when done item has no image", async () => {
    const server = new TestServer(async function* (thread) {
      const generatedImage = {
        id: "msg_partial_image",
        thread_id: thread.id,
        created_at: "2026-05-27T00:00:02.000Z",
        type: "generated_image" as const,
        image: null,
      };

      yield { type: "thread.item.added", item: generatedImage };
      yield {
        type: "thread.item.updated",
        item_id: generatedImage.id,
        update: {
          type: "generated_image.updated",
          image: {
            id: "img_call_1",
            url: "data:image/png;base64,cGFydGlhbA==",
          },
          progress: 0.5,
        },
      };
      yield {
        type: "thread.item.done",
        item: generatedImage,
      };
    });
    const thread = makeThread();
    await server.store.saveThread(thread, defaultContext);

    const result = (await server.process(
      JSON.stringify({
        type: "threads.add_user_message",
        params: {
          thread_id: thread.id,
          input: {
            content: [{ type: "input_text", text: "Generate an image preview" }],
            attachments: [],
            inference_options: {},
          },
        },
        metadata: {},
      }),
      defaultContext,
    )) as StreamingResult;

    await decodeStream(result);
    await expect(server.store.loadItem(thread.id, "msg_partial_image", defaultContext)).resolves.toMatchObject({
      type: "generated_image",
      image: {
        id: "img_call_1",
        url: "data:image/png;base64,cGFydGlhbA==",
      },
    });
  });
```

- [ ] **Step 2: Run server test to verify it fails**

Run:

```bash
bun test tests/server.test.ts
```

Expected: FAIL because `ChatKitServer.updatePendingItems(...)` does not apply `generated_image.updated` updates to pending generated image items and `mergePendingUpdatesIntoDoneItem(...)` does not preserve generated image pending state.

- [ ] **Step 3: Apply generated image pending updates and merge fallback**

Modify `src/server.ts` in `updatePendingItems(...)`, after the workflow update branches and before `return false`:

```ts
    } else if (item.type === "generated_image" && update.type === "generated_image.updated") {
      pendingItems.set(itemId, { ...item, image: update.image });
      return true;
    }
```

The resulting branch order should remain:

```ts
    if (
      item.type === "assistant_message" &&
      (update.type === "assistant_message.content_part.added" ||
        update.type === "assistant_message.content_part.text_delta" ||
        update.type === "assistant_message.content_part.annotation_added" ||
        update.type === "assistant_message.content_part.done")
    ) {
      pendingItems.set(itemId, this.applyAssistantMessageUpdate(item, update));
      return true;
    } else if (item.type === "workflow" && update.type === "workflow.task.added") {
      item.workflow.tasks.splice(update.task_index, 0, update.task);
      pendingItems.set(itemId, item);
      return true;
    } else if (item.type === "workflow" && update.type === "workflow.task.updated") {
      item.workflow.tasks[update.task_index] = update.task;
      pendingItems.set(itemId, item);
      return true;
    } else if (item.type === "generated_image" && update.type === "generated_image.updated") {
      pendingItems.set(itemId, { ...item, image: update.image });
      return true;
    }
```

Modify `mergePendingUpdatesIntoDoneItem(...)`, after the workflow branch and before `return doneItem`:

```ts
    if (doneItem.type === "generated_image" && pendingItem.type === "generated_image") {
      return doneItem.image ? doneItem : { ...doneItem, image: pendingItem.image };
    }
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
bun test tests/server.test.ts tests/agents.test.ts
```

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:

```bash
bun run verify
```

Expected: PASS.

- [ ] **Step 6: Commit server lifecycle support**

Run:

```bash
git add src/server.ts tests/server.test.ts
git commit -m "Persist Agents generated image updates"
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

- Keep generated image conversion in the existing `ResponseStreamConverter` class.
- Treat raw SDK event payloads as `unknown` and extract only string/integer fields.
- Pass `null` for final image conversion's partial image index so custom converters can distinguish final images from partial images.
- Preserve existing assistant text, annotation, client tool, merge fairness, cancellation, and export behavior.
- Let custom converter exceptions propagate through the stream iterator.
- Continue validating all emitted events through `ThreadStreamEventSchema.parse(...)`.
- Do not add workflow, reasoning, guardrail rollback, widget, or input replay behavior in this milestone.
