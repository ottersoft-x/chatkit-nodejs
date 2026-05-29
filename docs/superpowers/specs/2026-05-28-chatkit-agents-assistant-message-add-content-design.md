# ChatKit Agents Assistant Message Add Content Design

## Purpose

This milestone brings the TypeScript `streamAgentResponse(...)` bridge closer to
the Python `chatkit.agents` reference for `response.output_item.added` events.
When the OpenAI Responses stream adds an assistant message that already includes
content, ChatKit should surface that content in the `thread.item.added` event
instead of emitting an empty assistant message.

This is a narrow parity fix. It reuses the existing assistant content conversion
path and does not introduce new schemas, public APIs, or stream event shapes.

## Scope

This milestone includes:

- Converting initial assistant message content on `response.output_item.added`.
- Preserving supported `output_text` content, including annotation conversion.
- Preserving supported `refusal` content as existing ChatKit `output_text`
  content with no annotations.
- Continuing to skip unsupported assistant content parts through the existing
  converter behavior.
- Focused tests for populated `thread.item.added` content and regressions around
  normal streaming deltas.

This milestone does not include:

- New ChatKit assistant content schemas.
- Separate refusal, audio, image, or file content part schemas.
- Changes to `response.content_part.added`, `response.output_text.delta`,
  `response.output_text.done`, `response.refusal.delta`, or
  `response.refusal.done` behavior.
- Changes to final `response.output_item.done` or normalized `response_done`
  conversion.
- Changes to input replay, guardrail rollback, persistence, or server APIs.

## Current Behavior

`src/agents/stream.ts` handles `response.output_item.added` by creating an
assistant message item for message outputs. The helper that emits the add event
always calls `assistantItem(context, itemId, [])`, so the resulting
`thread.item.added` event has an empty `content` array even when the raw
Responses item contains initial content.

The Python reference converts `item.content` during the same event and includes
the converted content in the added assistant message.

The TypeScript bridge already has the content conversion pieces needed for this
behavior:

- `convertTextContentPart(...)` converts supported assistant content parts.
- `assistantContentFromItem(...)` converts final assistant message content.
- Unsupported content parts are skipped by returning `null`.

## Desired Behavior

When `convertSdkEvent(...)` receives `response.output_item.added` with
`item.type === "message"`, it should:

1. Resolve the assistant message id exactly as it does today.
2. Convert `item.content` with the existing assistant text content converter.
3. Emit `thread.item.added` with the converted content array.
4. Keep the existing workflow-ending behavior before the assistant message is
   added.

If `item.content` is absent, not an array, or contains only unsupported parts,
the emitted assistant message should keep the current `content: []` behavior.

Supported content parts follow the existing conversion rules:

- `output_text` maps to `output_text`, including converted annotations.
- `refusal` maps to `output_text` with `text` from `part.refusal` and an empty
  annotations array.
- all other content part types are ignored.

## Architecture

Update `assistantMessageAddedEvents(...)` in `src/agents/stream.ts` so it accepts
an optional assistant content array. The helper should continue to own:

- setting `state.activeItemId`,
- ending any active workflow,
- emitting the single `thread.item.added` event.

The `response.output_item.added` message branch should compute the content before
calling that helper:

- use the same converter passed to `convertSdkEvent(...)`;
- reuse `assistantContentFromItem(...)` so initial and final assistant message
  content follow one conversion path;
- keep unsupported content filtering centralized in the converter layer.

No state should be accumulated for this initial content. Later delta, done, and
final message events remain the source of incremental updates and final message
completion.

## Data Flow

1. The Responses stream emits `response.output_item.added` with a message item.
2. The stream bridge resolves the ChatKit assistant item id from `item.id` or
   falls back to `store.generateItemId(...)`.
3. The bridge converts any initial `item.content` into ChatKit assistant message
   content.
4. The bridge emits `thread.item.added` with that content.
5. Later content-part updates and final item events continue through the existing
   event paths.

## Error Handling

Malformed content should not throw unless existing annotation conversion already
throws. Missing or invalid text fields should skip only that content part, which
matches the current final-content conversion behavior.

This milestone does not add new error event handling or recovery logic.

## Testing

Add focused tests in `tests/agents.test.ts`:

- `response.output_item.added` with initial `output_text` content emits a
  populated assistant message.
- Initial output text annotations are converted the same way as final message
  annotations.
- Initial `refusal` content is represented as `output_text` with empty
  annotations.
- Unsupported initial content parts are skipped, preserving `content: []` when no
  supported parts remain.
- The existing streaming delta path still emits an empty added message when the
  added event contains no initial content.

Verification should use:

```bash
bun test tests/agents.test.ts
bun run typecheck
bun run verify
```

## Acceptance Criteria

- `response.output_item.added` assistant message events preserve supported
  initial content.
- Existing streams without initial content keep the current empty added-message
  behavior.
- Final message, refusal streaming, annotation streaming, workflow, guardrail,
  and input conversion behavior do not change.
- No public schema or API changes are introduced.
- `bun run verify` passes.
