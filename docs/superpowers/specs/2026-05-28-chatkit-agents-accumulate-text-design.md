# ChatKit Agents Accumulate Text Design

## Purpose

This milestone ports the Python ChatKit `accumulate_text` helper into the Bun
Agents API. The parity matrix currently tracks `agents-accumulate-text` as a
deferred row because Python exposes a helper that converts raw Agents text delta
events into a streaming `Text` or `Markdown` widget, while Bun has no equivalent
export.

The goal is a small parity slice: add the helper, prove it with a direct port of
the upstream behavior test, show that it composes with Bun's existing
`streamWidget(...)` implementation, and update the parity matrix from
`deferred` to `covered`.

## Scope

In scope:

- Add an exported `accumulateText(...)` helper to the Agents public API.
- Match Python's `accumulate_text` behavior for raw response output text delta
  events.
- Support `Text` and `Markdown` widget inputs, preserving all existing widget
  props while replacing `value` during accumulation.
- Always yield the initial widget before consuming stream events.
- Yield cumulative text updates for each matching text delta.
- Always yield a final widget with the accumulated `value` and
  `streaming: false`, including when the event stream has no matching deltas.
- Add one direct parity test and one composition test with the existing widget
  streaming path.
- Update `docs/parity/matrix.json` so `agents-accumulate-text` cites the new
  source and tests as covered.

Out of scope:

- Changing `streamAgentResponse(...)` text aggregation behavior.
- Changing server persistence, HTTP/SSE handling, widget serialization, or
  `diffWidget(...)`.
- Accepting normalized JavaScript SDK event shapes such as
  `output_text_delta` or nested `raw_model_stream_event` events.
- Handling refusal deltas, non-text content parts, multi-part assistant content,
  or annotation events.
- Adding live OpenAI calls or Python pytest to the default Bun verification
  path.

## Public API

Add this helper under the Agents package exports:

```ts
function accumulateText<TWidget extends TextProps | MarkdownProps>(
  events: AsyncIterable<unknown>,
  baseWidget: TWidget,
): AsyncGenerator<TWidget, void, unknown>
```

The helper name follows TypeScript camelCase while preserving the Python helper's
purpose. It should be exported from `src/agents/index.ts`, and therefore from the
root `src/index.ts` export chain.

The helper operates on widget prop objects rather than component instances. This
mirrors Python's helper, where callers pass `Text(...)` or `Markdown(...)` model
instances. In Bun, callers can pass the result of `Text({ ... })` or
`Markdown({ ... })` because those component helpers return object-shaped widget
values with `type`, `value`, and optional `streaming` props.

## Event Handling

The implementation should be strict upstream parity.

For each event in `events`, append to the accumulated string only when all of
these are true:

- The event is an object.
- `event.type === "raw_response_event"`.
- `event.data` is an object.
- `event.data.type === "response.output_text.delta"`.
- `event.data.delta` is a string.

The yielded sequence is:

1. The original `baseWidget`.
2. A shallow copy of `baseWidget` with `value` set to the cumulative text after
   each matching delta.
3. A final shallow copy of `baseWidget` with `value` set to the final cumulative
   text and `streaming: false`.

Non-matching events should be ignored. The helper should not validate or throw
for unrelated event shapes because Python simply ignores non-matching stream
events.

The helper should not mutate `baseWidget`. Each update after the initial yield
should be a new object.

## Composition With Widget Streaming

The helper should produce an async component iterable that callers can wrap in a
widget root for `streamWidget(...)` and `AgentContext.streamWidget(...)`. Bun
`Text` and `Markdown` are components, not valid roots, so composition should use
a small wrapper such as `Card({ children: [text] })` around each accumulated
component. The direct helper does not require an `id` prop, matching Python's
`key="text"` fixture. Callers who want compact
`widget.streaming_text.value_delta` updates through Bun's `diffWidget(...)`
should provide a stable `id` on the base component, as existing widget streaming
tests already require.

The composition test should use a stable `id` and prove that:

- The first helper yield becomes the initial widget item.
- The cumulative helper update becomes a widget streaming text delta.
- The final helper yield stops streaming by producing the existing root update
  behavior for `streaming: false`.

This keeps the helper small while proving it works with the existing Bun widget
streaming infrastructure.

## Parity Matrix Update

Update the `agents-accumulate-text` row in `docs/parity/matrix.json`:

- Change `status` from `deferred` to `covered`.
- Add the new test file entries that prove direct parity and composition.
- Add the new source file.
- Keep the upstream reference to `test_accumulate_text`.
- Update notes to state that Bun exports `accumulateText(...)` with strict raw
  response text delta parity and local widget streaming composition coverage.

Because the row is no longer deferred, update the parity smoke assertion that
tracks known deferred rows.

## Error Handling

Errors thrown by the input async iterable should propagate to the caller. If the
caller stops consuming the helper early, normal async generator cleanup should
return control to the upstream iterator through JavaScript's standard async
iteration behavior.

The helper should not catch stream errors, synthesize widget errors, or close any
agent context queue. It is only a transformation from an Agents event iterable to
a widget iterable.

## Testing Strategy

Use TDD for implementation.

Direct parity test:

- Import `accumulateText` and `Text`.
- Build an async iterable with two `raw_response_event` /
  `response.output_text.delta` events.
- Collect the helper output.
- Assert the sequence is initial `Text`, first cumulative update, second
  cumulative update, and final `Text` with `streaming: false`.

Composition test:

- Wrap each yielded component from
  `accumulateText(events, Text({ id: "text", value: "", streaming: true }))`
  in `Card({ children: [text] })`, then pass that root iterable to
  `streamWidget(...)`.
- Assert the output includes the initial widget item, a
  `widget.streaming_text.value_delta` update for the cumulative delta, and a
  final done item whose widget has `streaming: false`.

Matrix test:

- Update the existing parity smoke expectations so the row is covered and no
  longer listed as a known deferred gap.

Focused verification:

```bash
bun test tests/agents.test.ts tests/widgets.test.ts tests/parity-smoke.test.ts
bun run typecheck
```

Full verification:

```bash
bun run verify
bun run verify:parity
```

## Success Criteria

- `accumulateText(...)` is exported from the public Agents API.
- The helper matches Python's `accumulate_text` contract for raw response output
  text delta events.
- The helper yields an initial widget, cumulative updates, and a final
  non-streaming widget without mutating the original widget.
- The helper composes with existing Bun widget streaming when callers provide a
  stable widget component `id`.
- `agents-accumulate-text` is marked covered in the parity matrix with source
  and test references.
- `bun run verify` and `bun run verify:parity` pass.
