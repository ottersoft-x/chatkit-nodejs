# ChatKit Widget Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add full widget runtime parity for `chatkit-bun`, including widget builders, dynamic widgets, `.widget` templates, widget diffing, and widget streaming helpers.

**Architecture:** Add a focused `src/widgets/` module exported from `src/index.ts`. Keep `WidgetItemSchema.widget` as plain JSON in `src/types/core.ts`, while `src/widgets/` owns typed construction, serialization, template rendering, diffing, and `ThreadStreamEvent` generation.

**Tech Stack:** Bun, TypeScript, Zod, `bun:test`, `nunjucks` for Jinja-like rendering with strict undefined behavior, existing `ThreadMetadata`/`ThreadItem`/`ThreadStreamEvent` schemas.

---

## Scope Check

The approved spec covers one subsystem: widget runtime parity. It touches new widget files, public exports, fixture tests, one dependency, and server event helper integration. Bun HTTP routing, Agents SDK conversion, and upstream sync automation stay outside this plan.

## File Structure

- Create: `src/widgets/types.ts`
  - Define JSON-like widget value types, dynamic widget schemas, root schemas, and public widget type aliases.
- Create: `src/widgets/serialization.ts`
  - Normalize widget builders and raw dynamic widgets into plain JSON while omitting `undefined` recursively and preserving required `type` fields.
- Create: `src/widgets/components.ts`
  - Export static widget builder functions for the Python widget catalogue.
- Create: `src/widgets/template.ts`
  - Implement `WidgetTemplate`, async `fromFile(...)`, `build(...)`, and deprecated `buildBasic(...)`.
- Create: `src/widgets/diff.ts`
  - Implement `diffWidget(...)`.
- Create: `src/widgets/stream.ts`
  - Implement `streamWidget(...)`.
- Create: `src/widgets/index.ts`
  - Re-export widget module public API.
- Modify: `src/index.ts`
  - Export `src/widgets`.
- Modify: `package.json`
  - Add `nunjucks` runtime dependency and `@types/nunjucks` dev dependency.
- Create: `tests/widgets.test.ts`
  - Translate `packages/chatkit-python/tests/test_widgets.py` plus `streamWidget(...)` coverage.
- Modify: `tests/exports.test.ts`
  - Assert widget public exports.
- Create: `tests/assets/widgets/*`
  - Copy upstream `.widget` and expected `.json` fixtures from `packages/chatkit-python/tests/assets/widgets`.

## Parity Reference

Use these upstream files while executing:

- `packages/chatkit-python/chatkit/widgets.py`
- `packages/chatkit-python/chatkit/server.py`
- `packages/chatkit-python/tests/test_widgets.py`
- `packages/chatkit-python/tests/assets/widgets/*.widget`
- `packages/chatkit-python/tests/assets/widgets/*.json`

Do not import Python code at runtime. Python files are reference material for names, fields, serialized JSON, error conditions, and observable event ordering.

## Task 1: Add Renderer Dependency And Widget Fixtures

**Files:**
- Modify: `package.json`
- Copy into: `tests/assets/widgets/card_no_data.widget`
- Copy into: `tests/assets/widgets/card_no_data.json`
- Copy into: `tests/assets/widgets/card_with_data.widget`
- Copy into: `tests/assets/widgets/card_with_data.json`
- Copy into: `tests/assets/widgets/list_view_no_data.widget`
- Copy into: `tests/assets/widgets/list_view_no_data.json`
- Copy into: `tests/assets/widgets/list_view_with_data.widget`
- Copy into: `tests/assets/widgets/list_view_with_data.json`
- Copy into: `tests/assets/widgets/basic_root.widget`
- Copy into: `tests/assets/widgets/basic_root.json`

- [ ] **Step 1: Install the renderer dependency**

Run:

```bash
bun add nunjucks && bun add -d @types/nunjucks
```

Expected: `package.json` and `bun.lock` update with `nunjucks` in `dependencies` and `@types/nunjucks` in `devDependencies`.

- [ ] **Step 2: Copy upstream widget fixtures**

Run:

```bash
mkdir -p tests/assets/widgets && cp packages/chatkit-python/tests/assets/widgets/*.widget tests/assets/widgets/ && cp packages/chatkit-python/tests/assets/widgets/*.json tests/assets/widgets/
```

Expected: `tests/assets/widgets` contains five `.widget` files and five `.json` files matching the upstream fixture names.

- [ ] **Step 3: Add a fixture presence test**

Create `tests/widgets.test.ts` with this starter content:

```ts
import { describe, expect, test } from "bun:test";

const widgetFixtures = [
  "card_no_data",
  "card_with_data",
  "list_view_no_data",
  "list_view_with_data",
  "basic_root",
] as const;

describe("widgets", () => {
  test("has copied upstream widget fixtures", async () => {
    for (const name of widgetFixtures) {
      expect(await Bun.file(`tests/assets/widgets/${name}.widget`).exists()).toBe(true);
      expect(await Bun.file(`tests/assets/widgets/${name}.json`).exists()).toBe(true);
    }
  });
});
```

- [ ] **Step 4: Run the fixture test**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: PASS with the fixture presence test.

- [ ] **Step 5: Commit fixture and dependency baseline**

Run:

```bash
git add package.json bun.lock tests/widgets.test.ts tests/assets/widgets
git commit -m "Add widget parity fixtures"
```

## Task 2: Add Widget Core Types And Serialization

**Files:**
- Create: `src/widgets/types.ts`
- Create: `src/widgets/serialization.ts`
- Create: `src/widgets/index.ts`
- Modify: `tests/widgets.test.ts`

- [ ] **Step 1: Add failing serialization and dynamic root tests**

Append these tests inside `describe("widgets", ...)` in `tests/widgets.test.ts`:

```ts
import {
  DynamicWidgetRootSchema,
  serializeWidget,
  type DynamicWidgetRoot,
} from "../src/widgets";

test("serializes dynamic widgets while omitting undefined fields", () => {
  const widget: DynamicWidgetRoot = {
    type: "Card",
    key: undefined,
    children: [
      {
        type: "Text",
        value: "Hello",
        streaming: undefined,
        color: undefined,
      },
    ],
  };

  expect(serializeWidget(widget)).toEqual({
    type: "Card",
    children: [{ type: "Text", value: "Hello" }],
  });
});

test("validates dynamic widget roots", () => {
  expect(DynamicWidgetRootSchema.parse({ type: "Basic", children: [] })).toEqual({
    type: "Basic",
    children: [],
  });

  expect(() => DynamicWidgetRootSchema.parse({ type: "Text", value: "No root" })).toThrow();
});
```

- [ ] **Step 2: Run the widget tests and verify they fail**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: FAIL because `../src/widgets` does not exist.

- [ ] **Step 3: Create widget type schemas**

Create `src/widgets/types.ts`:

```ts
import { z } from "zod";

export type WidgetPrimitive = string | number | boolean | null;
export type WidgetJson = WidgetPrimitive | WidgetJson[] | { [key: string]: WidgetJson | undefined };

export const WidgetJsonSchema: z.ZodType<WidgetJson> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(WidgetJsonSchema),
    z.record(z.string(), z.union([WidgetJsonSchema, z.undefined()])),
  ]),
);

export const DynamicWidgetComponentSchema = z
  .object({
    type: z.string(),
    key: z.string().optional(),
    id: z.string().optional(),
    children: z
      .union([
        z.lazy(() => DynamicWidgetComponentSchema),
        z.array(z.lazy(() => DynamicWidgetComponentSchema)),
      ])
      .optional(),
  })
  .catchall(WidgetJsonSchema.optional());
export type DynamicWidgetComponent = z.infer<typeof DynamicWidgetComponentSchema>;

export const BasicRootSchema = DynamicWidgetComponentSchema.extend({
  type: z.literal("Basic"),
});
export type BasicRoot = z.infer<typeof BasicRootSchema>;

export const DynamicWidgetRootSchema = DynamicWidgetComponentSchema.extend({
  type: z.enum(["Card", "ListView", "Basic"]),
});
export type DynamicWidgetRoot = z.infer<typeof DynamicWidgetRootSchema>;

export type WidgetComponent = DynamicWidgetComponent;
export type WidgetRoot = DynamicWidgetRoot;
```

- [ ] **Step 4: Create serialization helper**

Create `src/widgets/serialization.ts`:

```ts
import { DynamicWidgetRootSchema, type WidgetJson, type WidgetRoot } from "./types";

export function omitUndefinedDeep(value: unknown): WidgetJson {
  if (Array.isArray(value)) {
    return value
      .filter((item) => item !== undefined)
      .map((item) => omitUndefinedDeep(item)) as WidgetJson[];
  }

  if (value != null && typeof value === "object") {
    const source =
      typeof (value as { toJSON?: () => unknown }).toJSON === "function"
        ? (value as { toJSON: () => unknown }).toJSON()
        : value;

    if (source == null || typeof source !== "object" || Array.isArray(source)) {
      return omitUndefinedDeep(source);
    }

    const entries = Object.entries(source as Record<string, unknown>)
      .filter(([, entryValue]) => entryValue !== undefined)
      .map(([key, entryValue]) => [key, omitUndefinedDeep(entryValue)] as const);

    return Object.fromEntries(entries) as WidgetJson;
  }

  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value === null
  ) {
    return value;
  }

  throw new TypeError(`Unsupported widget value: ${String(value)}`);
}

export function serializeWidget(widget: WidgetRoot): Record<string, unknown> {
  const parsed = DynamicWidgetRootSchema.parse(widget);
  const serialized = omitUndefinedDeep(parsed);
  if (serialized == null || typeof serialized !== "object" || Array.isArray(serialized)) {
    throw new TypeError("Widget root must serialize to an object.");
  }
  return serialized as Record<string, unknown>;
}
```

- [ ] **Step 5: Create widget barrel export**

Create `src/widgets/index.ts`:

```ts
export * from "./serialization";
export * from "./types";
```

- [ ] **Step 6: Run the focused widget tests**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: PASS for fixture, serialization, and dynamic root tests.

- [ ] **Step 7: Commit widget core types**

Run:

```bash
git add src/widgets tests/widgets.test.ts
git commit -m "Add widget core serialization"
```

## Task 3: Add Static Widget Builder Catalogue

**Files:**
- Create: `src/widgets/components.ts`
- Modify: `src/widgets/index.ts`
- Modify: `tests/widgets.test.ts`

- [ ] **Step 1: Add failing static builder tests**

Update the `../src/widgets` import in `tests/widgets.test.ts` to include the static builders used below:

```ts
import {
  Badge,
  Basic,
  Box,
  Button,
  Caption,
  Card,
  Checkbox,
  Col,
  DatePicker,
  Divider,
  Form,
  Icon,
  Image,
  Input,
  Label,
  ListView,
  ListViewItem,
  Markdown,
  RadioGroup,
  Row,
  Select,
  Spacer,
  Text,
  Textarea,
  Title,
  Transition,
  Chart,
} from "../src/widgets";
```

Append these tests:

```ts
test("builds static widgets with Python component names", () => {
  const widget = Card({
    children: [
      Text({ value: "Hello", streaming: undefined }),
      Button({ label: "Open", onClickAction: { type: "open" } }),
    ],
  });

  expect(serializeWidget(widget)).toEqual({
    type: "Card",
    children: [
      { type: "Text", value: "Hello" },
      { type: "Button", label: "Open", onClickAction: { type: "open" } },
    ],
  });
});

test("exports the Python widget component catalogue", () => {
  const catalogue = [
    Basic({ children: [] }),
    ListView({ children: [ListViewItem({ children: [Text({ value: "Row" })] })] }),
    Markdown({ value: "**Hello**" }),
    Title({ value: "Title" }),
    Caption({ value: "Caption" }),
    Badge({ label: "New" }),
    Box({ children: [] }),
    Row({ children: [] }),
    Col({ children: [] }),
    Form({ children: [] }),
    Divider({}),
    Icon({ name: "check" }),
    Image({ src: "https://example.com/image.png", alt: "Image" }),
    Checkbox({ name: "ok", label: "OK" }),
    Spacer({}),
    Select({ name: "choice", options: [{ label: "A", value: "a" }] }),
    DatePicker({ name: "date" }),
    Input({ name: "input" }),
    Label({ label: "Name" }),
    RadioGroup({ name: "radio", options: [{ label: "A", value: "a" }] }),
    Textarea({ name: "body" }),
    Transition({ children: Text({ value: "Child" }) }),
    Chart({ data: [], xAxis: "x", yAxis: "y" }),
  ];

  expect(catalogue.map((component) => component.type)).toEqual([
    "Basic",
    "ListView",
    "Markdown",
    "Title",
    "Caption",
    "Badge",
    "Box",
    "Row",
    "Col",
    "Form",
    "Divider",
    "Icon",
    "Image",
    "Checkbox",
    "Spacer",
    "Select",
    "DatePicker",
    "Input",
    "Label",
    "RadioGroup",
    "Textarea",
    "Transition",
    "Chart",
  ]);
});
```

- [ ] **Step 2: Run the widget tests and verify they fail**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: FAIL because static builders are not exported.

- [ ] **Step 3: Implement static component builders**

Create `src/widgets/components.ts`:

```ts
import type { ActionConfig } from "../actions";
import type { DynamicWidgetComponent, DynamicWidgetRoot, WidgetJson } from "./types";

type WidgetProps = Record<string, WidgetJson | ActionConfig | DynamicWidgetComponent | DynamicWidgetComponent[] | undefined>;

function component<TType extends string, TProps extends WidgetProps>(
  type: TType,
  props: TProps,
): { type: TType } & TProps {
  return { type, ...props };
}

type Children = DynamicWidgetComponent[];
type SingleChild = DynamicWidgetComponent;

export type BasicProps = WidgetProps & { children?: Children };
export function Basic(props: BasicProps = {}): DynamicWidgetRoot {
  return component("Basic", props) as DynamicWidgetRoot;
}

export type CardProps = WidgetProps & {
  children: Children;
  asForm?: boolean;
  confirm?: WidgetJson;
  cancel?: WidgetJson;
};
export function Card(props: CardProps): DynamicWidgetRoot {
  return component("Card", props) as DynamicWidgetRoot;
}

export type ListViewProps = WidgetProps & { children: DynamicWidgetComponent[] };
export function ListView(props: ListViewProps): DynamicWidgetRoot {
  return component("ListView", props) as DynamicWidgetRoot;
}

export type ListViewItemProps = WidgetProps & { children: Children; onClickAction?: ActionConfig };
export function ListViewItem(props: ListViewItemProps): DynamicWidgetComponent {
  return component("ListViewItem", props);
}

export type TextProps = WidgetProps & { value: string; streaming?: boolean };
export function Text(props: TextProps): DynamicWidgetComponent {
  return component("Text", props);
}

export type MarkdownProps = WidgetProps & { value: string; streaming?: boolean };
export function Markdown(props: MarkdownProps): DynamicWidgetComponent {
  return component("Markdown", props);
}

export type TitleProps = WidgetProps & { value: string };
export function Title(props: TitleProps): DynamicWidgetComponent {
  return component("Title", props);
}

export type CaptionProps = WidgetProps & { value: string };
export function Caption(props: CaptionProps): DynamicWidgetComponent {
  return component("Caption", props);
}

export type BadgeProps = WidgetProps & { label: string };
export function Badge(props: BadgeProps): DynamicWidgetComponent {
  return component("Badge", props);
}

export type BoxProps = WidgetProps & { children?: Children };
export function Box(props: BoxProps = {}): DynamicWidgetComponent {
  return component("Box", props);
}

export type RowProps = WidgetProps & { children?: Children };
export function Row(props: RowProps = {}): DynamicWidgetComponent {
  return component("Row", props);
}

export type ColProps = WidgetProps & { children?: Children };
export function Col(props: ColProps = {}): DynamicWidgetComponent {
  return component("Col", props);
}

export type FormProps = WidgetProps & { children?: Children; onSubmitAction?: ActionConfig };
export function Form(props: FormProps = {}): DynamicWidgetComponent {
  return component("Form", props);
}

export function Divider(props: WidgetProps = {}): DynamicWidgetComponent {
  return component("Divider", props);
}

export type IconProps = WidgetProps & { name: string };
export function Icon(props: IconProps): DynamicWidgetComponent {
  return component("Icon", props);
}

export type ImageProps = WidgetProps & { src: string; alt?: string };
export function Image(props: ImageProps): DynamicWidgetComponent {
  return component("Image", props);
}

export type ButtonProps = WidgetProps & { label: string; onClickAction?: ActionConfig };
export function Button(props: ButtonProps): DynamicWidgetComponent {
  return component("Button", props);
}

export function Spacer(props: WidgetProps = {}): DynamicWidgetComponent {
  return component("Spacer", props);
}

export type SelectProps = WidgetProps & { name: string; options: WidgetJson[] };
export function Select(props: SelectProps): DynamicWidgetComponent {
  return component("Select", props);
}

export type DatePickerProps = WidgetProps & { name: string };
export function DatePicker(props: DatePickerProps): DynamicWidgetComponent {
  return component("DatePicker", props);
}

export type CheckboxProps = WidgetProps & { name: string; label?: string };
export function Checkbox(props: CheckboxProps): DynamicWidgetComponent {
  return component("Checkbox", props);
}

export type InputProps = WidgetProps & { name: string };
export function Input(props: InputProps): DynamicWidgetComponent {
  return component("Input", props);
}

export type LabelProps = WidgetProps & { label: string };
export function Label(props: LabelProps): DynamicWidgetComponent {
  return component("Label", props);
}

export type RadioGroupProps = WidgetProps & { name: string; options: WidgetJson[] };
export function RadioGroup(props: RadioGroupProps): DynamicWidgetComponent {
  return component("RadioGroup", props);
}

export type TextareaProps = WidgetProps & { name: string };
export function Textarea(props: TextareaProps): DynamicWidgetComponent {
  return component("Textarea", props);
}

export type TransitionProps = WidgetProps & { children: SingleChild };
export function Transition(props: TransitionProps): DynamicWidgetComponent {
  return component("Transition", props);
}

export type ChartProps = WidgetProps & { data: WidgetJson[]; xAxis?: string; yAxis?: string };
export function Chart(props: ChartProps): DynamicWidgetComponent {
  return component("Chart", props);
}
```

- [ ] **Step 4: Export static builders**

Update `src/widgets/index.ts`:

```ts
export * from "./components";
export * from "./serialization";
export * from "./types";
```

- [ ] **Step 5: Run widget tests**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: PASS for static builder tests.

- [ ] **Step 6: Run typecheck**

Run:

```bash
bun run typecheck
```

Expected: PASS. If the broad `WidgetProps` union rejects `ActionConfig` or component arrays, adjust `WidgetJson`/`WidgetProps` in this task only so the public builder tests compile and serialized output stays unchanged.

- [ ] **Step 7: Commit static widget builders**

Run:

```bash
git add src/widgets tests/widgets.test.ts
git commit -m "Add static widget builders"
```

## Task 4: Add WidgetTemplate Rendering

**Files:**
- Create: `src/widgets/template.ts`
- Modify: `src/widgets/index.ts`
- Modify: `tests/widgets.test.ts`

- [ ] **Step 1: Add failing template fixture tests**

Update the widget import in `tests/widgets.test.ts` to include `WidgetTemplate`.

Append these tests:

```ts
const fixtureData: Record<string, Record<string, unknown> | undefined> = {
  list_view_no_data: undefined,
  card_no_data: undefined,
  list_view_with_data: {
    items: [
      { id: "blue", label: "Blue line", color: "blue-500" },
      { id: "orange", label: "Orange line", color: "orange-500" },
      { id: "purple", label: "Purple line", color: "purple-500" },
    ],
  },
  card_with_data: {
    channel: "#proj-chatkit",
    time: "4:48 PM",
    user: { image: "/pam.png", name: "Pam Beesly" },
  },
};

for (const [name, data] of Object.entries(fixtureData)) {
  test(`renders ${name}.widget`, async () => {
    const template = await WidgetTemplate.fromFile(`tests/assets/widgets/${name}.widget`);
    const expected = await Bun.file(`tests/assets/widgets/${name}.json`).json();

    expect(template.build(data)).toEqual(expected);
  });
}

test("builds Basic roots through buildBasic", async () => {
  const template = await WidgetTemplate.fromFile("tests/assets/widgets/basic_root.widget");
  const expected = await Bun.file("tests/assets/widgets/basic_root.json").json();

  const widget = template.buildBasic({
    name: "Harry Potter",
    bio: "The boy who lived",
  });

  expect(widget).toEqual(expected);
  expect(widget.type).toBe("Basic");
});

test("throws on missing template variables", async () => {
  const template = await WidgetTemplate.fromFile("tests/assets/widgets/basic_root.widget");

  expect(() => template.build({ name: "Hermione Granger" })).toThrow();
});
```

- [ ] **Step 2: Run widget tests and verify they fail**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: FAIL because `WidgetTemplate` is not exported.

- [ ] **Step 3: Implement WidgetTemplate**

Create `src/widgets/template.ts`:

```ts
import nunjucks from "nunjucks";

import { BasicRootSchema, DynamicWidgetRootSchema, type BasicRoot, type DynamicWidgetRoot } from "./types";
import { serializeWidget } from "./serialization";

const env = new nunjucks.Environment(undefined, {
  autoescape: false,
  throwOnUndefined: true,
});

export interface WidgetTemplateDefinition {
  version: string;
  name: string;
  template: string;
  jsonSchema?: Record<string, unknown>;
}

function callerDirectory(): string | null {
  const stack = new Error().stack ?? "";
  const lines = stack.split("\n").slice(2);
  for (const line of lines) {
    const match = line.match(/\(?((?:file:\/\/)?\/[^:)]+):\d+:\d+\)?$/);
    if (!match) continue;
    const path = match[1].replace(/^file:\/\//, "");
    if (!path.includes("/src/widgets/template.")) {
      return path.slice(0, path.lastIndexOf("/"));
    }
  }
  return null;
}

function resolveWidgetPath(path: string): string {
  if (path.startsWith("/")) return path;
  return `${callerDirectory() ?? process.cwd()}/${path}`;
}

function normalizeData(data: unknown): Record<string, unknown> {
  if (data == null) return {};
  if (typeof data === "object" && typeof (data as { toJSON?: () => unknown }).toJSON === "function") {
    const json = (data as { toJSON: () => unknown }).toJSON();
    if (json != null && typeof json === "object" && !Array.isArray(json)) {
      return json as Record<string, unknown>;
    }
  }
  if (typeof data === "object" && !Array.isArray(data)) {
    return data as Record<string, unknown>;
  }
  throw new TypeError("Widget template data must be an object.");
}

export class WidgetTemplate {
  readonly version: string;
  readonly name: string;
  readonly template: string;
  readonly dataSchema: Record<string, unknown>;

  constructor(definition: WidgetTemplateDefinition) {
    if (definition.version !== "1.0") {
      throw new Error(`Unsupported widget spec version: ${definition.version}`);
    }
    this.version = definition.version;
    this.name = definition.name;
    this.template = definition.template;
    this.dataSchema = definition.jsonSchema ?? {};
  }

  static async fromFile(path: string): Promise<WidgetTemplate> {
    const definition = await Bun.file(resolveWidgetPath(path)).json();
    return new WidgetTemplate(definition as WidgetTemplateDefinition);
  }

  build(data?: unknown): DynamicWidgetRoot {
    const rendered = env.renderString(this.template, normalizeData(data));
    const parsed = JSON.parse(rendered);
    const widget = DynamicWidgetRootSchema.parse(parsed);
    return serializeWidget(widget) as DynamicWidgetRoot;
  }

  buildBasic(data?: unknown): BasicRoot {
    const rendered = env.renderString(this.template, normalizeData(data));
    const parsed = JSON.parse(rendered);
    const widget = BasicRootSchema.parse(parsed);
    return serializeWidget(widget) as BasicRoot;
  }
}
```

- [ ] **Step 4: Export WidgetTemplate**

Update `src/widgets/index.ts`:

```ts
export * from "./components";
export * from "./serialization";
export * from "./template";
export * from "./types";
```

- [ ] **Step 5: Run template tests**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: PASS for fixture rendering and strict missing-variable tests.

- [ ] **Step 6: Commit widget template rendering**

Run:

```bash
git add src/widgets tests/widgets.test.ts
git commit -m "Add widget template rendering"
```

## Task 5: Add Widget Diffing

**Files:**
- Create: `src/widgets/diff.ts`
- Modify: `src/widgets/index.ts`
- Modify: `tests/widgets.test.ts`

- [ ] **Step 1: Add failing diff tests translated from Python**

Update the widget import in `tests/widgets.test.ts` to include `diffWidget`.

Append these tests:

```ts
test("diffWidget returns no updates for equal widgets", () => {
  expect(diffWidget(Card({ children: [] }), Card({ children: [] }))).toEqual([]);
});

test("diffWidget streams cumulative Text value deltas", () => {
  expect(
    diffWidget(
      Card({ children: [Text({ id: "text", value: "Hello", streaming: true })] }),
      Card({ children: [Text({ id: "text", value: "Hello, world!", streaming: true })] }),
    ),
  ).toEqual([
    {
      type: "widget.streaming_text.value_delta",
      component_id: "text",
      delta: ", world!",
      done: false,
    },
  ]);
});

test("diffWidget marks streaming Text deltas done when streaming stops", () => {
  expect(
    diffWidget(
      Card({ children: [Text({ id: "text", value: "Hello", streaming: true })] }),
      Card({ children: [Text({ id: "text", value: "Hello, world!", streaming: false })] }),
    ),
  ).toEqual([
    {
      type: "widget.streaming_text.value_delta",
      component_id: "text",
      delta: ", world!",
      done: true,
    },
  ]);
});

test("diffWidget returns a root replacement for non-streaming text changes", () => {
  expect(
    diffWidget(
      Card({ children: [Text({ value: "Hello" })] }),
      Card({ children: [Text({ value: "world!" })] }),
    ),
  ).toEqual([
    {
      type: "widget.root.updated",
      widget: { type: "Card", children: [{ type: "Text", value: "world!" }] },
    },
  ]);
});

test("diffWidget rejects late streaming node ids", () => {
  expect(() =>
    diffWidget(
      Card({ children: [Text({ value: "Hello", streaming: true })] }),
      Card({ children: [Text({ id: "text", value: "Hello, world!", streaming: true })] }),
    ),
  ).toThrow("was not present when the widget was initially rendered");
});

test("diffWidget rejects non-cumulative streaming text updates", () => {
  expect(() =>
    diffWidget(
      Card({ children: [Text({ id: "text", value: "Hello", streaming: true })] }),
      Card({ children: [Text({ id: "text", value: "world!", streaming: true })] }),
    ),
  ).toThrow("not a prefix");
});
```

- [ ] **Step 2: Run widget tests and verify they fail**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: FAIL because `diffWidget` is not exported.

- [ ] **Step 3: Implement diffWidget**

Create `src/widgets/diff.ts`:

```ts
import type { ThreadItemUpdate } from "../types/server";
import { serializeWidget } from "./serialization";
import type { DynamicWidgetComponent, WidgetRoot } from "./types";

type WidgetNode = Record<string, unknown>;
type WidgetUpdate = Extract<
  ThreadItemUpdate,
  { type: "widget.root.updated" | "widget.streaming_text.value_delta" | "widget.component.updated" }
>;

function asNode(value: unknown): WidgetNode {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? (value as WidgetNode)
    : {};
}

function isStreamingText(node: WidgetNode): boolean {
  return (node.type === "Text" || node.type === "Markdown") && typeof node.value === "string";
}

function childrenOf(node: WidgetNode): WidgetNode[] {
  const children = node.children;
  if (Array.isArray(children)) return children.map(asNode);
  if (children != null && typeof children === "object") return [asNode(children)];
  return [];
}

function valuesEqual(before: unknown, after: unknown): boolean {
  return JSON.stringify(before) === JSON.stringify(after);
}

function requiresFullReplace(before: WidgetNode, after: WidgetNode): boolean {
  if (before.type !== after.type || before.id !== after.id || before.key !== after.key) {
    return true;
  }

  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of keys) {
    if (key === "value" && isStreamingText(before) && isStreamingText(after)) {
      const beforeValue = String(before.value ?? "");
      const afterValue = String(after.value ?? "");
      if (afterValue.startsWith(beforeValue)) continue;
    }

    const beforeValue = before[key];
    const afterValue = after[key];
    if (Array.isArray(beforeValue) && Array.isArray(afterValue)) {
      if (beforeValue.length !== afterValue.length) return true;
      for (let index = 0; index < beforeValue.length; index += 1) {
        if (requiresFullReplace(asNode(beforeValue[index]), asNode(afterValue[index]))) {
          return true;
        }
      }
      continue;
    }

    if (
      beforeValue != null &&
      afterValue != null &&
      typeof beforeValue === "object" &&
      typeof afterValue === "object" &&
      !Array.isArray(beforeValue) &&
      !Array.isArray(afterValue)
    ) {
      if (requiresFullReplace(asNode(beforeValue), asNode(afterValue))) return true;
      continue;
    }

    if (!valuesEqual(beforeValue, afterValue)) return true;
  }

  return false;
}

function streamingTextById(root: WidgetNode): Map<string, WidgetNode> {
  const nodes = new Map<string, WidgetNode>();

  function visit(node: WidgetNode): void {
    if (isStreamingText(node) && typeof node.id === "string") {
      nodes.set(node.id, node);
    }
    for (const child of childrenOf(node)) visit(child);
  }

  visit(root);
  return nodes;
}

export function diffWidget(before: WidgetRoot, after: WidgetRoot): WidgetUpdate[] {
  const beforeRoot = serializeWidget(before);
  const afterRoot = serializeWidget(after);

  if (requiresFullReplace(beforeRoot, afterRoot)) {
    return [{ type: "widget.root.updated", widget: afterRoot }];
  }

  const beforeNodes = streamingTextById(beforeRoot);
  const afterNodes = streamingTextById(afterRoot);
  const updates: WidgetUpdate[] = [];

  for (const [id, afterNode] of afterNodes) {
    const beforeNode = beforeNodes.get(id);
    if (!beforeNode) {
      throw new Error(
        `Node ${id} was not present when the widget was initially rendered. All nodes with ID must persist across all widget updates.`,
      );
    }

    const beforeValue = String(beforeNode.value ?? "");
    const afterValue = String(afterNode.value ?? "");
    if (beforeValue === afterValue) continue;
    if (!afterValue.startsWith(beforeValue)) {
      throw new Error(
        `Node ${id} was updated with a new value that is not a prefix of the initial value. All widget updates must be cumulative.`,
      );
    }

    updates.push({
      type: "widget.streaming_text.value_delta",
      component_id: id,
      delta: afterValue.slice(beforeValue.length),
      done: afterNode.streaming !== true,
    });
  }

  return updates;
}
```

- [ ] **Step 4: Export diffWidget**

Update `src/widgets/index.ts`:

```ts
export * from "./components";
export * from "./diff";
export * from "./serialization";
export * from "./template";
export * from "./types";
```

- [ ] **Step 5: Run focused widget tests**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: PASS for diff tests.

- [ ] **Step 6: Commit widget diffing**

Run:

```bash
git add src/widgets tests/widgets.test.ts
git commit -m "Add widget diffing"
```

## Task 6: Add streamWidget Helper

**Files:**
- Create: `src/widgets/stream.ts`
- Modify: `src/widgets/index.ts`
- Modify: `tests/widgets.test.ts`

- [ ] **Step 1: Add failing streamWidget tests**

Update the widget import in `tests/widgets.test.ts` to include `streamWidget`.

Append these helpers and tests:

```ts
const testThread = {
  id: "thr_widget",
  created_at: "2026-05-27T00:00:00.000Z",
  status: { type: "active" as const },
  metadata: {},
};

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) values.push(value);
  return values;
}

test("streamWidget emits a done event for one-off widgets", async () => {
  const events = await collect(
    streamWidget(testThread, Card({ children: [Text({ value: "Done" })] }), {
      copyText: "Done",
      generateId: () => "msg_widget",
      now: () => "2026-05-27T00:00:01.000Z",
    }),
  );

  expect(events).toEqual([
    {
      type: "thread.item.done",
      item: {
        id: "msg_widget",
        type: "widget",
        thread_id: "thr_widget",
        created_at: "2026-05-27T00:00:01.000Z",
        widget: { type: "Card", children: [{ type: "Text", value: "Done" }] },
        copy_text: "Done",
      },
    },
  ]);
});

test("streamWidget diffs async widget generators", async () => {
  async function* widgets() {
    yield Card({ children: [Text({ id: "text", value: "Hello", streaming: true })] });
    yield Card({ children: [Text({ id: "text", value: "Hello, world!", streaming: true })] });
    yield Card({ children: [Text({ id: "text", value: "Hello, world!", streaming: false })] });
  }

  const events = await collect(
    streamWidget(testThread, widgets(), {
      generateId: () => "msg_stream",
      now: () => "2026-05-27T00:00:02.000Z",
    }),
  );

  expect(events.map((event) => event.type)).toEqual([
    "thread.item.added",
    "thread.item.updated",
    "thread.item.done",
  ]);
  expect(events[1]).toEqual({
    type: "thread.item.updated",
    item_id: "msg_stream",
    update: {
      type: "widget.streaming_text.value_delta",
      component_id: "text",
      delta: ", world!",
      done: false,
    },
  });
});

test("streamWidget rejects empty async widget generators", async () => {
  async function* widgets() {}

  await expect(
    collect(
      streamWidget(testThread, widgets(), {
        generateId: () => "msg_empty",
        now: () => "2026-05-27T00:00:03.000Z",
      }),
    ),
  ).rejects.toThrow("yield an initial widget");
});
```

- [ ] **Step 2: Run widget tests and verify they fail**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: FAIL because `streamWidget` is not exported.

- [ ] **Step 3: Implement streamWidget**

Create `src/widgets/stream.ts`:

```ts
import type { StoreItemType } from "../store";
import { defaultGenerateId } from "../store";
import type { ThreadMetadata, ThreadItem } from "../types/core";
import type { ThreadStreamEvent } from "../types/server";
import { diffWidget } from "./diff";
import { serializeWidget } from "./serialization";
import type { WidgetRoot } from "./types";

export interface StreamWidgetOptions {
  copyText?: string | null;
  generateId?: (itemType: StoreItemType) => string;
  now?: () => string;
}

function isAsyncIterable(value: unknown): value is AsyncIterable<WidgetRoot> {
  return value != null && typeof (value as AsyncIterable<WidgetRoot>)[Symbol.asyncIterator] === "function";
}

function makeWidgetItem(
  thread: ThreadMetadata,
  itemId: string,
  widget: WidgetRoot,
  options: Required<Pick<StreamWidgetOptions, "generateId" | "now">> & StreamWidgetOptions,
): ThreadItem {
  const item: ThreadItem = {
    id: itemId,
    type: "widget",
    thread_id: thread.id,
    created_at: options.now(),
    widget: serializeWidget(widget),
    copy_text: options.copyText ?? undefined,
  };
  return item;
}

export async function* streamWidget(
  thread: ThreadMetadata,
  widget: WidgetRoot | AsyncIterable<WidgetRoot>,
  options: StreamWidgetOptions = {},
): AsyncIterable<ThreadStreamEvent> {
  const resolvedOptions = {
    generateId: options.generateId ?? defaultGenerateId,
    now: options.now ?? (() => new Date().toISOString()),
    copyText: options.copyText,
  };
  const itemId = resolvedOptions.generateId("message");

  if (!isAsyncIterable(widget)) {
    yield {
      type: "thread.item.done",
      item: makeWidgetItem(thread, itemId, widget, resolvedOptions),
    };
    return;
  }

  const iterator = widget[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) {
    throw new Error("streamWidget async iterable must yield an initial widget.");
  }

  let lastState = first.value;
  yield {
    type: "thread.item.added",
    item: makeWidgetItem(thread, itemId, lastState, resolvedOptions),
  };

  for (;;) {
    const next = await iterator.next();
    if (next.done) break;

    const updates = diffWidget(lastState, next.value);
    for (const update of updates) {
      yield { type: "thread.item.updated", item_id: itemId, update };
    }
    lastState = next.value;
  }

  yield {
    type: "thread.item.done",
    item: makeWidgetItem(thread, itemId, lastState, resolvedOptions),
  };
}
```

- [ ] **Step 4: Export streamWidget**

Update `src/widgets/index.ts`:

```ts
export * from "./components";
export * from "./diff";
export * from "./serialization";
export * from "./stream";
export * from "./template";
export * from "./types";
```

- [ ] **Step 5: Run stream tests**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: PASS for stream tests.

- [ ] **Step 6: Commit widget streaming**

Run:

```bash
git add src/widgets tests/widgets.test.ts
git commit -m "Add widget streaming helper"
```

## Task 7: Export Widget Public API

**Files:**
- Modify: `src/index.ts`
- Modify: `tests/exports.test.ts`

- [ ] **Step 1: Add failing public export test**

Update `tests/exports.test.ts` to import the widget API:

```ts
import {
  BaseStore,
  Card,
  ChatKitServer,
  NonStreamingResult,
  SQLiteStore,
  StreamingResult,
  WidgetTemplate,
  createActionConfig,
  decodeJsonBytes,
  diffWidget,
  encodeJsonBytes,
  streamWidget,
} from "../src";
```

Inside the existing public exports test, add:

```ts
expect(typeof Card).toBe("function");
expect(typeof WidgetTemplate).toBe("function");
expect(typeof diffWidget).toBe("function");
expect(typeof streamWidget).toBe("function");
```

- [ ] **Step 2: Run export tests and verify they fail**

Run:

```bash
bun test tests/exports.test.ts
```

Expected: FAIL because `src/index.ts` does not export `src/widgets`.

- [ ] **Step 3: Export widgets from package root**

Update `src/index.ts`:

```ts
export * from "./actions";
export * from "./errors";
export * from "./serialization";
export * from "./server";
export * from "./sqlite-store";
export * from "./store";
export * from "./types/core";
export * from "./types/server";
export * from "./widgets";
```

- [ ] **Step 4: Run export tests**

Run:

```bash
bun test tests/exports.test.ts
```

Expected: PASS.

- [ ] **Step 5: Commit public widget exports**

Run:

```bash
git add src/index.ts tests/exports.test.ts
git commit -m "Export widget APIs"
```

## Task 8: Full Verification And Parity Review

**Files:**
- Review: `src/widgets/*.ts`
- Review: `tests/widgets.test.ts`
- Review: `docs/superpowers/specs/2026-05-27-chatkit-widget-parity-design.md`
- Review: `package.json`

- [ ] **Step 1: Run focused widget tests**

Run:

```bash
bun test tests/widgets.test.ts
```

Expected: PASS with all widget tests.

- [ ] **Step 2: Run full verification**

Run:

```bash
bun run verify
```

Expected: PASS with typecheck and the full Bun test suite.

- [ ] **Step 3: Check the final diff against the approved spec**

Run:

```bash
git diff --stat master...HEAD
git diff -- src/widgets tests/widgets.test.ts tests/exports.test.ts src/index.ts package.json
```

Expected:

- Diff adds `src/widgets/`.
- Diff adds widget tests and fixtures.
- Diff exports widget APIs.
- Diff does not add Bun HTTP route helpers.
- Diff does not add Agents SDK conversion.
- Diff does not add upstream sync automation.

- [ ] **Step 4: Run a spec coverage checklist**

Confirm each approved requirement has an implementation location:

```text
Public widget module: src/widgets/index.ts and src/index.ts
Static builders/classes: src/widgets/components.ts
Shared widget data types: src/widgets/types.ts
Recursive serialization: src/widgets/serialization.ts
WidgetTemplate.fromFile/build/buildBasic: src/widgets/template.ts
Jinja-like strict rendering: src/widgets/template.ts and tests/widgets.test.ts
diffWidget: src/widgets/diff.ts
streamWidget: src/widgets/stream.ts
Translated test_widgets.py coverage: tests/widgets.test.ts
Fixtures: tests/assets/widgets
```

Expected: Every line maps to implemented code and tests.

- [ ] **Step 5: Commit final cleanup if needed**

If verification or the coverage checklist required small corrections, commit them:

```bash
git add src tests package.json bun.lock
git commit -m "Complete widget parity verification"
```

If no files changed after Step 2, skip this commit.

## Execution Notes

- Use TDD for each task: add the failing test, run it, implement the smallest code that passes, run the focused test, then commit.
- Keep widget runtime behavior JSON-first. Do not change `WidgetItemSchema.widget` away from plain JSON-compatible data.
- Keep dependency changes limited to `nunjucks` and `@types/nunjucks` unless the template fixture tests prove `nunjucks` cannot meet strict undefined behavior in Bun.
- If `nunjucks` fails strict undefined behavior, replace Task 1 dependency and Task 4 renderer code with the smallest renderer that passes the same tests; keep the public `WidgetTemplate` API unchanged.
- Do not implement HTTP helpers, Agents SDK conversion, or sync tooling in this milestone.
