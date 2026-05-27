import { describe, expect, test } from "bun:test";

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
  DynamicWidgetRootSchema,
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
  serializeWidget,
  Spacer,
  Text,
  Textarea,
  Title,
  Transition,
  Chart,
  type DynamicWidgetRoot,
} from "../src/widgets";

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

  test("keeps catalogue component names when props include type", () => {
    const widget = Card({
      type: "Basic",
      children: [Text({ type: "Card", value: "Hello" } as any)],
    } as any);

    expect(serializeWidget(widget)).toEqual({
      type: "Card",
      children: [{ type: "Text", value: "Hello" }],
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
});
