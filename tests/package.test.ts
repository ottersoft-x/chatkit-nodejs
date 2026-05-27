import { describe, expect, test } from "bun:test";

import packageJson from "../package.json";

describe("package metadata", () => {
  test("stays private and exposes Bun verification scripts", () => {
    expect(packageJson.name).toBe("chatkit-bun");
    expect(packageJson.private).toBe(true);
    expect(packageJson.type).toBe("module");
    expect(packageJson.module).toBe("src/index.ts");
    expect(packageJson.scripts).toMatchObject({
      test: "bun test",
      typecheck: "bunx tsc --noEmit",
      verify: "bun run typecheck && bun test",
    });
  });

  test("declares zod as runtime validation dependency", () => {
    expect(typeof packageJson.dependencies?.zod).toBe("string");
  });
});
