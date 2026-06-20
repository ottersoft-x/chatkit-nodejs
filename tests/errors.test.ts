import { describe, test } from "node:test";

import { expect } from "./helpers/expect.js";

import { CustomStreamError, StreamError } from "../src/errors.js";

describe("errors", () => {
  test("StreamError defaults stream.error to retryable", () => {
    const error = new StreamError("stream.error");

    expect(error).toBeInstanceOf(StreamError);
    expect(error.name).toBe("StreamError");
    expect(error.message).toBe("stream.error");
    expect(error.code).toBe("stream.error");
    expect(error.allowRetry).toBe(true);
  });

  test("StreamError allows custom codes and retry override", () => {
    const error = new StreamError("rate_limit.exceeded", { allowRetry: true });

    expect(error.message).toBe("rate_limit.exceeded");
    expect(error.code).toBe("rate_limit.exceeded");
    expect(error.allowRetry).toBe(true);
  });

  test("StreamError defaults unknown codes to non-retryable", () => {
    const error = new StreamError("rate_limit.exceeded");

    expect(error.allowRetry).toBe(false);
  });

  test("CustomStreamError uses custom code and defaults to non-retryable", () => {
    const error = new CustomStreamError("Try again later");

    expect(error).toBeInstanceOf(CustomStreamError);
    expect(error.name).toBe("CustomStreamError");
    expect(error.message).toBe("Try again later");
    expect(error.code).toBe("custom");
    expect(error.allowRetry).toBe(false);
  });

  test("CustomStreamError allows retry override", () => {
    const error = new CustomStreamError("Try again later", { allowRetry: true });

    expect(error.allowRetry).toBe(true);
  });
});
