export class ChatKitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class NotFoundError extends ChatKitError {}

export class UnsupportedOperationError extends ChatKitError {}

export class ValidationError extends ChatKitError {
  constructor(
    message: string,
    override readonly cause?: unknown,
  ) {
    super(message);
  }
}

export type StreamErrorCode = "stream.error" | "custom" | (string & {});

export interface StreamErrorOptions {
  allowRetry?: boolean;
}

function defaultAllowRetry(code: StreamErrorCode): boolean {
  return code === "stream.error";
}

export class StreamError extends ChatKitError {
  readonly allowRetry: boolean;

  constructor(
    readonly code: StreamErrorCode,
    options: StreamErrorOptions = {},
  ) {
    super(code);
    this.allowRetry = options.allowRetry ?? defaultAllowRetry(code);
  }
}

export class CustomStreamError extends ChatKitError {
  readonly code = "custom";
  readonly allowRetry: boolean;

  constructor(
    message: string,
    options: StreamErrorOptions = {},
  ) {
    super(message);
    this.allowRetry = options.allowRetry ?? false;
  }
}
