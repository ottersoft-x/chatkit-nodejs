export interface ChatKitStreamRuntime {
  signal: AbortSignal;
  supportsExplicitCancel: boolean;
}

export interface ChatKitStreamRuntimeOptions {
  signal?: AbortSignal;
  supportsExplicitCancel?: boolean;
}

export class StreamCancelledError extends Error {
  constructor(message = "Stream cancelled") {
    super(message);
    this.name = "StreamCancelledError";
  }
}

export function defaultChatKitStreamRuntime(
  options: ChatKitStreamRuntimeOptions = {},
): ChatKitStreamRuntime {
  return {
    signal: options.signal ?? new AbortController().signal,
    supportsExplicitCancel: options.supportsExplicitCancel ?? false,
  };
}

export function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) {
    throw new StreamCancelledError();
  }
}

export async function nextWithAbort<T>(
  next: Promise<IteratorResult<T>>,
  signal: AbortSignal,
): Promise<IteratorResult<T>> {
  // Observe rejections so an abort that wins the race below cannot leave the
  // losing next() promise as an unhandled rejection.
  void next.catch(() => {});
  throwIfAborted(signal);

  let cleanup = (): void => {};
  const abort = new Promise<never>((_, reject) => {
    const rejectCancelled = () => reject(new StreamCancelledError());
    signal.addEventListener("abort", rejectCancelled, { once: true });
    cleanup = () => signal.removeEventListener("abort", rejectCancelled);
  });

  try {
    return await Promise.race([next, abort]);
  } finally {
    cleanup();
  }
}

export async function returnIterator<T>(iterator: AsyncIterator<T>): Promise<void> {
  await iterator.return?.();
}
