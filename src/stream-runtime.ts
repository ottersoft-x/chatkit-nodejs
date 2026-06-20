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
  const observedNext = next.catch((error: unknown) => {
    throw error;
  });
  void observedNext.catch(() => {});
  throwIfAborted(signal);

  let cleanup = (): void => {};
  const abort = new Promise<never>((_, reject) => {
    const rejectCancelled = () => reject(new StreamCancelledError());
    signal.addEventListener("abort", rejectCancelled, { once: true });
    cleanup = () => signal.removeEventListener("abort", rejectCancelled);
  });

  try {
    return await Promise.race([observedNext, abort]);
  } finally {
    cleanup();
  }
}

export async function returnIterator<T>(iterator: AsyncIterator<T>): Promise<void> {
  await iterator.return?.();
}
