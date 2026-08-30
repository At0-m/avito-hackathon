import {
  isRecapDTO,
  type RecapDTO,
  type RecapRequestStatusDTO,
  type RecapResponseDTO,
} from './dto.ts';
import { GenerationFailedError } from './polling.ts';

export interface EventSourceLike {
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void;
  close(): void;
  onerror: ((event: Event) => void) | null;
}

export type EventSourceFactory = (url: string) => EventSourceLike;

export interface StreamRecapOptions {
  onProgress?: (status: RecapRequestStatusDTO) => void;
  signal?: AbortSignal;
  eventSourceFactory?: EventSourceFactory;
}

export class StreamUnavailableError extends Error {
  constructor() {
    super('SSE stream is unavailable');
    this.name = 'StreamUnavailableError';
  }
}

export function streamRecapUntilReady(
  initial: RecapRequestStatusDTO,
  options: StreamRecapOptions = {},
): Promise<RecapDTO> {
  const factory = options.eventSourceFactory ?? defaultEventSourceFactory;
  const streamURL = initial.links.stream;
  if (!factory || !streamURL) return Promise.reject(new StreamUnavailableError());

  return new Promise((resolve, reject) => {
    let settled = false;
    const source = factory(streamURL);

    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      options.signal?.removeEventListener('abort', onAbort);
      source.close();
      callback();
    };

    const parse = (event: MessageEvent<string>): RecapResponseDTO =>
      JSON.parse(event.data) as RecapResponseDTO;

    const onStatus = (event: MessageEvent<string>) => {
      try {
        const value = parse(event);
        if (!isRecapDTO(value)) options.onProgress?.(value);
      } catch {
        finish(() => reject(new StreamUnavailableError()));
      }
    };
    const onReady = (event: MessageEvent<string>) => {
      try {
        const value = parse(event);
        if (!isRecapDTO(value)) throw new StreamUnavailableError();
        finish(() => resolve(value));
      } catch (cause) {
        finish(() => reject(cause));
      }
    };
    const onFailed = (event: MessageEvent<string>) => {
      try {
        const value = parse(event);
        if (isRecapDTO(value)) throw new StreamUnavailableError();
        options.onProgress?.(value);
        finish(() =>
          reject(
            new GenerationFailedError(
              value.error?.code ?? 'internal_error',
              value.error?.message ?? 'Не удалось собрать итоги',
              value.error?.retryable ?? false,
            ),
          ),
        );
      } catch (cause) {
        finish(() => reject(cause));
      }
    };
    const onAbort = () =>
      finish(() => reject(new DOMException('Aborted', 'AbortError')));

    source.addEventListener('status', onStatus);
    source.addEventListener('ready', onReady);
    source.addEventListener('failed', onFailed);
    source.onerror = () => finish(() => reject(new StreamUnavailableError()));

    if (options.signal?.aborted) onAbort();
    else options.signal?.addEventListener('abort', onAbort, { once: true });
  });
}

const defaultEventSourceFactory: EventSourceFactory | undefined =
  typeof EventSource === 'undefined' ? undefined : (url) => new EventSource(url);
