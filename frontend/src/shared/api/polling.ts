import { isRecapDTO, type RecapDTO, type RecapRequestStatusDTO, type RecapResponseDTO } from './dto.ts';

export class GenerationFailedError extends Error {
  readonly code: string;
  readonly retryable: boolean;

  constructor(code: string, message: string, retryable: boolean) {
    super(message);
    this.name = 'GenerationFailedError';
    this.code = code;
    this.retryable = retryable;
  }
}

export interface PollRecapOptions {
  fetchStatus: (id: string, signal?: AbortSignal) => Promise<RecapResponseDTO>;
  onProgress?: (status: RecapRequestStatusDTO) => void;
  signal?: AbortSignal;
  wait?: (milliseconds: number, signal?: AbortSignal) => Promise<void>;
  minimumDelayMS?: number;
}

export async function pollRecapUntilReady(
  initial: RecapRequestStatusDTO,
  options: PollRecapOptions,
): Promise<RecapDTO> {
  const wait = options.wait ?? abortableDelay;
  const minimumDelayMS = Math.max(0, options.minimumDelayMS ?? 150);
  let status = initial;

  while (true) {
    options.onProgress?.(status);
    if (status.status === 'failed') {
      throw new GenerationFailedError(
        status.error?.code ?? 'internal_error',
        status.error?.message ?? 'Не удалось собрать итоги',
        status.error?.retryable ?? false,
      );
    }
    if (status.status === 'ready') {
      throw new GenerationFailedError(
        'internal_error',
        'Готовый результат временно недоступен',
        true,
      );
    }

    const delay = Math.max(minimumDelayMS, status.poll_after_ms || 500);
    await wait(delay, options.signal);

    const response = await options.fetchStatus(status.id, options.signal);
    if (isRecapDTO(response)) return response;
    status = response;
  }
}

export function abortableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException('Aborted', 'AbortError'));
      return;
    }

    const onAbort = () => {
      globalThis.clearTimeout(timeout);
      reject(new DOMException('Aborted', 'AbortError'));
    };
    const timeout = globalThis.setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, milliseconds);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
