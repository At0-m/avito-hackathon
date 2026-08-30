import { endpoints } from './api';
import type {
  APIErrorCode,
  APIErrorDTO,
  ProfileDTO,
  RecapExplanationDTO,
  RecapResponseDTO,
  ShareCardDTO,
} from './dto';

export class APIError extends Error {
  readonly code: APIErrorCode;
  readonly status: number;
  readonly requestId: string;

  constructor(status: number, payload: APIErrorDTO) {
    super(payload.message);
    this.name = 'APIError';
    this.status = status;
    this.code = payload.code;
    this.requestId = payload.request_id;
  }
}

export class NetworkError extends Error {
  constructor(cause: unknown) {
    super('Сервис недоступен');
    this.name = 'NetworkError';
    this.cause = cause;
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url, {
      ...init,
      headers: { 'Content-Type': 'application/json', ...init?.headers },
    });
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === 'AbortError') throw cause;
    throw new NetworkError(cause);
  }

  const body: unknown = await response.json().catch(() => null);

  if (!response.ok) {
    if (body && typeof body === 'object' && 'code' in body) {
      throw new APIError(response.status, body as APIErrorDTO);
    }
    throw new NetworkError(new Error(`HTTP ${response.status}`));
  }

  return body as T;
}

export function getProfiles(): Promise<ProfileDTO[]> {
  return request<ProfileDTO[]>(endpoints.profiles());
}

export function createRecap(
  profileId: string,
  year: number,
  signal?: AbortSignal,
): Promise<RecapResponseDTO> {
  return request<RecapResponseDTO>(endpoints.recaps(), {
    method: 'POST',
    body: JSON.stringify({ profile_id: profileId, year }),
    signal,
  });
}

export function getRecap(recapId: string, signal?: AbortSignal): Promise<RecapResponseDTO> {
  return request<RecapResponseDTO>(endpoints.recap(recapId), { signal });
}

export function getExplanation(recapId: string): Promise<RecapExplanationDTO> {
  return request<RecapExplanationDTO>(endpoints.recapExplanation(recapId));
}

export function getShareCard(recapId: string): Promise<ShareCardDTO> {
  return request<ShareCardDTO>(endpoints.recapShare(recapId));
}
