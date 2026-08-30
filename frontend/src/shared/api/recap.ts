import { adaptProfiles, adaptRecap, applyExplanation } from './adapter';
import { createRecap, getExplanation, getProfiles, getRecap, getShareCard } from './client';
import { isRecapDTO, type RecapRequestStatusDTO, type ShareCardDTO } from './dto';
import { GenerationFailedError, pollRecapUntilReady } from './polling';
import { StreamUnavailableError, streamRecapUntilReady } from './streaming';
import type { Profile, Recap } from '@/shared/types/recap';

export type GenerationProgress = RecapRequestStatusDTO;
export type ProgressListener = (status: GenerationProgress) => void;

export function fetchProfiles(): Promise<Profile[]> {
  return getProfiles().then(adaptProfiles);
}

export async function generateRecap(
  profileId: string,
  year: number,
  onProgress?: ProgressListener,
  signal?: AbortSignal,
): Promise<Recap> {
  const response = await createRecap(profileId, year, signal);
  if (isRecapDTO(response)) return adaptRecap(response);
  let latest = response;
  try {
    const ready = await streamRecapUntilReady(response, {
      signal,
      onProgress: (status) => {
        latest = status;
        onProgress?.(status);
      },
    });
    return adaptRecap(ready);
  } catch (cause) {
    if (cause instanceof GenerationFailedError) throw cause;
    if (cause instanceof DOMException && cause.name === 'AbortError') throw cause;
    if (!(cause instanceof StreamUnavailableError)) throw cause;
  }
  const ready = await pollRecapUntilReady(latest, {
    fetchStatus: getRecap,
    onProgress,
    signal,
  });
  return adaptRecap(ready);
}

export async function loadRecap(recapId: string, signal?: AbortSignal): Promise<Recap> {
  const response = await getRecap(recapId, signal);
  if (isRecapDTO(response)) return adaptRecap(response);
  let latest = response;
  try {
    const ready = await streamRecapUntilReady(response, {
      signal,
      onProgress: (status) => {
        latest = status;
      },
    });
    return adaptRecap(ready);
  } catch (cause) {
    if (cause instanceof GenerationFailedError) throw cause;
    if (cause instanceof DOMException && cause.name === 'AbortError') throw cause;
    if (!(cause instanceof StreamUnavailableError)) throw cause;
  }
  const ready = await pollRecapUntilReady(latest, {
    fetchStatus: getRecap,
    signal,
  });
  return adaptRecap(ready);
}

export function loadExplanation(recap: Recap): Promise<Recap> {
  if (!recap.capabilities.explanationAvailable) return Promise.resolve(recap);
  return getExplanation(recap.recapId).then((dto) => applyExplanation(recap, dto));
}

export function loadShareCard(recapId: string): Promise<ShareCardDTO> {
  return getShareCard(recapId);
}
