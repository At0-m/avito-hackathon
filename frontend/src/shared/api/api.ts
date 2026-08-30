export const API_BASE = '/api/v1';

export const endpoints = {
  profiles: () => `${API_BASE}/profiles`,

  profile: (profileId: string) => `${API_BASE}/profiles/${profileId}`,

  recaps: () => `${API_BASE}/recaps`,

  recap: (recapId: string) => `${API_BASE}/recaps/${recapId}`,

  recapStream: (recapId: string) => `${API_BASE}/recaps/${recapId}/stream`,

  recapExplanation: (recapId: string) => `${API_BASE}/recaps/${recapId}/explanation`,

  recapShare: (recapId: string) => `${API_BASE}/recaps/${recapId}/share`,

  recapInteractions: (recapId: string) => `${API_BASE}/recaps/${recapId}/interactions`,
} as const;
