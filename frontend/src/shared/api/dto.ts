export type VerticalCode = 'goods' | 'transport' | 'real_estate' | 'jobs' | 'services';

export type CategoryCode =
  | 'electronics'
  | 'home_and_garden'
  | 'clothing_and_accessories'
  | 'hobbies_and_leisure'
  | 'cars'
  | 'apartments'
  | 'vacancies'
  | 'personal_services';

export type AccentToken = 'violet' | 'blue' | 'green' | 'orange';

export type ArchetypeRoleCode =
  | 'findings_seeker'
  | 'showcase_owner'
  | 'universal_citizen'
  | 'city_observer';

export type ArchetypeStyleCode =
  | 'thoughtful'
  | 'explorer'
  | 'district_expert'
  | 'regular'
  | 'result_oriented'
  | 'city_local';

export type AchievementCode =
  | 'deal_master'
  | 'findings_collector'
  | 'city_navigator'
  | 'frequent_guest'
  | 'old_timer'
  | 'doorstep_delivery'
  | 'own_showcase'
  | 'findings_hunter'
  | 'city_rhythm';

export type AchievementLevel = 'newcomer' | 'local' | 'expert' | 'guru';

export type MetricUnit =
  | 'events'
  | 'days'
  | 'months'
  | 'items'
  | 'categories'
  | 'verticals'
  | 'actions'
  | 'ratio';

export type RuleOperator = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte';

export interface ProfileDTO {
  id: string;
  name: string;
  description: string;
  avatar_url?: string | null;
  available_years: number[];
}

export interface VerticalDTO {
  code: VerticalCode;
  title: string;
}

export interface CategoryDTO {
  code: CategoryCode;
  title: string;
  vertical_code: VerticalCode;
}

export interface MetricValueDTO {
  metric_code: string;
  value: number;
  unit?: MetricUnit | null;
  secondary_label?: string | null;
}

export interface AchievementDTO {
  code: AchievementCode;
  title: string;
  description: string;
  level: AchievementLevel;
  icon: string;
  metric_code?: string;
  current_value?: number | null;
  next_level_threshold?: number | null;
}

export interface CardVisualDTO {
  kind:
    | 'illustration'
    | 'district'
    | 'street'
    | 'calendar'
    | 'badge'
    | 'chart'
    | 'character'
    | 'skyline';
  asset_code?: string | null;
}

interface BaseCardDTO {
  id: string;
  position: number;
  visibility: 'personal' | 'shareable';
  eyebrow?: string | null;
  title: string;
  description?: string | null;
  visual?: CardVisualDTO;
  explainable: boolean;
}

export interface IntroCardDTO extends BaseCardDTO {
  type: 'intro';
  data: { year: number };
}

export interface MetricCardDTO extends BaseCardDTO {
  type: 'metric';
  data: MetricValueDTO;
}

export interface DistrictCardDTO extends BaseCardDTO {
  type: 'district';
  data: {
    vertical: VerticalDTO;
    activity_share: number;
    top_category?: CategoryDTO;
  };
}

export interface ArchetypeCardDTO extends BaseCardDTO {
  type: 'archetype';
  data: {
    role: { code: ArchetypeRoleCode; title: string };
    style: { code: ArchetypeStyleCode; title: string };
  };
}

export interface AchievementsCardDTO extends BaseCardDTO {
  type: 'achievements';
  data: { items: AchievementDTO[]; total_count: number };
}

export interface SummaryCardDTO extends BaseCardDTO {
  type: 'summary';
  data: {
    role_code: ArchetypeRoleCode;
    style_code: ArchetypeStyleCode;
    achievement_codes: AchievementCode[];
  };
}

export interface FinalCardDTO extends BaseCardDTO {
  type: 'final';
  data: { show_share_button: boolean; show_feedback: boolean };
}

export type RecapCardDTO =
  | IntroCardDTO
  | MetricCardDTO
  | DistrictCardDTO
  | ArchetypeCardDTO
  | AchievementsCardDTO
  | SummaryCardDTO
  | FinalCardDTO;

export interface RecapDTO {
  schema_version: string;
  id: string;
  profile_id: string;
  year: number;
  profile: { id: string; name: string; avatar_url?: string | null };
  generation: {
    algorithm_version: string;
    feature_schema_version: string;
    activity_hash: string;
    generated_at: string;
    narrative: { source: 'mistral' | 'template'; prompt_version: string; model?: string | null };
  };
  theme: { code: 'city'; main_district: VerticalDTO; accent_token?: AccentToken | null };
  cards: RecapCardDTO[];
  capabilities: {
    share_available: boolean;
    explanation_available: boolean;
    feedback_available: boolean;
  };
}

export type RecapRequestState = 'queued' | 'processing' | 'ready' | 'failed';

export interface RecapRequestStatusDTO {
  id: string;
  profile_id: string;
  year: number;
  status: RecapRequestState;
  stage: string;
  progress_percent: number;
  attempt: number;
  max_attempts: number;
  poll_after_ms: number;
  links: { self: string; stream?: string };
  error?: { code: string; message: string; retryable: boolean };
}

export type RecapResponseDTO = RecapDTO | RecapRequestStatusDTO;

export function isRecapDTO(value: RecapResponseDTO): value is RecapDTO {
  return 'schema_version' in value && 'cards' in value;
}

export interface RuleFactDTO {
  metric_code: string;
  actual: number;
  operator: RuleOperator;
  threshold: number;
  matched: boolean;
}

export interface DecisionExplanationDTO {
  card_id: string;
  kind: 'archetype_role' | 'archetype_style' | 'achievement';
  code: string;
  reason: string;
  rule_version: string;
  facts: RuleFactDTO[];
}

export interface RecapExplanationDTO {
  recap_id: string;
  algorithm_version: string;
  activity_hash: string;
  decisions: DecisionExplanationDTO[];
}

export interface ShareCardDTO {
  schema_version: string;
  recap_id: string;
  profile_name: string;
  avatar_url?: string;
  year: number;
  title: string;
  subtitle: string;
  main_district: VerticalDTO;
  facts: { kind: 'main_district' | 'active_days' | 'top_achievement'; label: string; value: string }[];
  achievements: { code: AchievementCode; title: string; level: AchievementLevel; icon: string }[];
  visual: { theme: 'city' };
}

export type APIErrorCode =
  | 'invalid_argument'
  | 'profile_not_found'
  | 'recap_not_found'
  | 'explanation_not_available'
  | 'share_not_available'
  | 'insufficient_activity'
  | 'rate_limit_exceeded'
  | 'dependency_unavailable'
  | 'internal_error';

export interface APIErrorDTO {
  code: APIErrorCode;
  message: string;
  request_id: string;
  details?: { field?: string | null; reason: string }[];
}
