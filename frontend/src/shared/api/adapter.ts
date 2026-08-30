import type {
  AchievementLevel,
  DistrictCardDTO,
  MetricCardDTO,
  ProfileDTO,
  RecapDTO,
  RecapExplanationDTO,
  RuleFactDTO,
  VerticalCode,
} from './dto';
import {
  OTHER_DISTRICT,
  type Badge,
  type BrandTone,
  type Chapter,
  type District,
  type Profile,
  type Recap,
} from '@/shared/types/recap';

/**
 * Приведение контракта бэкенда к модели экранов.
 * Здесь и только здесь разрешено знать про обе стороны.
 *
 * Принцип: ничего не выдумывать. Если бэкенд поля не отдаёт — оно остаётся
 * пустым, а компонент его не рисует. Подставлять нули и правдоподобные числа
 * нельзя: recap, который врёт, хуже recap, который чего-то не показывает.
 */

/** Районы разложены по брендовым цветам Авито — пять вертикалей на четыре тона. */
const VERTICAL_TONE: Record<VerticalCode, { tone: BrandTone; shade: 0 | 1 | 2 }> = {
  real_estate: { tone: 'blue', shade: 0 },
  transport: { tone: 'blue', shade: 2 },
  goods: { tone: 'purple', shade: 0 },
  services: { tone: 'purple', shade: 1 },
  jobs: { tone: 'green', shade: 0 },
};

const PROFILE_TONES: BrandTone[] = ['blue', 'purple', 'green', 'red'];

/**
 * Тексты бэкенда местами обращаются к пользователю в мужском роде.
 * На фронте приводим известные формулировки к нейтральным: так одна и та же
 * копия подходит любому профилю без ветвления по имени или полу.
 */
function neutralizeProfileCopy(text: string | undefined): string | undefined {
  if (!text) return text;

  const replacements: Array<[RegExp, string]> = [
    [/Ты сохранял объявления, к которым хотелось вернуться/gu, 'Сохранялись объявления, к которым хотелось вернуться'],
    [/Ты внимательно исследовал предложения города/gu, 'Предложения города изучались особенно внимательно'],
    [/Ты регулярно пополнял город своими предложениями/gu, 'В городе регулярно появлялись новые собственные предложения'],
    [/Ты был активен в покупательских и продавцовских сценариях/gu, 'Активность была заметна и в покупательских, и в продавцовских сценариях'],
    [/Ты успешно завершал продажи и уверенно управлял своей витриной/gu, 'Продажи успешно завершались, а витрина оставалась под контролем'],
    [/Ты собрал коллекцию объявлений, к которым хотелось возвращаться/gu, 'Собралась коллекция объявлений, к которым хотелось возвращаться'],
    [/Ты исследовал разные улицы и категории города Авито/gu, 'Исследовались разные улицы и категории города Авито'],
    [/Ты регулярно возвращался в город в течение года/gu, 'Возвращение в город происходило регулярно в течение года'],
    [/Ты использовал доставку, чтобы сделки проходили удобнее/gu, 'Доставка помогала проводить сделки удобнее'],
    [/Ты активно пополнял собственную витрину объявлениями/gu, 'Собственная витрина активно пополнялась объявлениями'],
    [/Ты находил подходящие предложения и завершал покупки/gu, 'Находились подходящие предложения и завершались покупки'],
    [/Ты сохранял серию активности несколько дней подряд/gu, 'Активность сохранялась несколько дней подряд'],
    [/Ты много изучал предложения, сохранял интересные варианты и редко переходил к завершённым сделкам/gu, 'Предложения активно изучались, интересные варианты сохранялись, а до завершённых сделок доходило реже'],
    [/Ты исследовал много категорий и несколько районов без одного доминирующего направления/gu, 'Исследовались разные категории и несколько районов без одного доминирующего направления'],
    [/Ты внимательно изучал и сохранял предложения, не торопясь переходить к покупке/gu, 'Предложения внимательно изучались и сохранялись без спешки с покупкой'],
    [/Ты возвращался в город регулярно на протяжении значительной части года/gu, 'Возвращение в город происходило регулярно на протяжении значительной части года'],
    [/Ты был активен ([^.]+)(\.)/gu, 'Активность сохранялась на протяжении $1$2'],
    [/Ты совершил (\d+) сделок/gu, 'Завершённых сделок за год: $1'],
  ];

  return replacements.reduce(
    (result, [pattern, replacement]) => result.replace(pattern, replacement),
    text,
  );
}

const LEVEL_TITLE: Record<AchievementLevel, string> = {
  newcomer: 'Новичок',
  local: 'Местный',
  expert: 'Эксперт',
  guru: 'Гуру',
};

/**
 * Подписи метрик. Дублируют таблицу `metric_definitions` на бэкенде —
 * это временное решение: как только titles поедут в API, словарь удаляется.
 */
const METRIC_LABEL: Record<string, string> = {
  active_days: 'Активные дни',
  active_months: 'Активные месяцы',
  buyer_actions_count: 'Покупательские действия',
  chats_started_count: 'Начатые чаты',
  completed_deals_count: 'Завершённые сделки',
  delivery_count: 'Использования доставки',
  delivery_usage_rate: 'Доля сделок с доставкой',
  favorite_to_purchase_rate: 'Отношение избранного к покупкам',
  favorites_count: 'Добавления в избранное',
  max_activity_streak: 'Максимальная серия активности',
  meaningful_events: 'Значимые действия',
  publish_to_sale_rate: 'Отношение публикаций к продажам',
  published_listings_count: 'Опубликованные объявления',
  purchases_count: 'Покупки',
  sales_count: 'Продажи',
  saved_searches_count: 'Сохранённые поиски',
  seller_actions_count: 'Продавцовские действия',
  top_category_share: 'Доля главной категории',
  top_vertical_share: 'Доля главной вертикали',
  unique_categories: 'Уникальные категории',
  unique_verticals: 'Уникальные вертикали',
  views_count: 'Просмотры объявлений',
};

const OPERATOR_WORD: Record<RuleFactDTO['operator'], string> = {
  gte: 'не меньше',
  gt: 'больше',
  lte: 'не больше',
  lt: 'меньше',
  eq: 'ровно',
  neq: 'не равно',
};

export function adaptProfiles(items: ProfileDTO[]): Profile[] {
  return items.map((item, index) => ({
    id: item.id,
    name: item.name,
    description: item.description,
    // Пустой массив вместо undefined: экран не должен падать,
    // если бэкенд не прислал годы для профиля.
    availableYears: item.available_years ?? [],
    avatarUrl: item.avatar_url,
    tone: PROFILE_TONES[index % PROFILE_TONES.length],
  }));
}

/** Сид города из `activity_hash`: та же активность — та же застройка. */
function seedFromHash(activityHash: string): number {
  const hex = activityHash.replace(/^sha256:/, '').slice(0, 8);
  const parsed = Number.parseInt(hex, 16);
  return Number.isNaN(parsed) ? 1 : parsed;
}

function formatMetric(value: number): string {
  return Number.isInteger(value) ? value.toLocaleString('ru-RU') : value.toFixed(2);
}

function factToText(fact: RuleFactDTO): string {
  const label = METRIC_LABEL[fact.metric_code] ?? fact.metric_code;
  return `${label}: ${formatMetric(fact.actual)} — ${OPERATOR_WORD[fact.operator]} ${formatMetric(fact.threshold)}`;
}

/**
 * Районы. Бэкенд отдаёт только главную вертикаль и её долю, поэтому город
 * состоит максимум из двух кварталов: известного и «остального».
 * Разложить остаток по вертикалям нечем — угадывать мы не будем.
 */
function buildDistricts(card: DistrictCardDTO | undefined): District[] {
  if (!card) return [];

  const { vertical, activity_share: share, top_category: topCategory } = card.data;
  const palette = VERTICAL_TONE[vertical.code] ?? { tone: 'blue', shade: 0 };

  const districts: District[] = [
    {
      id: vertical.code,
      title: vertical.title,
      share,
      tone: palette.tone,
      shade: palette.shade,
      topCategory: topCategory?.title,
    },
  ];

  const rest = 1 - share;
  if (rest > 0.005) {
    districts.push({
      id: OTHER_DISTRICT,
      title: 'Остальной город',
      share: rest,
      tone: 'blue',
      shade: 2,
    });
  }

  return districts;
}

export function adaptRecap(dto: RecapDTO): Recap {
  const cards = [...dto.cards].sort((a, b) => a.position - b.position);
  const profileName = dto.profile.name;

  const districtCard = cards.find((card) => card.type === 'district');
  const archetypeCard = cards.find((card) => card.type === 'archetype');
  const achievementsCard = cards.find((card) => card.type === 'achievements');
  const summaryCard = cards.find((card) => card.type === 'summary');
  const activeDaysCard = cards.find(
    (card): card is MetricCardDTO => card.type === 'metric' && card.data.metric_code === 'active_days',
  );

  const districts = buildDistricts(districtCard);
  const mainDistrictId = districts[0]?.id;

  const badges: Badge[] = (achievementsCard?.data.items ?? []).map((item) => ({
    id: item.code,
    group: item.level,
    groupTitle: LEVEL_TITLE[item.level],
    title: item.title,
    reason: neutralizeProfileCopy(item.description),
    facts: [],
  }));

  const chapters: Chapter[] = cards.map((card, index) => {
    const base = {
      index: index + 1,
      kind: card.type,
      eyebrow: card.eyebrow ?? undefined,
      title: card.title,
      narrative: neutralizeProfileCopy(card.description ?? '') ?? '',
    };

    switch (card.type) {
      case 'intro':
        // Город обязан существовать с первого экрана, иначе три главы подряд
        // показывают пустое поле: карточка района у бэкенда только четвёртая.
        return { ...base, kind: 'intro' as const, districtId: mainDistrictId };
      case 'metric':
        return {
          ...base,
          kind: 'metric' as const,
          stat: {
            value: formatMetric(card.data.value),
            label: METRIC_LABEL[card.data.metric_code] ?? card.data.metric_code,
          },
          // secondary_label уточняет цифру («Активность была заметна в 1 месяце»),
          // description у метрик обычно пустое — берём то, что есть.
          narrative: card.data.secondary_label ?? base.narrative,
          districtId: mainDistrictId,
        };
      case 'district':
        return {
          ...base,
          kind: 'district' as const,
          stat: {
            value: `${Math.round(card.data.activity_share * 100)}%`,
            label: 'года прошло в этом районе',
          },
          // Фокус на настоящем районе бэкенда. Раньше здесь стоял OTHER_DISTRICT,
          // из-за чего подсвечивался несуществующий квартал, а реальный гас.
          districtId: mainDistrictId,
        };
      default:
        return base;
    }
  });

  return {
    recapId: dto.id,
    profileId: dto.profile_id,
    profileName,
    year: dto.year,
    rulesVersion: dto.generation.algorithm_version,
    seed: seedFromHash(dto.generation.activity_hash),
    // Своего названия города у бэкенда нет: берём заголовок summary,
    // он сгенерирован под этого пользователя.
    cityName: summaryCard?.title ?? 'Твой город за год',
    // Готовая персональная суммаризация. Одинаково работает и для template,
    // и для mistral — фронт свою версию не пишет и источник не различает.
    summaryText: neutralizeProfileCopy(summaryCard?.description ?? undefined),
    totals: {
      activeDays: activeDaysCard?.data.value,
      districts: districts.length,
      sites: 0,
    },
    role: { title: archetypeCard?.data.role.title ?? '—' },
    style: { title: archetypeCard?.data.style.title ?? '—' },
    districts,
    chapters,
    badges,
    // «Что не достроено» бэкенд не моделирует. Панель скрывается,
    // а не заполняется выдуманными строками.
    unfinished: [],
    capabilities: {
      shareAvailable: dto.capabilities.share_available,
      explanationAvailable: dto.capabilities.explanation_available,
      feedbackAvailable: dto.capabilities.feedback_available,
    },
    narrativeSource: dto.generation.narrative.source,
  };
}

/** Догружает обоснования решений в уже собранный recap. */
export function applyExplanation(recap: Recap, dto: RecapExplanationDTO): Recap {
  const role = dto.decisions.find((decision) => decision.kind === 'archetype_role');
  const style = dto.decisions.find((decision) => decision.kind === 'archetype_style');

  return {
    ...recap,
    role: { ...recap.role, reason: neutralizeProfileCopy(role?.reason) },
    style: { ...recap.style, reason: neutralizeProfileCopy(style?.reason) },
    badges: recap.badges.map((badge) => {
      const decision = dto.decisions.find(
        (item) => item.kind === 'achievement' && item.code === badge.id,
      );
      if (!decision) return badge;
      return {
        ...badge,
        reason: neutralizeProfileCopy(decision.reason),
        facts: decision.facts.map(factToText),
      };
    }),
  };
}
