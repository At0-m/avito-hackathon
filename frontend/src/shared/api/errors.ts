import { APIError, NetworkError } from './client';
import { GenerationFailedError } from './polling';

export interface FailureView {
  title: string;
  hint?: string;
  retryable: boolean;
}

const BY_CODE: Record<string, FailureView> = {
  insufficient_activity: {
    title: 'За этот год слишком мало действий',
    hint: 'Чтобы собрать итоги, нужно хотя бы несколько недель активности: просмотры, избранное, диалоги. Пока их не хватает даже на один квартал.',
    retryable: false,
  },
  profile_not_found: {
    title: 'Профиль не найден',
    hint: 'Возможно, его больше нет. Выбери другой из списка.',
    retryable: false,
  },
  recap_not_found: {
    title: 'Такого города нет',
    hint: 'Итоги по этой ссылке не найдены — попробуй собрать их заново.',
    retryable: false,
  },
  invalid_argument: {
    title: 'Запрос не принят',
    hint: 'Что-то не так с параметрами. Вернись к выбору профиля и попробуй ещё раз.',
    retryable: false,
  },
  rate_limit_exceeded: {
    title: 'Слишком много запросов',
    hint: 'Подожди несколько секунд и повтори.',
    retryable: true,
  },
  dependency_unavailable: {
    title: 'Источник данных временно недоступен',
    hint: 'Это временно. Попробуй повторить через минуту.',
    retryable: true,
  },
  internal_error: {
    title: 'Что-то сломалось на сервере',
    hint: 'Мы не смогли собрать итоги. Попробуй ещё раз.',
    retryable: true,
  },
  explanation_not_available: {
    title: 'Обоснование недоступно',
    retryable: false,
  },
  share_not_available: {
    title: 'Публичная карточка недоступна',
    retryable: false,
  },
};

export function describeFailure(error: unknown): FailureView {
  if (error instanceof APIError) {
    return (
      BY_CODE[error.code] ?? {
        title: 'Не удалось выполнить запрос',
        hint: 'Попробуй ещё раз.',
        retryable: true,
      }
    );
  }

  if (error instanceof GenerationFailedError) {
    const view = BY_CODE[error.code] ?? {
      title: 'Не удалось собрать итоги',
      hint: 'Попробуй повторить генерацию.',
      retryable: error.retryable,
    };
    return { ...view, retryable: error.retryable };
  }

  if (error instanceof NetworkError) {
    return {
      title: 'Сервис недоступен',
      hint: 'Похоже, нет связи с сервером. Проверь подключение и повтори.',
      retryable: true,
    };
  }

  return {
    title: 'Непредвиденная ошибка',
    hint: 'Попробуй вернуться назад и повторить.',
    retryable: true,
  };
}
