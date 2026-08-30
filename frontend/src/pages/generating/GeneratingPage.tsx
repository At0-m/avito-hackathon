import { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { generateRecap, type GenerationProgress } from '@/shared/api/recap';
import { describeFailure, type FailureView } from '@/shared/api/errors';
import { cacheRecap } from '@/shared/api/cache';
import './GeneratingPage.css';

const STEPS = [
  'Читаем действия за год',
  'Раскладываем их по районам',
  'Ищем повторяющиеся сценарии',
  'Назначаем роль и звания',
  'Строим город',
] as const;

const MIN_VISIBLE_MS = 900;

const STAGE_TO_STEP: Record<string, number> = {
  queued: 0,
  starting: 0,
  loading_activity: 0,
  computing_features: 1,
  personalizing: 3,
  generating_narrative: 3,
  persisting_snapshot: 4,
  ready: 4,
};

export function GeneratingPage() {
  const { profileId = '', year = '' } = useParams();
  const navigate = useNavigate();
  const [step, setStep] = useState(0);
  const [progress, setProgress] = useState(0);
  const [failure, setFailure] = useState<FailureView | null>(null);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    const startedAt = Date.now();

    const holdRemaining = () =>
      new Promise<void>((resolve) =>
        window.setTimeout(resolve, Math.max(0, MIN_VISIBLE_MS - (Date.now() - startedAt))),
      );

    const onProgress = (status: GenerationProgress) => {
      if (!active) return;
      const boundedProgress = Math.max(0, Math.min(100, status.progress_percent));
      const stageStep =
        STAGE_TO_STEP[status.stage] ?? Math.min(Math.floor(boundedProgress / 20), STEPS.length - 1);
      setStep((current) => Math.max(current, stageStep));
      setProgress((current) => Math.max(current, boundedProgress));
    };

    void generateRecap(profileId, Number(year), onProgress, controller.signal)
      .then(async (recap) => {
        await holdRemaining();
        if (!active) return;
        setStep(STEPS.length - 1);
        setProgress(100);
        cacheRecap(recap);
        void navigate(`/recap/${recap.recapId}`, { replace: true });
      })
      .catch((cause: unknown) => {
        if (!active) return;
        if (cause instanceof DOMException && cause.name === 'AbortError') return;
        setFailure(describeFailure(cause));
      });

    return () => {
      active = false;
      controller.abort();
    };
  }, [navigate, profileId, year]);

  if (failure) {
    return (
      <main className="generating">
        <div className="generating__inner">
          <p className="kicker">Города пока нет</p>
          <h1 className="generating__title">{failure.title}</h1>
          {failure.hint && <p className="generating__hint">{failure.hint}</p>}
          <div className="generating__actions">
            {failure.retryable && (
              <button
                type="button"
                className="btn btn--primary"
                onClick={() => window.location.reload()}
              >
                Повторить
              </button>
            )}
            <button
              type="button"
              className={failure.retryable ? 'btn btn--ghost' : 'btn btn--primary'}
              onClick={() => void navigate('/')}
            >
              Выбрать другой профиль
            </button>
          </div>
        </div>
      </main>
    );
  }

  const visibleProgress = Math.max(progress, ((step + 1) / STEPS.length) * 100);

  return (
    <main className="generating">
      <div className="generating__inner">
        <p className="kicker">Собираем итоги года</p>
        <h1 className="generating__title">Город строится</h1>

        <ol className="generating__steps" aria-live="polite">
          {STEPS.map((label, index) => (
            <li
              key={label}
              className={
                'generating__step' +
                (index < step ? ' generating__step--done' : '') +
                (index === step ? ' generating__step--active' : '')
              }
            >
              <span className="generating__marker" aria-hidden="true" />
              {label}
            </li>
          ))}
        </ol>

        <div className="generating__bar" aria-label={`Готово на ${Math.round(visibleProgress)}%`}>
          <div className="generating__fill" style={{ width: `${visibleProgress}%` }} />
        </div>
      </div>
    </main>
  );
}
