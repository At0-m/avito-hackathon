import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { CityCanvas } from '@/entities/city/CityCanvas';
import { cacheRecap, getCachedRecap } from '@/shared/api/cache';
import { loadExplanation, loadRecap } from '@/shared/api/recap';
import { describeFailure, type FailureView } from '@/shared/api/errors';
import { ShareCardModal } from '@/widgets/share-card/ShareCardModal';
import { DISTRICTS, pluralize } from '@/shared/lib/plural';
import { LEVEL_TONE } from '@/shared/lib/palette';
import { BadgesPanel } from './components/BadgesPanel';
import { DistrictLegend } from './components/DistrictLegend';
import { TraitsPanel } from './components/TraitsPanel';
import { UnfinishedPanel } from './components/UnfinishedPanel';
import type { DistrictId, Recap } from '@/shared/types/recap';
import './RecapPage.css';

export function RecapPage() {
  const { recapId = '' } = useParams();
  const navigate = useNavigate();

  const [recap, setRecap] = useState<Recap | null>(() => getCachedRecap(recapId) ?? null);
  const [failure, setFailure] = useState<FailureView | null>(null);
  const [step, setStep] = useState(0);
  const [focus, setFocus] = useState<DistrictId | null>(null);
  const [shareOpen, setShareOpen] = useState(false);
  const explained = useRef<string | null>(null);

  useEffect(() => {
    if (recap) return;
    let active = true;
    const controller = new AbortController();

    void loadRecap(recapId, controller.signal)
      .then((value) => {
        if (!active) return;
        cacheRecap(value);
        setRecap(value);
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
  }, [recap, recapId]);

  const total = recap?.chapters.length ?? 0;
  const isFinal = total > 0 && step === total - 1;

  const next = useCallback(() => setStep((v) => Math.min(v + 1, total - 1)), [total]);
  const back = useCallback(() => setStep((v) => Math.max(v - 1, 0)), []);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'ArrowRight' || event.key === ' ') {
        event.preventDefault();
        next();
      }
      if (event.key === 'ArrowLeft') back();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [back, next]);

  useEffect(() => {
    if (!recap || !recap.capabilities.explanationAvailable) return;
    if (explained.current === recap.recapId) return;
    explained.current = recap.recapId;

    loadExplanation(recap)
      .then((value) => {
        cacheRecap(value);
        setRecap(value);
      })
      .catch(() => undefined);
  }, [recap]);

  const revealed = useMemo(() => {
    if (!recap) return new Set<DistrictId>();
    const districtStep = recap.chapters.findIndex((item) => item.kind === 'district');
    if (isFinal || (districtStep >= 0 && step >= districtStep)) {
      return new Set(recap.districts.map((d) => d.id));
    }
    const ids = recap.chapters
      .slice(0, step + 1)
      .map((chapter) => chapter.districtId)
      .filter((id): id is DistrictId => Boolean(id));
    return new Set(ids);
  }, [isFinal, recap, step]);

  if (failure) {
    return (
      <main className="recap recap--message">
        <div>
          <p className="kicker">Итоги недоступны</p>
          <h1 className="recap__chapter-title">{failure.title}</h1>
          {failure.hint && <p className="recap__narrative">{failure.hint}</p>}
          <div className="recap__controls">
            <button type="button" className="btn btn--primary" onClick={() => void navigate('/')}>
              К выбору профиля
            </button>
          </div>
        </div>
      </main>
    );
  }

  if (!recap) {
    return (
      <main className="recap recap--message">
        <p className="kicker">Загружаем итоги…</p>
      </main>
    );
  }

  const chapter = recap.chapters[step];
  const hasReasons =
    recap.capabilities.explanationAvailable &&
    Boolean(recap.role.reason ?? recap.style.reason);

  return (
    <main className="recap">
      <header className="recap__header">
        <button type="button" className="recap__logo" onClick={() => void navigate('/')}>
          Итоги года · {recap.year}
        </button>

        <ol className="recap__progress" aria-label={`Глава ${step + 1} из ${total}`}>
          {recap.chapters.map((item) => (
            <li
              key={item.index}
              className={'recap__tick' + (item.index <= step + 1 ? ' recap__tick--done' : '')}
            />
          ))}
        </ol>

        <div className="recap__header-actions">
          {}
          {isFinal && recap.capabilities.shareAvailable && (
            <button type="button" className="btn btn--ghost" onClick={() => setShareOpen(true)}>
              Поделиться городом
            </button>
          )}
        </div>
      </header>

      <section className="recap__stage">
        <div className="recap__intro">
          <p className="kicker">
            Глава {step + 1} из {total}
            {isFinal ? ' · Город собран' : ''}
          </p>

          {isFinal ? (
            <>
              <h1 className="recap__city-name">{recap.cityName}</h1>
              <p className="recap__totals">
                {recap.totals.activeDays !== undefined &&
                  `${recap.totals.activeDays} активных дней · `}
                {pluralize(recap.totals.districts, DISTRICTS)}
              </p>
              {}
              {recap.summaryText && <p className="recap__summary">{recap.summaryText}</p>}
            </>
          ) : chapter.kind === 'archetype' ? (
            <div className="archetype">
              {chapter.eyebrow && <p className="archetype__eyebrow">{chapter.eyebrow}</p>}
              <h1 className="recap__chapter-title">{recap.role.title}</h1>

              <p className="archetype__style">
                <span className="archetype__style-label">Стиль</span>
                <span className="archetype__style-value">{recap.style.title}</span>
              </p>

              {hasReasons && (
                <details className="archetype__why">
                  <summary className="archetype__why-toggle">Почему так?</summary>
                  <div className="archetype__why-content">
                    {recap.role.reason && <p className="recap__reason">{recap.role.reason}</p>}
                    {recap.style.reason && <p className="recap__reason">{recap.style.reason}</p>}
                  </div>
                </details>
              )}
            </div>
          ) : (
            <>
              <h1 className="recap__chapter-title">{chapter.title}</h1>
              {chapter.stat && (
                <p className="recap__stat">
                  <span className="recap__stat-value">{chapter.stat.value}</span>
                  <span className="recap__stat-label">{chapter.stat.label}</span>
                </p>
              )}
              {chapter.narrative && (
                <p
                  className={
                    chapter.kind === 'summary'
                      ? 'recap__narrative recap__summary'
                      : 'recap__narrative'
                  }
                >
                  {chapter.narrative}
                </p>
              )}

              {}
              {chapter.kind === 'achievements' && recap.badges.length > 0 && (
                <ul className="recap__awards">
                  {recap.badges.map((badge) => (
                    <li key={badge.id} className="recap__award">
                      <span
                        className="recap__award-dot"
                        style={{ background: LEVEL_TONE[badge.group] }}
                        aria-hidden="true"
                      />
                      <span className="recap__award-title">{badge.title}</span>
                      <span className="recap__award-level">{badge.groupTitle}</span>
                    </li>
                  ))}
                </ul>
              )}
            </>
          )}
        </div>

        <CityCanvas
          recap={recap}
          revealed={revealed}
          focus={isFinal ? focus : (chapter.districtId ?? null)}
          showSites={false}
          interactive={isFinal}
          onSelectDistrict={(id) => setFocus((current) => (current === id ? null : id))}
        />

        {!isFinal && (
          <div className="recap__controls">
            <button type="button" className="btn btn--ghost" onClick={back} disabled={step === 0}>
              Назад
            </button>
            <button type="button" className="btn btn--primary" onClick={next}>
              Дальше →
            </button>
          </div>
        )}
      </section>

      {isFinal && (
        <section className="recap__final">
          <DistrictLegend
            districts={recap.districts}
            revealed={revealed}
            active={focus}
            onSelect={(id) => setFocus((current) => (current === id ? null : id))}
          />

          <div className="recap__panels">
            <TraitsPanel role={recap.role} style={recap.style} />
            <BadgesPanel badges={recap.badges} />
            {}
            {recap.unfinished.length > 0 && <UnfinishedPanel items={recap.unfinished} />}
          </div>

          <footer className="recap__footer">
            <p className="recap__privacy">
              В городе нет переписок, цен и данных других людей — только форма твоего года.
            </p>
            <p className="recap__year-note">Итоги собраны по твоей активности за {recap.year} год.</p>
          </footer>
        </section>
      )}

      {shareOpen && <ShareCardModal recapId={recap.recapId} onClose={() => setShareOpen(false)} />}
    </main>
  );
}
