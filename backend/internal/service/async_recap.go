package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/eventing"
	"recap-personalization/internal/model"
	recap "recap-personalization/internal/recap"
	"recap-personalization/internal/repository"
)

type RecapResult struct {
	Recap   *model.Recap
	Request *model.RecapRequestStatusResponse
}

func (s *Service) RequestRecap(ctx context.Context, profileID string, year int) (*RecapResult, bool, error) {
	existing, err := s.recaps.GetRecapByProfileAndYear(ctx, profileID, year)
	if err == nil {
		return &RecapResult{Recap: existing}, false, nil
	}
	if !errors.Is(err, repository.ErrRecapNotFound) {
		return nil, false, err
	}

	profile, err := s.profiles.GetProfileByID(ctx, profileID)
	if err != nil {
		return nil, false, err
	}
	if !containsYear(profile.AvailableYears, year) {
		return nil, false, ErrYearNotAvailable
	}

	parsedProfileID, err := uuid.Parse(profileID)
	if err != nil {
		return nil, false, fmt.Errorf("parse profile id: %w", err)
	}
	now := time.Now().UTC()
	request := &model.RecapRequest{
		ID:               uuid.New(),
		ProfileID:        parsedProfileID,
		Year:             year,
		Status:           model.RecapRequestQueued,
		Stage:            "queued",
		AlgorithmVersion: recap.AlgorithmVersion,
		IdempotencyKey:   fmt.Sprintf("%s:%d:%s", profileID, year, recap.AlgorithmVersion),
		MaxAttempts:      s.workerMaxAttempts,
		AvailableAt:      now,
	}
	stored, created, err := s.recapRequests.CreateOrGetRecapRequest(ctx, request)
	if err != nil {
		return nil, false, err
	}
	if !created && stored.Status == model.RecapRequestFailed && stored.Retryable {
		restarted, restartErr := s.recapRequests.RestartFailedRecapRequest(ctx, stored.ID.String())
		if restartErr == nil {
			stored = restarted
			created = true
		} else if errors.Is(restartErr, repository.ErrRecapRequestNotFound) {
			refreshed, readErr := s.recapRequests.GetRecapRequestByID(ctx, stored.ID.String())
			if readErr != nil {
				return nil, false, readErr
			}
			stored = refreshed
		} else {
			return nil, false, restartErr
		}
	}
	return &RecapResult{
		Request: model.NewRecapRequestStatusResponse(stored, s.recapPollAfter),
	}, created, nil
}

func (s *Service) GetRecapResult(ctx context.Context, id string) (*RecapResult, error) {
	value, err := s.recaps.GetRecapByID(ctx, id)
	if err == nil {
		return &RecapResult{Recap: value}, nil
	}
	if !errors.Is(err, repository.ErrRecapNotFound) {
		return nil, err
	}

	request, requestErr := s.recapRequests.GetRecapRequestByID(ctx, id)
	if requestErr != nil {
		if errors.Is(requestErr, repository.ErrRecapRequestNotFound) {
			return nil, repository.ErrRecapNotFound
		}
		return nil, requestErr
	}
	if request.Status == model.RecapRequestReady {
		return nil, fmt.Errorf("ready recap request %s has no snapshot", request.ID)
	}
	return &RecapResult{
		Request: model.NewRecapRequestStatusResponse(request, s.recapPollAfter),
	}, nil
}

func (s *Service) ProcessRecapRequest(
	ctx context.Context,
	request *model.RecapRequest,
	workerID string,
) error {
	report := func(stage string, progress int) error {
		if err := s.recapRequests.UpdateRecapRequestProgress(
			ctx,
			request.ID.String(),
			workerID,
			stage,
			progress,
		); err != nil {
			return err
		}
		if s.lifecyclePublisher != nil {
			event := eventing.NewLifecycle(
				request.ID,
				model.RecapRequestProcessing,
				stage,
				progress,
				request.AttemptCount,
			)
			if err := s.lifecyclePublisher.PublishLifecycle(ctx, event); err != nil {
				log.Printf("publish lifecycle progress for %s: %v", request.ID, err)
			}
		}
		return nil
	}

	if err := report("loading_activity", 15); err != nil {
		return err
	}
	value, err := s.generateRecapValue(
		ctx,
		request.ID.String(),
		request.ProfileID.String(),
		request.Year,
		report,
	)
	if err != nil {
		return err
	}
	if err := report("persisting_snapshot", 90); err != nil {
		return err
	}
	return s.recaps.FinalizeRecapRequest(
		ctx,
		request.ID.String(),
		workerID,
		value,
	)
}
