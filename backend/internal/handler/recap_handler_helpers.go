package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"recap-personalization/internal/model"
	recap "recap-personalization/internal/recap"
	"recap-personalization/internal/repository"
	"recap-personalization/internal/service"
)

func bindCreateRecapRequest(c *gin.Context) (*model.CreateRecapRequest, bool) {
	var request model.CreateRecapRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeAPIError(
			c,
			http.StatusBadRequest,
			model.ErrCodeInvalidArgument,
			"Invalid request body",
			model.ErrorDetail{Field: "body", Reason: err.Error()},
		)
		return nil, false
	}
	if request.Year < 2000 || request.Year > 2100 {
		writeAPIError(
			c,
			http.StatusBadRequest,
			model.ErrCodeInvalidArgument,
			"Year must be between 2000 and 2100",
			model.ErrorDetail{Field: "year", Reason: "out_of_range"},
		)
		return nil, false
	}
	return &request, true
}

func (h *Handler) loadReadyRecap(c *gin.Context, failureMessage string) (*model.Recap, bool) {
	value, err := h.service.GetRecap(c.Request.Context(), c.Param("id"))
	if errors.Is(err, repository.ErrRecapNotFound) {
		writeAPIError(c, http.StatusNotFound, model.ErrCodeRecapNotFound, "Recap not found")
		return nil, false
	}
	if err != nil {
		writeAPIError(
			c,
			http.StatusInternalServerError,
			model.ErrCodeInternalError,
			failureMessage,
			model.ErrorDetail{Reason: err.Error()},
		)
		return nil, false
	}
	return value, true
}

func (h *Handler) writeRecapGenerationError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repository.ErrProfileNotFound):
		writeAPIError(c, http.StatusNotFound, model.ErrCodeProfileNotFound, "Profile not found")
	case errors.Is(err, service.ErrYearNotAvailable):
		writeAPIError(
			c,
			http.StatusBadRequest,
			model.ErrCodeInvalidArgument,
			"The selected year is not available for this profile",
			model.ErrorDetail{Field: "year", Reason: "not_available"},
		)
	case errors.Is(err, recap.ErrInsufficientActivity):
		writeAPIError(
			c,
			http.StatusUnprocessableEntity,
			model.ErrCodeInsufficientActivity,
			"Not enough activity data for the selected year",
		)
	case errors.Is(err, service.ErrActivitySourceMissing), errors.Is(err, service.ErrActivitySourceUnavailable):
		writeAPIError(
			c,
			http.StatusServiceUnavailable,
			model.ErrCodeDependencyUnavailable,
			"Activity source is unavailable",
		)
	default:
		writeAPIError(
			c,
			http.StatusInternalServerError,
			model.ErrCodeInternalError,
			"Failed to generate recap",
			model.ErrorDetail{Reason: err.Error()},
		)
	}
}
