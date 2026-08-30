package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"recap-personalization/internal/model"
	"recap-personalization/internal/repository"
)

func (h *Handler) CreateRecap(c *gin.Context) {
	request, ok := bindCreateRecapRequest(c)
	if !ok {
		return
	}

	result, _, err := h.service.RequestRecap(
		c.Request.Context(),
		request.ProfileID.String(),
		request.Year,
	)
	if err != nil {
		h.writeRecapGenerationError(c, err)
		return
	}
	if result.Recap != nil {
		c.Header("Location", "/api/v1/recaps/"+result.Recap.ID.String())
		c.JSON(http.StatusOK, result.Recap)
		return
	}

	setPollingHeaders(c, result.Request.Links.Self, result.Request.PollAfterMS)
	c.JSON(http.StatusAccepted, result.Request)
}

func (h *Handler) GetRecap(c *gin.Context) {
	if _, ok := parseUUIDParam(c, "id", "Invalid recap id"); !ok {
		return
	}

	result, err := h.service.GetRecapResult(c.Request.Context(), c.Param("id"))
	if errors.Is(err, repository.ErrRecapNotFound) {
		writeAPIError(c, http.StatusNotFound, model.ErrCodeRecapNotFound, "Recap not found")
		return
	}
	if err != nil {
		writeAPIError(
			c,
			http.StatusInternalServerError,
			model.ErrCodeInternalError,
			"Failed to get recap",
			model.ErrorDetail{Reason: err.Error()},
		)
		return
	}
	if result.Recap != nil {
		c.JSON(http.StatusOK, result.Recap)
		return
	}

	setPollingHeaders(c, result.Request.Links.Self, result.Request.PollAfterMS)
	c.JSON(http.StatusOK, result.Request)
}

func (h *Handler) GetRecapExplanation(c *gin.Context) {
	if _, ok := parseUUIDParam(c, "id", "Invalid recap id"); !ok {
		return
	}
	value, ok := h.loadReadyRecap(c, "Failed to get recap explanation")
	if !ok {
		return
	}
	if !value.Capabilities.ExplanationAvailable || value.Explanation == nil {
		writeAPIError(
			c,
			http.StatusConflict,
			model.ErrCodeExplanationNotAvailable,
			"Explanation not available for this recap",
		)
		return
	}
	c.JSON(http.StatusOK, value.Explanation)
}

func (h *Handler) GetShareCard(c *gin.Context) {
	if _, ok := parseUUIDParam(c, "id", "Invalid recap id"); !ok {
		return
	}
	value, ok := h.loadReadyRecap(c, "Failed to get share card")
	if !ok {
		return
	}
	if !value.Capabilities.ShareAvailable || value.Share == nil {
		writeAPIError(
			c,
			http.StatusConflict,
			model.ErrCodeShareNotAvailable,
			"Share card not available for this recap",
		)
		return
	}
	c.JSON(http.StatusOK, value.Share)
}
