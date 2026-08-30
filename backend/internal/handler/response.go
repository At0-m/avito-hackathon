package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"recap-personalization/internal/model"
)

func writeAPIError(
	c *gin.Context,
	status int,
	code, message string,
	details ...model.ErrorDetail,
) {
	value := model.APIError{
		Code:      code,
		Message:   message,
		RequestID: generateRequestID(),
	}
	if len(details) > 0 {
		value.Details = details
	}
	c.JSON(status, value)
}

func parseUUIDParam(c *gin.Context, name, message string) (uuid.UUID, bool) {
	value, err := uuid.Parse(c.Param(name))
	if err != nil {
		writeAPIError(
			c,
			http.StatusBadRequest,
			model.ErrCodeInvalidArgument,
			message,
			model.ErrorDetail{Field: name, Reason: "must_be_uuid"},
		)
		return uuid.Nil, false
	}
	return value, true
}

func setPollingHeaders(c *gin.Context, location string, pollAfterMS int) {
	c.Header("Location", location)
	seconds := (pollAfterMS + 999) / 1000
	if seconds < 1 {
		seconds = 1
	}
	c.Header("Retry-After", strconv.Itoa(seconds))
}
