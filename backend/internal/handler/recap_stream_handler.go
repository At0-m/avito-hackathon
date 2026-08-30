package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"recap-personalization/internal/model"
	"recap-personalization/internal/repository"
	"recap-personalization/internal/service"
)

func (h *Handler) StreamRecap(c *gin.Context) {
	if _, ok := parseUUIDParam(c, "id", "Invalid recap id"); !ok {
		return
	}

	initial, err := h.service.GetRecapResult(c.Request.Context(), c.Param("id"))
	if errors.Is(err, repository.ErrRecapNotFound) {
		writeAPIError(c, http.StatusNotFound, model.ErrCodeRecapNotFound, "Recap not found")
		return
	}
	if err != nil {
		writeAPIError(c, http.StatusInternalServerError, model.ErrCodeInternalError, "Failed to get recap")
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache, no-transform")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	retryMS := h.ssePollInterval.Milliseconds()
	if retryMS < 250 {
		retryMS = 250
	}
	if _, err := fmt.Fprintf(c.Writer, "retry: %d\n\n", retryMS); err != nil {
		return
	}
	c.Writer.Flush()

	lastPayload, terminal, err := writeRecapEvent(c, initial)
	if err != nil || terminal {
		return
	}

	pollTicker := time.NewTicker(h.ssePollInterval)
	heartbeatTicker := time.NewTicker(h.sseHeartbeatInterval)
	defer pollTicker.Stop()
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-heartbeatTicker.C:
			if _, err := fmt.Fprint(c.Writer, ": keep-alive\n\n"); err != nil {
				return
			}
			c.Writer.Flush()
		case <-pollTicker.C:
			result, err := h.service.GetRecapResult(c.Request.Context(), c.Param("id"))
			if err != nil {
				return
			}
			payload, eventName, terminal, err := marshalRecapEvent(result)
			if err != nil {
				return
			}
			if string(payload) == lastPayload {
				if terminal {
					return
				}
				continue
			}
			if err := writeSSE(c, eventName, payload); err != nil {
				return
			}
			lastPayload = string(payload)
			if terminal {
				return
			}
		}
	}
}

func writeRecapEvent(c *gin.Context, result *service.RecapResult) (string, bool, error) {
	payload, eventName, terminal, err := marshalRecapEvent(result)
	if err != nil {
		return "", false, err
	}
	if err := writeSSE(c, eventName, payload); err != nil {
		return "", false, err
	}
	return string(payload), terminal, nil
}

func marshalRecapEvent(result *service.RecapResult) ([]byte, string, bool, error) {
	if result == nil {
		return nil, "", false, errors.New("empty recap result")
	}
	if result.Recap != nil {
		payload, err := json.Marshal(result.Recap)
		return payload, "ready", true, err
	}
	if result.Request == nil {
		return nil, "", false, errors.New("recap result has no payload")
	}
	payload, err := json.Marshal(result.Request)
	if err != nil {
		return nil, "", false, err
	}
	if result.Request.Status == model.RecapRequestFailed {
		return payload, "failed", true, nil
	}
	return payload, "status", false, nil
}

func writeSSE(c *gin.Context, eventName string, payload []byte) error {
	if _, err := fmt.Fprintf(c.Writer, "event: %s\n", eventName); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(c.Writer, "data: %s\n\n", payload); err != nil {
		return err
	}
	c.Writer.Flush()
	return nil
}
