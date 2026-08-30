package handler

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"recap-personalization/internal/service"
)

type Handler struct {
	service              *service.Service
	ssePollInterval      time.Duration
	sseHeartbeatInterval time.Duration
}

func NewHandler(service *service.Service) *Handler {
	return &Handler{
		service:              service,
		ssePollInterval:      250 * time.Millisecond,
		sseHeartbeatInterval: 10 * time.Second,
	}
}

func (h *Handler) ConfigureSSE(pollInterval, heartbeatInterval time.Duration) {
	if pollInterval > 0 {
		h.ssePollInterval = pollInterval
	}
	if heartbeatInterval > 0 {
		h.sseHeartbeatInterval = heartbeatInterval
	}
}

func (h *Handler) RegisterRoutes(router *gin.Engine) {
	api := router.Group("/api/v1")
	{
		api.GET("/profiles", h.GetProfiles)
		api.GET("/profiles/:id", h.GetProfileByID)

		api.POST("/recaps", h.CreateRecap)
		api.GET("/recaps/:id", h.GetRecap)
		api.GET("/recaps/:id/stream", h.StreamRecap)
		api.GET("/recaps/:id/explanation", h.GetRecapExplanation)
		api.GET("/recaps/:id/share", h.GetShareCard)

		api.POST("/recaps/:id/interactions", h.SaveInteraction)
	}
}

func generateRequestID() string {
	return uuid.New().String()
}
