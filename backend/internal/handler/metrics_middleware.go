package handler

import (
	"time"

	"github.com/gin-gonic/gin"
	"recap-personalization/internal/telemetry"
)

// MetricsMiddleware records request counts and durations labelled by the
// route template (for example /api/v1/recaps/:id), never by the raw URL.
func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		method := c.Request.Method
		telemetry.HTTPRequests.Inc(route, method, telemetry.StatusClass(c.Writer.Status()))
		telemetry.HTTPDuration.Observe(time.Since(started).Seconds(), route, method)
	}
}
