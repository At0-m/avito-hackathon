package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"recap-personalization/internal/config"
	"recap-personalization/internal/handler"
	"recap-personalization/internal/recap/narrative"
	"recap-personalization/internal/recap/pipeline"
	"recap-personalization/internal/repository"
	"recap-personalization/internal/service"
	"recap-personalization/pkg/database"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	postgres, err := database.NewPostgresDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer postgres.Close()

	clickHouse := database.NewClickHouseHTTP(
		cfg.ClickHouseURL,
		cfg.ClickHouseDB,
		cfg.ClickHouseUser,
		cfg.ClickHousePass,
	)
	pingContext, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := clickHouse.Ping(pingContext); err != nil {
		cancelPing()
		log.Fatal(err)
	}
	cancelPing()

	var provider narrative.Provider
	if cfg.MistralAPIKey != "" {
		provider = narrative.MistralHTTPProvider{
			APIKey:   cfg.MistralAPIKey,
			Model:    cfg.MistralModel,
			Endpoint: cfg.MistralEndpoint,
			Timeout:  cfg.MistralTimeout,
		}
	}
	generator := pipeline.NewGenerator(provider)
	for _, host := range cfg.PublicAvatarHosts {
		generator.Registry.PublicAvatarHosts[host] = struct{}{}
	}

	postgresRepository := repository.NewRepository(postgres)
	clickHouseRepository := repository.NewClickHouseRepository(clickHouse)
	appService := service.NewService(
		postgresRepository,
		clickHouseRepository,
		clickHouseRepository,
		generator,
	)
	appService.ConfigureAsync(cfg.RecapPollAfter, cfg.WorkerMaxAttempts)
	appHandler := handler.NewHandler(appService)
	appHandler.ConfigureSSE(cfg.SSEPollInterval, cfg.SSEHeartbeatInterval)

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())
	appHandler.RegisterRoutes(router)

	server := &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("backend listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("server shutdown failed: %v", err)
	}
}
