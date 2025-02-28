package main

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/DaniilZ77/broker/config"
	"github.com/DaniilZ77/broker/handlers"
	"github.com/DaniilZ77/broker/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httplog/v2"
)

const (
	envLocal = "local"
	envProd  = "prod"
)

func main() {
	ctx := context.Background()

	cfg := config.ReadConfig()
	logOpts := httplog.Options{
		MessageFieldName: "message",
		Tags: map[string]string{
			"env": cfg.Env,
		},
		SourceFieldName: "source",
	}

	if cfg.Env == envProd {
		logOpts.JSON = true
		logOpts.LogLevel = slog.LevelInfo
	}

	log := httplog.NewLogger("broker", logOpts)

	log.Info("config of app", slog.Group("config",
		slog.String("env", cfg.Env),
		slog.String("broker_port", cfg.BrokerPort),
		slog.Any("queue_names", cfg.QueueNames),
		slog.Int("queue_length", cfg.QueueLength),
		slog.Int("max_subscribers", cfg.MaxSubscribers),
		slog.Duration("callback_timeout", cfg.CallbackTimeout),
	))

	r := chi.NewRouter()
	r.Use(httplog.RequestLogger(log))
	r.Use(middleware.Heartbeat("/ping"))
	r.Use(middleware.Recoverer)
	r.Mount("/debug", middleware.Profiler())

	r.Route("/v1", func(r chi.Router) {
		handlers.NewRouter(r, service.NewBroker(ctx, &service.BrokerConfig{
			QueueNames:      cfg.QueueNames,
			QueueLength:     cfg.QueueLength,
			MaxSubscribers:  cfg.MaxSubscribers,
			CallbackTimeout: cfg.CallbackTimeout,
		}, log.Logger))
	})

	if err := http.ListenAndServe(cfg.BrokerPort, r); err != nil {
		panic(err)
	}
}
