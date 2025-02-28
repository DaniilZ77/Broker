package handlers

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/httplog/v2"
)

type router struct {
	chi.Router
	broker Broker
}

type Broker interface {
	Push(ctx context.Context, queueName string, message []byte) error
	Subscribe(ctx context.Context, queueName string, callback string) error
}

func NewRouter(r chi.Router, broker Broker) {
	router := router{r, broker}

	r.Route("/queues/{queueName}", func(r chi.Router) {
		r.Post("/messages", router.messages)
		r.Post("/subscriptions", router.subscriptions)
	})
}

func (router *router) errorResponse(w http.ResponseWriter, err error, code int, log *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(struct {
		Message string `json:"message"`
	}{Message: err.Error()}); err != nil {
		log.Error(err.Error())
	}
}

func (router *router) messages(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "queueName")

	log := httplog.LogEntry(r.Context())

	message, err := io.ReadAll(r.Body)
	if err != nil {
		router.errorResponse(w, err, http.StatusInternalServerError, log)
		return
	}

	if err = router.broker.Push(r.Context(), queueName, message); err != nil {
		router.errorResponse(w, err, http.StatusBadRequest, log)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (router *router) subscriptions(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "queueName")
	callback := r.PostFormValue("callback")

	if err := router.broker.Subscribe(r.Context(), queueName, callback); err != nil {
		router.errorResponse(w, err, http.StatusBadRequest, httplog.LogEntry(r.Context()))
		return
	}

	w.WriteHeader(http.StatusOK)
}
