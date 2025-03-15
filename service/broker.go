package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type queue struct {
	messages chan []byte

	mu          *sync.RWMutex
	subscribers []string
}

type Broker struct {
	queues map[string]*queue
	cfg    *BrokerConfig
	log    *slog.Logger
}

type BrokerConfig struct {
	QueueNames      []string
	QueueLength     int
	MaxSubscribers  int
	CallbackTimeout time.Duration
}

func NewBroker(ctx context.Context, cfg *BrokerConfig, log *slog.Logger) *Broker {
	var res Broker
	res.log = log
	res.cfg = cfg
	res.queues = make(map[string]*queue, len(cfg.QueueNames))

	for _, name := range cfg.QueueNames {
		res.queues[name] = &queue{
			mu:          &sync.RWMutex{},
			messages:    make(chan []byte, cfg.QueueLength),
			subscribers: make([]string, 0, cfg.MaxSubscribers),
		}
	}

	go res.distribute(ctx)

	return &res
}

func (b *Broker) Push(ctx context.Context, queueName string, message []byte) error {
	queue, ok := b.queues[queueName]
	if !ok {
		b.log.Warn("invalid queue name", slog.String("name", queueName), slog.Any("expected", b.cfg.QueueNames))
		return fmt.Errorf("queue name must be one of %v", b.cfg.QueueNames)
	}

	select {
	case queue.messages <- message:
		return nil
	default:
		b.log.Warn("queue overflow", slog.Int("queue_length", b.cfg.QueueLength))
		return errors.New("queue overflow")
	}
}

func (b *Broker) Subscribe(ctx context.Context, queueName string, callback string) error {
	queue, ok := b.queues[queueName]
	if !ok {
		b.log.Warn("invalid queue name", slog.String("name", queueName), slog.Any("expected", b.cfg.QueueNames))
		return fmt.Errorf("queue name must be one of %v", b.cfg.QueueNames)
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.subscribers) == b.cfg.QueueLength {
		b.log.Warn("subscribers overflow", slog.Int("max_subscribers", b.cfg.MaxSubscribers))
		return errors.New("subscribers overflow")
	}
	queue.subscribers = append(queue.subscribers, callback)

	return nil
}

func (b *Broker) withCancel(cancel context.CancelFunc, f func()) {
	defer cancel()
	f()
}

func (b *Broker) send(ctx context.Context, queue *queue, message []byte) {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	for _, subscriber := range queue.subscribers {
		ctx, cancel := context.WithTimeout(ctx, b.cfg.CallbackTimeout)
		b.withCancel(cancel, func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, subscriber, bytes.NewReader(message))
			if err != nil {
				b.log.Error("failed to create request", slog.Any("error", err))
				return
			}

			if _, err = http.DefaultClient.Do(req); err != nil {
				b.log.Error("failed to send message", slog.Any("error", err))
			}
		})
	}
}

func (b *Broker) distribute(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			b.log.Error("recovered from panic", slog.Any("panic", r))
		}
		go b.distribute(ctx)
	}()

	for {
		for name, q := range b.queues {
			select {
			case m, ok := <-q.messages:
				if !ok {
					b.log.Error("queue closed", slog.String("name", name))
					continue
				}

				go b.send(ctx, q, m)
			default:
			}
		}
	}
}
