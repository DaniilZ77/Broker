package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"sync"
	"time"
)

type queue struct {
	mu          *sync.RWMutex
	messages    chan []byte
	subscribers []string
}

type Broker struct {
	sema   chan struct{}
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
	res.queues = make(map[string]*queue)
	res.sema = make(chan struct{}, runtime.GOMAXPROCS(-1))

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

func (b *Broker) send(ctx context.Context, queue *queue, message []byte) {
	b.sema <- struct{}{}
	defer func() { <-b.sema }()
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	for _, subscriber := range queue.subscribers {
		ctx, cancel := context.WithTimeout(ctx, b.cfg.CallbackTimeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, subscriber, bytes.NewReader(message))
		if err != nil {
			cancel()
			b.log.Error("failed to create request", slog.Any("error", err))
			continue
		}

		if _, err = http.DefaultClient.Do(req); err != nil {
			b.log.Error("failed to send message", slog.Any("error", err))
		}

		cancel()
	}
}

func (b *Broker) distribute(ctx context.Context) {
	for {
		for _, q := range b.queues {
			select {
			case m, ok := <-q.messages:
				if !ok {
					break
				}

				go b.send(ctx, q, m)
			default:
			}
		}
	}
}
