package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Config struct {
	PollInterval time.Duration
	BatchSize    int
	Topic        string
}

type Engine struct {
	repo     Repository
	producer *kafka.Producer
	cfg      Config
	logger   *slog.Logger
}

// NewEngine initializes the shared outbox engine.
func NewEngine(repo Repository, kafkaBrokers string, cfg Config, logger *slog.Logger) (*Engine, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"acks":              "all",
	})
	if err != nil {
		return nil, err
	}

	logger = logger.With("name", "[outbox]")

	return &Engine{
		repo:     repo,
		producer: p,
		cfg:      cfg,
		logger:   logger,
	}, nil
}

// Start spawns the background worker loops.
func (e *Engine) Start(ctx context.Context) {
	// Loop 1: Listen to Kafka Delivery Reports
	go e.listenToDeliveryReports(ctx)

	// Loop 2: Poll Database Outbox
	go e.pollLoop(ctx)
}

func (e *Engine) listenToDeliveryReports(ctx context.Context) {
	log := e.logger
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-e.producer.Events():
			if !ok {
				return
			}
			if ev, ok := event.(*kafka.Message); ok {
				recordID := ev.Opaque.(int64)

				if ev.TopicPartition.Error != nil {
					log.Error("Kafka delivery failed for", "record", recordID, "error", ev.TopicPartition.Error)
					if err := e.repo.MarkAsFailed(ctx, recordID, ev.TopicPartition.Error.Error()); err != nil {
						log.Error("Failed to mark record failed as in db.", "error", err.Error())
					}

				} else {
					if err := e.repo.MarkAsProcessed(ctx, recordID); err != nil {
						log.Error("Failed to mark record as successful in db.", "error", err.Error())

					}
				}
			}
		}
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.producer.Flush(15 * 1000)
			e.producer.Close()
			return
		case <-ticker.C:
			records, err := e.repo.FetchPending(ctx, e.cfg.BatchSize)
			if err != nil || len(records) == 0 {
				continue
			}

			var ids []int64
			for _, r := range records {
				ids = append(ids, r.ID)
			}

			if err := e.repo.MarkAsSending(ctx, ids); err != nil {
				continue
			}

			for _, record := range records {
				err := e.producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &e.cfg.Topic, Partition: kafka.PartitionAny},
					Value:          record.Payload,
					Key:            []byte(record.EventName),
					Opaque:         record.ID,
				}, nil)

				if err != nil {
					_ = e.repo.MarkAsFailed(ctx, record.ID, err.Error())
				}
			}
		}
	}
}
