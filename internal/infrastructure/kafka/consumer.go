package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"order-processor-service/internal/domain"

	kgo "github.com/segmentio/kafka-go"
)

type OrderPersister interface {
	MarkProcessed(ctx context.Context, order domain.Order) error
}

type Consumer struct {
	reader *kgo.Reader
	repo   OrderPersister
}

func NewConsumer(brokers []string, topic, group string, repo OrderPersister) *Consumer {
	return &Consumer{
		reader: kgo.NewReader(kgo.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     group,
			StartOffset: kgo.FirstOffset,
		}),
		repo: repo,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("fetch: %v", err)
			}
			return
		}

		var order domain.Order
		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("bozuk mesaj p=%d o=%d: %v", message.Partition, message.Offset, err)
			_ = c.reader.CommitMessages(ctx, message)
			continue
		}

		if err := c.repo.MarkProcessed(ctx, order); err != nil {
			log.Printf("persist processed order: %v", err)
			_ = c.reader.CommitMessages(ctx, message)
			continue
		}

		log.Printf("İŞLENDİ p=%d o=%d id=%s amount=%.2f", message.Partition, message.Offset, order.ID, order.Amount)

		if err := c.reader.CommitMessages(ctx, message); err != nil {
			log.Printf("commit: %v", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
