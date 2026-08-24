package kafka

import (
	"context"
	"encoding/json"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kgo.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{writer: &kgo.Writer{
		Addr:         kgo.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kgo.Hash{},
		RequiredAcks: kgo.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}}
}

func (p *Producer) Publish(ctx context.Context, key string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kgo.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now(),
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
