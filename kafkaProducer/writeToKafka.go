package kafkaProducer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

func WriteToKafka(ctx context.Context, kafkaConn *kafka.Writer, topic string, key, value []byte) error {
	return kafkaConn.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}
