package broker

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/niteshswarnakar/transactional-outbox/package/models"
)

type KafkaClient struct {
	Producer *kafka.Producer
}

func InitKafkaProducer(brokers string) (*KafkaClient, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"client.id":          "order-service",
		"message.timeout.ms": 10000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %v", err)
	}

	return &KafkaClient{
		Producer: p,
	}, nil
}

func (kc *KafkaClient) PublishOrder(topic string, order *models.Order) error {
	orderData, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %v", err)
	}

	return kc.Publish(topic, orderData)
}

// Publish sends raw bytes to a topic and waits for the broker's delivery ack.
func (kc *KafkaClient) Publish(topic string, value []byte) error {
	deliveryChan := make(chan kafka.Event, 1)

	err := kc.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
	}

	return nil
}

func (kc *KafkaClient) Close() {
	kc.Producer.Close()
}
