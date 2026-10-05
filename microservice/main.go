package microservice

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/niteshswarnakar/transactional-outbox/package/models"
)

func MicroServer(ctx context.Context) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:29092"
	}
	topic := "order-service"
	group := "microservice-group"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           group,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	db := initDB()
	defer db.Close()

	if err := consumer.SubscribeTopics([]string{"order-service"}, nil); err != nil {
		log.Fatalf("failed to subscribe to topic: %v", err)
	}

	fmt.Println("Microservice started, consuming from topic:", topic)

	run := true
	for run {
		select {
		case sig := <-ctx.Done():
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Printf("consumer error: %v", err)
				continue
			}
			for {
				err := handleMessage(msg, db)
				if err == nil {
					break
				}
				log.Printf("failed to handle message, retrying: %v", err)
				time.Sleep(time.Second)
			}
			if _, err := consumer.CommitMessage(msg); err != nil {
				log.Printf("failed to commit offset: %v", err)
			}
		}
	}
}

func handleMessage(msg *kafka.Message, db *gorm.DB) error {
	var order models.KafkaOrder
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		log.Printf("skipping unparseable message at %v: %v", msg.TopicPartition, err)
		return nil
	}

	err := db.Set("gorm:insert_option", "ON CONFLICT (id) DO NOTHING").Create(&order).Error
	if err != nil {
		return fmt.Errorf("insert order %d: %w", order.ID, err)
	}

	fmt.Printf("consumed order: id=%d name=%s price=%.2f\n", order.ID, order.Name, order.Price)
	return nil
}

// since it is microservice, it will have its own database connection and will not share the same connection with gateway service
func initDB() *gorm.DB {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("DB_PASSWORD")
	if password == "" {
		password = "postgres"
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "postgres"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
	db, err := gorm.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	return db
}
