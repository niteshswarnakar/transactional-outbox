package microservice

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/niteshswarnakar/transactional-outbox/package/models"
)

func MicroServer() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topic := "order-service"
	group := "microservice-group"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           group,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
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

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Microservice started, consuming from topic:", topic)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Printf("consumer error: %v", err)
				continue
			}
			handleMessage(msg, db)
		}
	}
}

func handleMessage(msg *kafka.Message, db *gorm.DB) {
	var order models.KafkaOrder
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		log.Printf("failed to unmarshal message: %v", err)
		return
	}

	if err := db.Create(&order).Error; err != nil {
		log.Printf("failed to insert order: %v", err)
		return
	}

	fmt.Printf("consumed order: id=%d name=%s price=%.2f\n", order.ID, order.Name, order.Price)
}

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
