package main

import (
	"log"
	"os"

	"github.com/labstack/echo/v4"
	"github.com/niteshswarnakar/transactional-outbox/microservice"
	"github.com/niteshswarnakar/transactional-outbox/package/database"
	broker "github.com/niteshswarnakar/transactional-outbox/package/kafka"
	"github.com/niteshswarnakar/transactional-outbox/package/models"
)

const orderTopic = "order-service"

func main() {
	e := echo.New()
	db := database.InitPostgresDB()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:29092"
	}
	kc, err := broker.InitKafkaProducer(brokers)
	if err != nil {
		log.Fatalf("failed to init kafka producer: %v", err)
	}
	defer kc.Close()

	go microservice.MicroServer()

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "healthy"})
	})

	e.POST("/orders", func(c echo.Context) error {
		return Handler(c, db, kc)
	})
	e.Logger.Fatal(e.Start(":8080"))
}

func Handler(c echo.Context, db *database.Database, kc *broker.KafkaClient) error {
	var o orderRequest
	if err := c.Bind(&o); err != nil {
		return c.JSON(400, map[string]string{"error": err.Error()})
	}

	// Create the order model
	order := models.Order{
		Name:  o.Name,
		Price: o.Price,
	}

	// Save the order to the database
	if err := db.DB.Create(&order).Error; err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}

	if err := kc.PublishOrder(orderTopic, &order); err != nil {
		log.Printf("failed to publish order %d: %v", order.ID, err)
	}

	return c.JSON(201, order)
}

type orderRequest struct {
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}
