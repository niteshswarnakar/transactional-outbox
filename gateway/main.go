package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/niteshswarnakar/transactional-outbox/microservice"
	"github.com/niteshswarnakar/transactional-outbox/package/database"
	broker "github.com/niteshswarnakar/transactional-outbox/package/kafka"
	"github.com/niteshswarnakar/transactional-outbox/package/models"
	"github.com/niteshswarnakar/transactional-outbox/package/outbox"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go outbox.NewWorkerThread(db.DB, kc, time.Second).Run(ctx)

	e.Use(middleware.CORS())

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "healthy"})
	})

	e.POST("/orders", func(c echo.Context) error {
		return Handler(c, db)
	})

	// Orders written by the gateway.
	e.GET("/orders", func(c echo.Context) error {
		return listOrders(c, db)
	})

	// Orders stored by the Kafka consumer.
	e.GET("/kafka-orders", func(c echo.Context) error {
		return listKafkaOrders(c, db)
	})
	e.Logger.Fatal(e.Start(":8080"))
}

func listOrders(c echo.Context, db *database.Database) error {
	orders := []models.Order{}
	if err := db.DB.Order("id").Find(&orders).Error; err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}
	return c.JSON(200, orders)
}

func listKafkaOrders(c echo.Context, db *database.Database) error {
	orders := []models.KafkaOrder{}
	if err := db.DB.Order("id").Find(&orders).Error; err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}
	return c.JSON(200, orders)
}

func Handler(c echo.Context, db *database.Database) error {
	var o orderRequest
	if err := c.Bind(&o); err != nil {
		return c.JSON(400, map[string]string{"error": err.Error()})
	}

	order := models.Order{
		Name:  o.Name,
		Price: o.Price,
	}

	// MAIN ATOMICITY COMES HERE :) - Nitesh
	err := db.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&order).Error; err != nil {
			return err
		}
		payload, err := json.Marshal(order)
		if err != nil {
			return err
		}
		return tx.Create(&models.Outbox{
			Topic:   orderTopic,
			Payload: string(payload),
		}).Error
	})
	if err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}

	return c.JSON(201, order)
}

type orderRequest struct {
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}
