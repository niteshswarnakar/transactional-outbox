package database

import (
	"fmt"
	"os"

	"github.com/jinzhu/gorm"
	"github.com/niteshswarnakar/transactional-outbox/package/models"
)

func GetDBString() string {
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
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
}

type Database struct {
	DB *gorm.DB
}

func InitPostgresDB() *Database {
	dbString := GetDBString()
	db, err := gorm.Open("postgres", dbString)
	if err != nil {
		panic(err)
	}

	db.AutoMigrate(&models.KafkaOrder{})
	db.AutoMigrate(&models.Order{})
	return &Database{
		DB: db,
	}
}
