package models

import "time"

// Outbox holds an event that must be published to Kafka. Euta worker go routine le pick garxa ra publish garxa.
type Outbox struct {
	ID          int64      `gorm:"primaryKey;autoIncrement"`
	Topic       string     `gorm:"not null"`
	Payload     string     `gorm:"type:text;not null"`
	CreatedAt   time.Time  `gorm:"not null;default:now()"`
	PublishedAt *time.Time `gorm:"index"`
}

func (Outbox) TableName() string {
	return "outbox"
}
