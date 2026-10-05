package outbox

import (
	"context"
	"log"
	"time"

	"github.com/jinzhu/gorm"
	broker "github.com/niteshswarnakar/transactional-outbox/package/kafka"
	"github.com/niteshswarnakar/transactional-outbox/package/models"
)

const batchSize = 100

// Worker Thread polls the outbox table and publishes pending rows to Kafka.
type WorkerThread struct {
	db       *gorm.DB
	kc       *broker.KafkaClient
	interval time.Duration
}

func NewWorkerThread(db *gorm.DB, kc *broker.KafkaClient, interval time.Duration) *WorkerThread {
	return &WorkerThread{db: db, kc: kc, interval: interval}
}

// Run keeps polling until the gateway server cancels
func (self *WorkerThread) Run(ctx context.Context) {
	ticker := time.NewTicker(self.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := self.publishPending(); err != nil {
				log.Printf("outbox relay: %v", err)
			}
		}
	}
}

func (self *WorkerThread) publishPending() error {
	tx := self.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer tx.Rollback()

	var rows []models.Outbox
	err := tx.Set("gorm:query_option", "FOR UPDATE SKIP LOCKED").
		Where("published_at IS NULL").
		Order("id").
		Limit(batchSize).
		Find(&rows).Error
	if err != nil {
		return err
	}

	now := time.Now()
	for _, row := range rows {
		if err := self.kc.Publish(row.Topic, []byte(row.Payload)); err != nil {
			log.Printf("outbox relay: publish id=%d failed: %v", row.ID, err)
			break
		}
		if err := tx.Model(&models.Outbox{}).Where("id = ?", row.ID).Update("published_at", now).Error; err != nil {
			return err
		}
	}

	return tx.Commit().Error
}
