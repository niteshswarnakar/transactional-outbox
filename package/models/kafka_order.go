package models

type KafkaOrder struct {
	ID    int32   `json:"id" gorm:"primaryKey;autoIncrement"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}
