package models

type Order struct {
	ID    int32   `json:"id" gorm:"primaryKey;autoIncrement"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}
