package models

type Order struct {
	ID    int32   `json:"id" gorm:"primaryKey;autoIncrement;default:nextval('orders_id_seq')"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}
