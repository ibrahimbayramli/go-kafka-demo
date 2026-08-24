package domain

import "time"

type OrderStatus string

const (
	OrderStatusQueued    OrderStatus = "queued"
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusProcessed OrderStatus = "processed"
)

func (s OrderStatus) IsValid() bool {
	switch s {
	case OrderStatusQueued, OrderStatusPending, OrderStatusProcessed:
		return true
	default:
		return false
	}
}

type Order struct {
	ID       string      `json:"id"`
	Customer string      `json:"customer"`
	Amount   float64     `json:"amount"`
	Created  time.Time   `json:"created"`
	Status   OrderStatus `json:"status"`
}

type CreateOrderRequest struct {
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
}
