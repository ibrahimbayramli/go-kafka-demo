package controllers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"order-processor-service/internal/domain"

	"github.com/google/uuid"
)

type Publisher interface {
	Publish(ctx context.Context, key string, value any) error
}

type Repository interface {
	SaveOrder(ctx context.Context, order domain.Order) error
	ListOrders(ctx context.Context) ([]domain.Order, error)
}

type OrderController struct {
	publisher Publisher
	repo      Repository
}

func NewOrderController(publisher Publisher, repo Repository) *OrderController {
	return &OrderController{publisher: publisher, repo: repo}
}

func (c *OrderController) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateOrderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "geçersiz JSON"})
		return
	}
	if req.Customer == "" || req.Amount <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "customer ve amount zorunlu"})
		return
	}

	order := domain.Order{
		ID:       uuid.NewString(),
		Customer: req.Customer,
		Amount:   req.Amount,
		Created:  time.Now().UTC(),
		Status:   domain.OrderStatusQueued,
	}

	if err := c.repo.SaveOrder(r.Context(), order); err != nil {
		log.Printf("save order: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "veritabanına kaydedilemedi"})
		return
	}

	if err := c.publisher.Publish(r.Context(), order.Customer, order); err != nil {
		log.Printf("publish: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "kuyruğa yazılamadı"})
		return
	}

	writeJSON(w, http.StatusAccepted, order)
}

func (c *OrderController) ListOrders(w http.ResponseWriter, r *http.Request) {
	orders, err := c.repo.ListOrders(r.Context())
	if err != nil {
		log.Printf("list orders: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "siparişler listelenemedi"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"orders": orders})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
