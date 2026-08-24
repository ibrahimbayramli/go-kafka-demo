package httpapi

import (
	"net/http"

	controllers "order-processor-service/internal/api/http/controllers"
)

type Server struct {
	orderController *controllers.OrderController
}

func NewServer(publisher controllers.Publisher, repository controllers.Repository) *Server {
	return &Server{orderController: controllers.NewOrderController(publisher, repository)}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders", s.orderController.ListOrders)
	mux.HandleFunc("POST /orders", s.orderController.CreateOrder)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}
