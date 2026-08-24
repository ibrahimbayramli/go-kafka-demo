package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"order-processor-service/internal/domain"

	_ "github.com/lib/pq"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(dsn string) (*Repository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Repository{db: db}, nil
}

func (r *Repository) Init(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS orders (
			id VARCHAR(64) PRIMARY KEY,
			customer VARCHAR(255) NOT NULL,
			amount DOUBLE PRECISION NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			status VARCHAR(32) NOT NULL DEFAULT 'pending'
		);`

	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *Repository) SaveOrder(ctx context.Context, order domain.Order) error {
	query := `
		INSERT INTO orders (id, customer, amount, created_at, status)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			customer = EXCLUDED.customer,
			amount = EXCLUDED.amount,
			created_at = EXCLUDED.created_at,
			status = EXCLUDED.status`

	_, err := r.db.ExecContext(ctx, query,
		order.ID,
		order.Customer,
		order.Amount,
		order.Created,
		order.Status,
	)
	return err
}

func (r *Repository) MarkProcessed(ctx context.Context, order domain.Order) error {
	query := `UPDATE orders SET status = $1 WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, domain.OrderStatusProcessed, order.ID)
	return err
}

func (r *Repository) ListOrders(ctx context.Context) ([]domain.Order, error) {
	query := `
		SELECT id, customer, amount, created_at, status
		FROM orders
		ORDER BY created_at DESC`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := make([]domain.Order, 0)
	for rows.Next() {
		var (
			order      domain.Order
			statusText string
		)
		if err := rows.Scan(&order.ID, &order.Customer, &order.Amount, &order.Created, &statusText); err != nil {
			return nil, err
		}
		order.Status = domain.OrderStatus(statusText)
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return orders, nil
}

func (r *Repository) Close() error {
	return r.db.Close()
}
