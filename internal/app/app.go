package app

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpapi "order-processor-service/internal/api/http"
	"order-processor-service/internal/config"
	kafka "order-processor-service/internal/infrastructure/kafka"
	"order-processor-service/internal/infrastructure/postgres"
)

type App struct {
	producer   *kafka.Producer
	consumer   *kafka.Consumer
	server     *http.Server
	repository *postgres.Repository
}

func New(cfg config.Config) (*App, error) {
	repository, err := postgres.NewRepository(cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}
	if err := repository.Init(context.Background()); err != nil {
		repository.Close()
		return nil, err
	}

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, repository)

	api := httpapi.NewServer(producer, repository)
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &App{
		producer:   producer,
		consumer:   consumer,
		server:     server,
		repository: repository,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		a.consumer.Run(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("HTTP dinleniyor: %s", a.server.Addr)
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
		log.Println("kapanıyor...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
		<-consumerDone
		return nil
	case err, ok := <-serverErr:
		if ok && err != nil {
			return err
		}
		return nil
	}
}

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	appInstance, err := New(cfg)
	if err != nil {
		log.Fatalf("app init: %v", err)
	}
	defer appInstance.producer.Close()
	defer appInstance.consumer.Close()
	defer appInstance.repository.Close()

	if err := appInstance.Run(ctx); err != nil {
		log.Fatalf("app: %v", err)
	}
}
