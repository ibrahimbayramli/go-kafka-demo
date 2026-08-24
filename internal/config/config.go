package config

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroup   string
	HTTPAddr     string
	PostgresDSN  string
}

func Load() (Config, error) {
	if err := godotenv.Load(".env.local"); err != nil {
		log.Printf("dotenv local load: %v", err)
	}

	brokersRaw, err := envRequired("KAFKA_BROKERS")
	if err != nil {
		return Config{}, err
	}
	kafkaBrokers := strings.Split(brokersRaw, ",")
	for i := range kafkaBrokers {
		kafkaBrokers[i] = strings.TrimSpace(kafkaBrokers[i])
	}

	kafkaTopic, err := envRequired("KAFKA_TOPIC")
	if err != nil {
		return Config{}, err
	}
	kafkaGroup, err := envRequired("KAFKA_GROUP")
	if err != nil {
		return Config{}, err
	}
	httpAddr, err := envRequired("HTTP_ADDR")
	if err != nil {
		return Config{}, err
	}
	postgresDSN, err := envRequired("POSTGRES_DSN")
	if err != nil {
		return Config{}, err
	}

	return Config{
		KafkaBrokers: kafkaBrokers,
		KafkaTopic:   kafkaTopic,
		KafkaGroup:   kafkaGroup,
		HTTPAddr:     httpAddr,
		PostgresDSN:  postgresDSN,
	}, nil
}

func envRequired(key string) (string, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return "", fmt.Errorf("%s is required; set it in .env.local", key)
	}
	return value, nil
}
