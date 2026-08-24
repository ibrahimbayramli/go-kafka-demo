# Order Processor Service

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.26+-00ADD8?style=for-the-badge&logo=go" alt="Go 1.26+" />
  <img src="https://img.shields.io/badge/Kafka-Event%20Driven-231F20?style=for-the-badge&logo=apachekafka" alt="Kafka" />
  <img src="https://img.shields.io/badge/PostgreSQL-Database-4169E1?style=for-the-badge&logo=postgresql" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/API-REST-4CAF50?style=for-the-badge" alt="REST API" />
</p>

A modern event-driven order processing service written in Go. It accepts incoming orders through HTTP, stores them in PostgreSQL, emits Kafka events, and asynchronously processes messages in the consumer layer.

## Overview

This project demonstrates a clean backend flow built for local development and event-driven processing:

- HTTP API for creating and listing orders
- PostgreSQL persistence for durable storage
- Kafka producer for publishing events
- Kafka consumer for async processing
- Typed domain model and clear project layering

## Architecture

```text
Client
  |
  v
HTTP API
  |
  | 1) Validate order request
  v
OrderController
  |
  | 2) Save order in PostgreSQL
  v
Repository
  |
  | 3) Publish order event to Kafka
  v
Kafka Producer
  |
  v
Kafka Topic: orders
  |
  | 4) Consumer reads message
  v
Kafka Consumer
  |
  | 5) Mark order as processed
  v
PostgreSQL
```

## Features

- Create orders through a REST endpoint
- List orders from the database
- Persist status transitions
- Publish order events to Kafka
- Process events asynchronously in the consumer
- Maintain a clean separation between API, domain, and infrastructure

## Prerequisites

- Go 1.26+
- PostgreSQL instance
- Kafka broker

## Run

```bash
go run .
```

## Endpoints

### GET /healthz

```bash
curl http://localhost:8080/healthz
```

Response:

```json
{"status":"ok"}
```

### POST /orders

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"Ali","amount":150.5}'
```

### GET /orders

```bash
curl http://localhost:8080/orders
```

## Notes

- Local configuration values are kept outside the repository.
- Real credentials should not be committed.
- Kafka and PostgreSQL must be running before launching the app.
