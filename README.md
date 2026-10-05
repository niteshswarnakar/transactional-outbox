# Transactional Outbox

A Go gateway saves an order and an outbox row in one DB transaction. A worker then publishes the outbox rows to Kafka, and a consumer stores them in a second table.

## Requirements

- Docker (with Docker Compose)
- `make`

## Run (Docker)

```sh
make build   # build the gateway image
make up      # start postgres, kafka and gateway
make down    # stop everything
```

The stack has three containers: `postgres`, `kafka` (single-node KRaft) and `gateway` on port 8080.

## Test

Open http://localhost:8080 and create an order. It appears in **Orders** right away, then in **Kafka orders** once it has been delivered.

Or use the API:

```sh
curl -X POST localhost:8080/orders -H 'Content-Type: application/json' -d '{"name":"book","price":12.5}'
curl localhost:8080/orders          # saved by the gateway
curl localhost:8080/kafka-orders    # stored by the Kafka consumer
```

To see the outbox guarantee, run `docker compose stop kafka`, create an order (it still succeeds and shows as pending), then `docker compose start kafka` and watch it become delivered.

The consumer can take a while to start on a fresh run, because the topic is created on the first publish.
