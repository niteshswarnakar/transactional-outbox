FROM golang:1.26-bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -tags dynamic -o /app/gateway ./gateway

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/gateway .

EXPOSE 8080

CMD ["./gateway"]
