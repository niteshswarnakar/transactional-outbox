run:
	go run gateway/main.go

build:
	docker compose -f docker-compose.yaml build

down:
	docker compose -f docker-compose.yaml down

up:
	docker compose -f docker-compose.yaml up -d