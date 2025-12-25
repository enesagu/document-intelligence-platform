.PHONY: up down logs ps seed

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

seed:
	@echo "Put a file into MinIO raw bucket via console: http://localhost:9001"
