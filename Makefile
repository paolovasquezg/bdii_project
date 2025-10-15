all: docker-up-build

.PHONY: docker-build docker-up docker-down docker-logs docker-backend-sh docker-frontend-sh

docker-build:
	docker compose build

docker-up-build:
	docker compose up --build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-backend-sh:
	docker compose exec backend /bin/sh -lc 'bash || sh'

docker-frontend-sh:
	docker compose exec frontend /bin/sh -lc 'bash || sh'