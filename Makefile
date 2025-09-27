# Start all services in detached mode
up:
	docker compose up -d

# Stop and remove all containers, networks, and volumes (clean slate)
down:
	docker compose down --volumes --remove-orphans

# Build only the PySpark batch job container
build-batch:
	docker compose build pyspark-batch

# Build all containers
build-all:
	docker compose build

# Run the PySpark batch job (inside its container)
run-batch:
	docker compose run --rm pyspark-batch

# Connect to TimescaleDB using psql CLI
psql:
	docker exec -it timescaledb psql -U transit_user -d transit_delays

# Tail logs for all services
logs:
	docker compose logs --follow

# Tail logs for a specific service (e.g., make logs SERVICE=mbta-consumer)
logs-service:
	docker compose logs --follow ${SERVICE}

# Restart a specific service (e.g., make restart SERVICE=streamlit-app)
restart:
	docker compose restart ${SERVICE}

# Rebuild and restart everything (if needed)
rebuild:
	docker compose down --volumes --remove-orphans
	docker compose build
	docker compose up -d

# Show running containers
ps:
	docker compose ps

# Access container shell (e.g., make shell SERVICE=mbta-consumer)
shell:
	docker exec -it ${SERVICE} /bin/bash