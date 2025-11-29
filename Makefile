# Variables
DOCKER_COMPOSE = docker-compose -f deploy/docker/docker-compose.yml
SPARK_MASTER_CONTAINER = sentinel-spark-master
KAFKA_CONTAINER = sentinel-kafka
DB_CONTAINER = sentinel-db

.PHONY: help up down topic spark-submit producer dashboard clean

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'


# --- INFRASTRUCTURE ---
up: ## Start the Docker infrastructure (Kafka, Spark, Postgres)
	$(DOCKER_COMPOSE) up -d
	@echo "⏳ Waiting 15s for Kafka to be ready..."
	@sleep 15
	@$(MAKE) topic

down: ## Stop and remove all containers
	$(DOCKER_COMPOSE) down

restart: down up ## Restart the infrastructure from scratch

logs: ## Follow Docker logs
	$(DOCKER_COMPOSE) logs -f

# --- SETUP & CONFIG ---

topic: ## Create the Kafka 'transactions' topic (Idempotent-ish)
	@echo "creating kafka topic..."
	@docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists \
		--topic transactions \
		--bootstrap-server localhost:9092 \
		--partitions 1 \
		--replication-factor 1 || echo "Topic might already exist"

db-shell: ## Connect to the Postgres shell inside Docker
	docker exec -it $(DB_CONTAINER) psql -U admin -d fraud_detection_db

# --- RUNNING COMPONENTS ---

submit: ## Submit the Spark Streaming Job
	@echo "🚀 Submitting Spark Job..."
	docker exec -it -u 0 $(SPARK_MASTER_CONTAINER) /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	--packages org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	/opt/spark-app/job.py

producer: ## Run the Golang Transaction Producer
	@echo "💸 Starting Transaction Generator..."
	cd src/producer && go run main.go

dashboard: ## Run the Textual TUI Dashboard
	@echo "📊 Starting Dashboard..."
	python3 src/dashboard/app.py

# --- CLEANUP ---

clean: down ## Nuke everything (Containers + Volumes)
	$(DOCKER_COMPOSE) down -v
	@echo "🧹 All data wiped."