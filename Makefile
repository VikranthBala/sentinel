# Variables
DOCKER_COMPOSE = docker-compose -f deploy/docker/docker-compose.yml
SPARK_MASTER_CONTAINER = sentinel-spark-master
KAFKA_CONTAINER = sentinel-kafka
DB_CONTAINER = sentinel-db

# Load environment variables from .env file
include deploy/docker/.env
export

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

topic: ## Create the Kafka 'transactions' topic (Idempotent)
	@echo "📝 Creating Kafka topic..."
	@docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists \
		--topic transactions \
		--bootstrap-server localhost:9092 \
		--partitions 1 \
		--replication-factor 1 || echo "✅ Topic already exists"

db-shell: ## Connect to the Postgres shell inside Docker
	docker exec -it $(DB_CONTAINER) psql -U admin -d fraud_detection_db

db-init: ## Initialize database tables
	@echo "🗄️  Initializing database schema..."
	docker exec -i $(DB_CONTAINER) psql -U admin -d fraud_detection_db < setup.sql
	@echo "✅ Database initialized"

# --- RUNNING COMPONENTS ---

submit: ## Submit the Spark Streaming Job with R2 support
	@echo "🚀 Submitting Spark Job with R2 archiving..."
	@echo "   Endpoint: $(S3_ENDPOINT)"
	docker exec -it -u 0 $(SPARK_MASTER_CONTAINER) /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		--conf spark.hadoop.fs.s3a.access.key=$(AWS_ACCESS_KEY_ID) \
		--conf spark.hadoop.fs.s3a.secret.key=$(AWS_SECRET_ACCESS_KEY) \
		--conf spark.hadoop.fs.s3a.endpoint=$(S3_ENDPOINT) \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
		--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
		/opt/spark-app/job.py --pipeline-id 1

producer: ## Run the Golang Transaction Producer
	@echo "💸 Starting Transaction Generator..."
	cd src/producer && go run main.go

dashboard: ## Run the Textual TUI Dashboard
	@echo "📊 Starting Dashboard..."
	python3 src/dashboard/app.py

# --- MONITORING ---

spark-ui: ## Open Spark Master UI
	@echo "🌐 Opening Spark UI at http://localhost:8080"
	@open http://localhost:8080 || xdg-open http://localhost:8080 || echo "Visit: http://localhost:8080"

kafka-consume: ## Consume messages from Kafka topic (for debugging)
	docker exec -it $(KAFKA_CONTAINER) kafka-console-consumer \
		--topic transactions \
		--from-beginning \
		--bootstrap-server localhost:9092

# --- CLEANUP ---

clean: down ## Nuke everything (Containers + Volumes)
	$(DOCKER_COMPOSE) down -v
	@echo "🧹 All data wiped."

clean-checkpoints: ## Remove Spark checkpoints (use if restarting fresh)
	docker volume rm sentinel_spark-checkpoints || true
	@echo "🗑️  Checkpoints cleared"

# --- UTILITY ---

status: ## Show status of all containers
	@echo "📊 Container Status:"
	@docker ps --filter "name=sentinel-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

env-check: ## Verify environment variables are set
	@echo "🔍 Checking R2 credentials..."
	@test -n "$(AWS_ACCESS_KEY_ID)" || (echo "❌ AWS_ACCESS_KEY_ID not set" && exit 1)
	@test -n "$(AWS_SECRET_ACCESS_KEY)" || (echo "❌ AWS_SECRET_ACCESS_KEY not set" && exit 1)
	@test -n "$(S3_ENDPOINT)" || (echo "❌ S3_ENDPOINT not set" && exit 1)
	@echo "✅ All credentials configured"