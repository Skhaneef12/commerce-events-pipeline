# Common commands. Run from the repo root.
.PHONY: jar test up down produce backfill logs clean

jar:            ## Compile, test and build the fat jar used by spark-submit
	cd spark-jobs && sbt test assembly

test:           ## Run all test suites
	cd spark-jobs && sbt test
	pytest -q producer/tests

up: jar         ## Start Kafka, MongoDB, the streaming job and Airflow
	mkdir -p data
	docker compose up -d --build

produce:        ## Send simulated events for 10 minutes
	pip install -q -r producer/requirements.txt
	cd producer && python producer.py --bootstrap localhost:29092 --rate 20 --duration 600

backfill:       ## Example: rebuild one week of gold metrics
	docker exec airflow airflow dags backfill -s 2026-10-01 -e 2026-10-07 daily_customer_metrics

logs:
	docker compose logs -f spark-streaming

down:
	docker compose down

clean: down     ## Remove local Delta tables and checkpoints
	rm -rf data
