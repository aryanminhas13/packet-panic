.PHONY: generate pipeline test serve api chaos rebuild lake-up pipeline-s3

generate:
	python generator/generate_events.py

pipeline: generate
	python pipeline/run_pipeline.py --reset

test:
	python -m unittest discover -s tests -v
	dbt test --project-dir warehouse --profiles-dir warehouse

serve:
	python -m http.server 4173 --directory dist

api:
	uvicorn ingestion.app:app --reload

chaos:
	python generator/generate_events.py --failure schema-drift --output data/raw/chaos-events.jsonl
	python pipeline/run_pipeline.py --input data/raw/chaos-events.jsonl


rebuild:
	python pipeline/run_pipeline.py --rebuild-from-lake

lake-up:
	docker compose up -d minio create-bucket

pipeline-s3: lake-up
	docker compose --profile pipeline run --rm pipeline
