.PHONY: bootstrap lint test integration demo benchmark verify
bootstrap:
	uv sync --dev
	cd web && npm install
lint:
	uv run ruff check .
test:
	uv run pytest -q
integration: test
demo:
	docker compose up --build
benchmark:
	uv run pytest -q --durations=10
verify: lint test
	cd web && npm run build
