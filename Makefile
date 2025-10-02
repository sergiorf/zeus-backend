.PHONY: setup bronze silver load gold all clean

    setup:
	python -m venv .venv && . .venv/bin/activate && pip install -e .

    bronze:
	zeus ingest cnpj --to bronze
	zeus ingest cvm --to bronze

    silver:
	zeus normalize cnpj
	zeus normalize cvm

    load:
	zeus load sqlite

    gold:
	zeus build gold

    all: bronze silver load gold

    clean:
	rm -f data/warehouse.sqlite
	rm -rf data/bronze data/silver data/gold
