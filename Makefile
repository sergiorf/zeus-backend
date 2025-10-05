.PHONY: setup bronze silver load gold all clean

setup:
	python -m venv .venv
	.venv/bin/pip install -e .

bronze:
	.venv/bin/zeus ingest cnpj --to bronze
	.venv/bin/zeus ingest cvm --to bronze

silver:
	.venv/bin/zeus normalize cnpj
	.venv/bin/zeus normalize cvm

load:
	.venv/bin/zeus load sqlite

gold:
	.venv/bin/zeus build gold

all: bronze silver load gold

clean:
	rm -f data/warehouse.sqlite
	rm -rf data/bronze data/silver data/gold
