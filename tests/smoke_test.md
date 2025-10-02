# Smoke Test

1) Place at least one .zip in `data/raw/cnpj/` and `data/raw/cvm/`.
2) `make setup && make bronze && make silver && make load`
3) `sqlite3 data/warehouse.sqlite ".tables"` should show `cnpj_firmographics` and `cvm_facts`.
