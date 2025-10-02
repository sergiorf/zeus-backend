CREATE TABLE IF NOT EXISTS file_ingest_log (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  path TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  bytes INTEGER NOT NULL,
  discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed_at TEXT,
  stage TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_file ON file_ingest_log(source, path, sha256);

CREATE TABLE IF NOT EXISTS cnpj_firmographics (
  cnpj14 TEXT PRIMARY KEY,
  legal_name TEXT,
  trade_name TEXT,
  status TEXT,
  opening_date TEXT,
  cnae TEXT,
  cnae_desc TEXT,
  legal_nature TEXT,
  size_bracket TEXT,
  uf TEXT,
  municipality_code TEXT,
  cep TEXT
);
CREATE INDEX IF NOT EXISTS ix_cnpj_cnae ON cnpj_firmographics(cnae);
CREATE INDEX IF NOT EXISTS ix_cnpj_uf ON cnpj_firmographics(uf);

CREATE TABLE IF NOT EXISTS cvm_facts (
  cnpj14 TEXT NOT NULL,
  company_name TEXT,
  period_start TEXT,
  period_end TEXT,
  fy TEXT,
  fq INTEGER,
  concept TEXT NOT NULL,
  value REAL,
  unit TEXT,
  consolidated INTEGER,
  PRIMARY KEY (cnpj14, period_end, concept, consolidated)
);
CREATE INDEX IF NOT EXISTS ix_cvm_concept ON cvm_facts(concept);

CREATE VIEW IF NOT EXISTS v_cvm_latest AS
SELECT cf.* FROM cvm_facts cf
JOIN (
  SELECT cnpj14, concept, MAX(period_end) AS max_pe
  FROM cvm_facts WHERE fq=0 GROUP BY cnpj14, concept
) mx ON mx.cnpj14=cf.cnpj14 AND mx.concept=cf.concept AND mx.max_pe=cf.period_end;
