CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS documents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL,
  file_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(source, file_hash)
);

CREATE TABLE IF NOT EXISTS document_versions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  version INT NOT NULL,
  raw_object_key TEXT NOT NULL,
  processed_object_key TEXT NOT NULL,
  extractor_version TEXT NOT NULL,
  trust_score NUMERIC(5,4) NOT NULL,
  language TEXT,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(document_id, version)
);

CREATE TABLE IF NOT EXISTS pages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  version_id UUID NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
  page_number INT NOT NULL,
  text TEXT NOT NULL,
  confidence NUMERIC(5,4) NOT NULL,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE(version_id, page_number)
);

CREATE TABLE IF NOT EXISTS artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  version_id UUID NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL, -- "normalized_text", "json_index", "diff", ...
  object_key TEXT NOT NULL,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lineage (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  input_version_id UUID NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
  output_artifact_id UUID REFERENCES artifacts(id) ON DELETE SET NULL,
  airflow_dag_id TEXT NOT NULL,
  airflow_run_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reprocess_requests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  reason TEXT,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'PENDING' -- PENDING/RUNNING/DONE/FAILED
);
