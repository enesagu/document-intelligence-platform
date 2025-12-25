from fastapi import FastAPI
from sqlalchemy import text
from .db import engine

app = FastAPI(title="Document Intelligence Platform API")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/documents")
def list_documents(limit: int = 50):
    q = text("""
        SELECT d.id, d.source, d.file_hash, d.created_at,
               (SELECT MAX(version) FROM document_versions dv WHERE dv.document_id=d.id) AS latest_version
        FROM documents d
        ORDER BY d.created_at DESC
        LIMIT :limit
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"limit": limit}).mappings().all()
    # Serialize UUIDs and Datetimes if needed, but FastAPI/Pydantic usually handles it if we return list of dicts or rows are compatible.
    # Text results might need conversion to dict explicitly if mappings() returns object that isn't directly JSON serializable by default JSONResponse?
    # FastAPI usually handles dicts fine.
    return {"items": [dict(row) for row in rows]}

@app.get("/documents/{document_id}/versions")
def versions(document_id: str):
    q = text("""
        SELECT id, version, trust_score, language, raw_object_key, processed_object_key, extractor_version, created_at
        FROM document_versions
        WHERE document_id = :d
        ORDER BY version DESC
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"d": document_id}).mappings().all()
    return {"items": [dict(row) for row in rows]}

@app.get("/versions/{version_id}/pages")
def pages(version_id: str):
    q = text("""
        SELECT page_number, confidence, left(text, 500) AS preview
        FROM pages
        WHERE version_id=:v
        ORDER BY page_number
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"v": version_id}).mappings().all()
    return {"items": [dict(row) for row in rows]}

@app.get("/lineage/{version_id}")
def lineage(version_id: str):
    q = text("""
      SELECT l.airflow_dag_id, l.airflow_run_id, l.task_id, l.created_at,
             a.artifact_type, a.object_key
      FROM lineage l
      LEFT JOIN artifacts a ON a.id = l.output_artifact_id
      WHERE l.input_version_id=:v
      ORDER BY l.created_at DESC
      LIMIT 200
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"v": version_id}).mappings().all()
    return {"items": [dict(row) for row in rows]}
