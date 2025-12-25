# Document Intelligence Platform (Enterprise Edition)

A production-like reference implementation for Document Intelligence with Lineage, Versioning, and Trust Scores.

## Features
- **Extraction**: PDF text extraction + **OCR Fallback (Tesseract)** for image-only PDFs.
- **Versioning**: Content-addressable storage (CAS), page-level version control.
- **Lineage**: Full audit trail of (Input Version -> Task -> Output Artifact).
- **Quality**: Automated "Trust Score" based on confidence, noise, and language detection.
- **Diffing**: Automated change detection between document versions.

## Architecture
Postgres + MinIO + Airflow + FastAPI.
Everything runs in Docker.

## Setup
1. `make up` (Builds images and starts services)
2. Open Airflow: http://localhost:8080 (admin/admin)
3. Open MinIO: http://localhost:9001 (minioadmin/minioadmin)
4. Open API Docs: http://localhost:8000/docs

## Usage
1. Upload a PDF to MinIO bucket `raw`.
2. Enable the `document_intelligence_pipeline` DAG in Airflow.
3. It will process the file, generating extracting text and JSON.
4. Check API or DB for results.
