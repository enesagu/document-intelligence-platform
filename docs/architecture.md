# Architecture

## Components
- **MinIO**: Object storage for Raw PDF and Processed JSON.
- **Postgres**: Metadata storage (Documents, Versions, Pages, Lineage).
- **Airflow**: Orchestrator (DAGs) for extraction pipeline.
- **Extractor**: Logic for PDF text extraction, OCR (Tesseract), and Validation.
- **API**: FastAPI service to query documents and lineage.

## Flow
1. User uploads PDF to MinIO `raw` bucket.
2. Airflow DAG triggers (scheduled or event-based).
3. DAG:
   - Downloads PDF.
   - Extract text (PyPDF2) + OCR Fallback (Tesseract).
   - Computes Trust Score.
   - Stores version in Postgres `document_versions`.
   - Generates Diff if previous version exists.
   - Stores pages in `pages` table.
   - Uploads JSON result to `processed` bucket.
   - Records Lineage in `lineage` table.
