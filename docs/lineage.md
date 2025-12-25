# Lineage Tracking

We track the provenance of every artifact.

## Database Schema
- **documents**: Unique files (content-addressable by hash).
- **document_versions**: Processing runs for a document.
- **artifacts**: Outputs (JSON, Diffs).
- **lineage**: Links Input Version -> Output Artifact via Airflow Task.

## Example Query
Check `services/api/app/main.py` for `/lineage/{version_id}` implementation.
