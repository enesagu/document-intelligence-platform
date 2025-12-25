# Threat Model

- **Malicious PDF**: Users could upload PDFs with exploits. Mitigation: Container separation, non-root users.
- **DDoS**: API rate limiting (not implemented yet).
- **Data Leakage**: MinIO is internal-only by default, API exposes data. Authentication required (basic auth logic valid but needs JWT integration for prod).
