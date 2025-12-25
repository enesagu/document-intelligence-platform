from __future__ import annotations

import io
import json
import hashlib
from datetime import datetime, timedelta

import boto3
from langdetect import detect
from PyPDF2 import PdfReader
import pytesseract
from pdf2image import convert_from_bytes

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

from sqlalchemy import create_engine, text as sql

DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(seconds=10)}

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
RAW_BUCKET = "raw"
PROCESSED_BUCKET = "processed"

DB_URL = "postgresql+psycopg2://dip:dip@postgres:5432/dip"
EXTRACTOR_VERSION = "v0.2.0-enhanced"

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

def db():
    return create_engine(DB_URL, future=True)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def normalize_text(t: str) -> str:
    t = t.replace("\x00", " ")
    t = " ".join(t.split())
    return t.strip()

def trust_score(pages_conf, lang_ok: bool, noise_ratio: float) -> float:
    avg = sum(pages_conf) / max(len(pages_conf), 1)
    score = avg
    if lang_ok:
        score += 0.05
    score -= min(0.20, noise_ratio * 0.5)
    return max(0.0, min(1.0, score))

with DAG(
    dag_id="document_intelligence_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dip", "ocr", "minio", "lineage", "enhanced"],
) as dag:

    @task
    def pick_next_object() -> dict:
        s3 = s3_client()
        try:
            resp = s3.list_objects_v2(Bucket=RAW_BUCKET, MaxKeys=50)
        except Exception:
            raise AirflowSkipException("Bucket not accessible or empty.")
            
        items = resp.get("Contents", [])
        if not items:
            raise AirflowSkipException("No objects in raw bucket.")
        
        items.sort(key=lambda x: x["LastModified"], reverse=True)
        key = items[0]["Key"]
        return {"raw_key": key}

    @task
    def download_raw(obj: dict) -> dict:
        s3 = s3_client()
        raw_key = obj["raw_key"]
        buf = io.BytesIO()
        s3.download_fileobj(RAW_BUCKET, raw_key, buf)
        data = buf.getvalue()
        return {"raw_key": raw_key, "bytes": data.hex()}

    @task
    def extract_text(payload: dict) -> dict:
        data = bytes.fromhex(payload["bytes"])
        raw_key = payload["raw_key"]

        reader = PdfReader(io.BytesIO(data))
        pages = []
        confs = []
        total_chars = 0
        noisy = 0

        for i, p in enumerate(reader.pages, start=1):
            txt = p.extract_text()
            used_ocr = False
            
            # OCR Fallback
            if not txt or len(txt.strip()) < 50:
                print(f"Page {i} lacks text, attempting OCR...")
                try:
                    # Convert page to image
                    indices = [i] # pdf2image uses 1-based indexing if first/last_page are set? No, it takes range.
                    # Efficiently convert single page
                    images = convert_from_bytes(data, first_page=i, last_page=i)
                    if images:
                        txt = pytesseract.image_to_string(images[0])
                        used_ocr = True
                except Exception as e:
                    print(f"OCR failed for page {i}: {e}")
            
            txt_n = normalize_text(txt or "")
            
            # Confidence estimation
            if used_ocr:
                # OCR confidence could be fetched from pytesseract.image_to_data but keeping it simple
                c = 0.85 if len(txt_n) > 100 else 0.4
            else:
                c = min(1.0, max(0.10, len(txt_n) / 1500))
                
            pages.append({"page_number": i, "text": txt_n, "confidence": c})
            confs.append(c)

            total_chars += len(txt_n)
            noisy += sum(1 for ch in txt_n if not (ch.isalnum() or ch.isspace() or ch in ".,;:-_()[]{}%/?'\""))

        noise_ratio = (noisy / max(total_chars, 1))
        
        language = None
        lang_ok = False
        try:
            sample = " ".join([p["text"] for p in pages[:2]])[:1500]
            language = detect(sample) if sample else None
            lang_ok = language is not None
        except Exception:
            language = None

        ts = trust_score(confs, lang_ok=lang_ok, noise_ratio=noise_ratio)

        return {
            "raw_key": raw_key,
            "file_hash": sha256_bytes(data),
            "pages": pages,
            "trust_score": ts,
            "language": language,
            "meta": {"noise_ratio": noise_ratio, "page_count": len(pages), "ocr_used": any(p.get("confidence") == 0.85 for p in pages)},
        }

    @task
    def upsert_version(extracted: dict) -> dict:
        source = "minio:raw"
        file_hash = extracted["file_hash"]
        raw_key = extracted["raw_key"]

        engine = db()
        with engine.begin() as conn:
            doc = conn.execute(
                sql("INSERT INTO documents(source, file_hash) VALUES (:s,:h) "
                    "ON CONFLICT (source, file_hash) DO UPDATE SET source=EXCLUDED.source "
                    "RETURNING id"),
                {"s": source, "h": file_hash},
            ).first()
            document_id = doc[0]

            v_row = conn.execute(
                sql("SELECT COALESCE(MAX(version),0) FROM document_versions WHERE document_id=:d"),
                {"d": document_id},
            ).first()
            v = v_row[0] + 1

            processed_key = f"processed/{document_id}/v{v}/extracted.json"
            dv = conn.execute(
                sql("""INSERT INTO document_versions
                       (document_id, version, raw_object_key, processed_object_key, extractor_version, trust_score, language, meta)
                       VALUES (:d,:v,:rk,:pk,:ev,:ts,:lang,:meta)
                       RETURNING id"""),
                {
                    "d": document_id,
                    "v": v,
                    "rk": raw_key,
                    "pk": processed_key,
                    "ev": EXTRACTOR_VERSION,
                    "ts": extracted["trust_score"],
                    "lang": extracted["language"],
                    "meta": json.dumps(extracted["meta"]),
                },
            ).first()
            version_id = dv[0]
            
            # Fetch previous version ID for diffing
            prev_version_id = None
            if v > 1:
                prev = conn.execute(
                    sql("SELECT id FROM document_versions WHERE document_id=:d AND version=:v"),
                    {"d": document_id, "v": v-1}
                ).first()
                if prev:
                    prev_version_id = prev[0]

        return {
            "document_id": str(document_id), 
            "version_id": str(version_id), 
            "version": v, 
            "prev_version_id": str(prev_version_id) if prev_version_id else None,
            "processed_key": processed_key, 
            **extracted
        }

    @task
    def generate_diff(versioned: dict) -> dict:
        # If there is a previous version, compare texts
        if not versioned.get("prev_version_id"):
            return versioned
            
        prev_id = versioned["prev_version_id"]
        curr_id = versioned["version_id"]
        
        engine = db()
        with engine.begin() as conn:
            # Fetch pages
            prev_pages = conn.execute(sql("SELECT page_number, text FROM pages WHERE version_id=:v ORDER BY page_number"), {"v": prev_id}).mappings().all()
            curr_pages = versioned["pages"]
            
            diffs = []
            # Naive comparison by page number
            curr_map = {p["page_number"]: p["text"] for p in curr_pages}
            prev_map = {p["page_number"]: p["text"] for p in prev_pages}
            
            all_pages = set(curr_map.keys()) | set(prev_map.keys())
            
            for p in all_pages:
                t1 = prev_map.get(p, "")
                t2 = curr_map.get(p, "")
                if t1 != t2:
                    diffs.append({
                        "page": p,
                        "type": "changed" if t1 and t2 else ("added" if t2 else "removed"),
                        "diff_len": len(t2) - len(t1)
                    })
            
            if diffs:
                # Store diff artifact
                diff_key = f"processed/{versioned['document_id']}/v{versioned['version']}/diff_v{versioned['version']-1}.json"
                conn.execute(
                    sql("""INSERT INTO artifacts(version_id, artifact_type, object_key, meta)
                           VALUES (:vid,'diff_json',:k,:m)"""),
                    {"vid": curr_id, "k": diff_key, "m": json.dumps({"diff_count": len(diffs)})}
                )
                # Upload to S3
                s3 = s3_client()
                s3.put_object(Bucket=PROCESSED_BUCKET, Key=diff_key, Body=json.dumps(diffs).encode('utf-8'))
                
        return versioned

    @task
    def store_processed(versioned: dict) -> dict:
        s3 = s3_client()
        processed_key = versioned["processed_key"]
        body = json.dumps(
            {
                "document_id": versioned["document_id"],
                "version": versioned["version"],
                "trust_score": versioned["trust_score"],
                "language": versioned["language"],
                "meta": versioned["meta"],
                "pages": versioned["pages"],
            },
            ensure_ascii=False,
        ).encode("utf-8")

        s3.put_object(Bucket=PROCESSED_BUCKET, Key=processed_key, Body=body, ContentType="application/json")
        return {"processed_key": processed_key, "version_id": versioned["version_id"]}

    @task
    def write_pages_to_db(versioned: dict) -> dict:
        engine = db()
        with engine.begin() as conn:
            for p in versioned["pages"]:
                conn.execute(
                    sql("""INSERT INTO pages(version_id, page_number, text, confidence, meta)
                           VALUES (:vid,:pn,:t,:c,:m)
                           ON CONFLICT (version_id, page_number) DO UPDATE SET text=EXCLUDED.text, confidence=EXCLUDED.confidence, meta=EXCLUDED.meta"""),
                    {
                        "vid": versioned["version_id"],
                        "pn": p["page_number"],
                        "t": p["text"],
                        "c": p["confidence"],
                        "m": json.dumps({}),
                    },
                )
        return {"version_id": versioned["version_id"]}

    @task
    def add_lineage(stored: dict, dag_run=None, ti=None) -> None:
        engine = db()
        with engine.begin() as conn:
            art = conn.execute(
                sql("""INSERT INTO artifacts(version_id, artifact_type, object_key, meta)
                       VALUES (:vid,'extracted_json',:k,:m) RETURNING id"""),
                {"vid": stored["version_id"], "k": stored["processed_key"], "m": json.dumps({"enhanced": True})},
            ).first()
            artifact_id = art[0]

            conn.execute(
                sql("""INSERT INTO lineage(input_version_id, output_artifact_id, airflow_dag_id, airflow_run_id, task_id)
                       VALUES (:invid,:outid,:dag,:run,:task)"""),
                {
                    "invid": stored["version_id"],
                    "outid": artifact_id,
                    "dag": dag_run.dag_id if dag_run else "unknown",
                    "run": dag_run.run_id if dag_run else "unknown",
                    "task": ti.task_id if ti else "unknown",
                },
            )

    picked = pick_next_object()
    raw = download_raw(picked)
    extracted = extract_text(raw)
    versioned = upsert_version(extracted)
    _diff = generate_diff(versioned)
    stored = store_processed(versioned)
    _pages = write_pages_to_db(versioned)
    add_lineage(stored)
