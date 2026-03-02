#!/usr/bin/env python3
"""
Download worker for backend-managed dataset archives.

The worker:
1. Claims pending download requests from Supabase
2. Collects requested source files from S3 buckets
3. Builds a ZIP archive locally
4. Uploads archive to the private download bucket
5. Generates a 7-day presigned URL
6. Sends a Brevo transactional email
7. Periodically cleans up expired archives
"""

import argparse
import json
import logging
import os
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from trees_api.config import StorageConfig, SupabaseConfig
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("download_worker")


DOWNLOAD_WORKER_INTERVAL = int(os.environ.get("DOWNLOAD_WORKER_INTERVAL", "15"))
DOWNLOAD_URL_EXPIRES_SECONDS = int(os.environ.get("DOWNLOAD_URL_EXPIRES_SECONDS", str(7 * 24 * 60 * 60)))
DOWNLOAD_RETENTION_DAYS = int(os.environ.get("DOWNLOAD_RETENTION_DAYS", "7"))
DOWNLOAD_KEY_PREFIX = os.environ.get("DOWNLOAD_KEY_PREFIX", "downloads").strip("/") or "downloads"

BREVO_API_URL = os.environ.get("BREVO_API_URL", "https://api.brevo.com/v3/smtp/email")
BREVO_API_KEY = os.environ.get("BREVO_API_KEY", "")
BREVO_SENDER_EMAIL = os.environ.get("BREVO_SENDER_EMAIL", "no-reply@3dtrees.earth")
BREVO_SENDER_NAME = os.environ.get("BREVO_SENDER_NAME", "3Dtrees")


def _archive_filename(dataset_id: int) -> str:
    return f"3dt_{dataset_id}.zip"


def _archive_root(dataset_id: int) -> str:
    return f"3dt_{dataset_id}"


def _artifact_base_name(dataset_id: int, dataset_item_id: int) -> str:
    return f"3dtree_{dataset_id}_{dataset_item_id}"


def _segmentation_candidates(dataset_id: int, dataset_item_id: int) -> List[str]:
    return [
        f"{dataset_id}/segmentation/{dataset_item_id}.laz",
        f"{dataset_id}/segmentation/{dataset_item_id}.las",
        f"{dataset_id}/segmentation/{dataset_item_id}/segmented.laz",
        f"{dataset_id}/segmentation/{dataset_item_id}/segmented.las",
    ]


def _resolve_segmentation_key(storage: StorageClient, dataset_id: int, dataset_item_id: int) -> Optional[str]:
    for key in _segmentation_candidates(dataset_id, dataset_item_id):
        if storage.file_exists(key, bucket=storage.bucket_name_products):
            return key
    return None


def _build_readme(
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_filename: str,
    segmentation_model: Optional[str],
    workflow_name: Optional[str],
) -> str:
    title = dataset.get("title") or f"Dataset {dataset.get('id')}"
    created = datetime.now(timezone.utc).isoformat()
    dataset_id = dataset.get("id")
    include_raw = bool(request_row.get("include_raw"))
    include_segmentation = bool(request_row.get("include_segmentation"))
    raw_line = "yes" if include_raw else "no"
    seg_line = "yes" if include_segmentation else "no"
    model_line = segmentation_model or "not available in current DB metadata"
    workflow_line = workflow_name or "not available"
    return (
        "# 3Dtrees Dataset Download\n\n"
        "## Dataset\n"
        f"- Dataset ID: {dataset_id}\n"
        f"- Title: {title}\n"
        f"- Download request ID: {request_row.get('id')}\n"
        f"- Generated at (UTC): {created}\n"
        f"- Includes raw data: {raw_line}\n"
        f"- Includes segmentation data: {seg_line}\n\n"
        "## Processing summary\n"
        f"- Galaxy workflow: {workflow_line}\n"
        f"- Segmentation model used: {model_line}\n\n"
        "## Archive structure\n"
        "- data/raw/: raw files if requested\n"
        "- data/segmentation/: segmentation files if requested\n"
        "- metadata.json: machine-readable metadata and filename mapping\n"
        "- LICENSE.txt: license notice, attribution, and citation guidance\n\n"
        "## Naming convention\n"
        f"- Archive filename: {archive_filename}\n"
        "- Raw file: 3dtree_{dataset_id}_{item_id}_raw.{ext}\n"
        "- Segmentation file: 3dtree_{dataset_id}_{item_id}_segmentation.{ext}\n\n"
        "## Dataset page\n"
        f"- https://3dtrees.earth/datasets/{dataset_id}\n"
    )


def _build_license_note(dataset: Dict[str, Any], segmentation_model: Optional[str]) -> str:
    dataset_id = dataset.get("id")
    title = dataset.get("title") or f"Dataset {dataset_id}"
    year = datetime.now(timezone.utc).year
    access_date = datetime.now(timezone.utc).date().isoformat()
    model_line = segmentation_model or "not available in current DB metadata"
    return (
        "3Dtrees License, Citation, and Attribution\n"
        "==========================================\n\n"
        "License note\n"
        "This archive is distributed through 3Dtrees. Dataset-specific legal terms can vary.\n"
        "Use the dataset page as the authoritative source for rights and restrictions.\n\n"
        "Attribution\n"
        "When reusing this data, include at minimum:\n"
        "- Project: 3Dtrees\n"
        f"- Dataset ID: {dataset_id}\n"
        f"- Dataset title: {title}\n"
        f"- Access date: {access_date}\n"
        f"- URL: https://3dtrees.earth/datasets/{dataset_id}\n\n"
        "Segmentation model note\n"
        f"- Model used (if available): {model_line}\n\n"
        "Suggested citation\n"
        f'3Dtrees ({year}). "{title}" (Dataset ID: {dataset_id}). '
        f"3dtrees.earth. Accessed {access_date}.\n"
    )


def _extract_model_from_parameters(
    node: Any,
    path: str = "",
) -> tuple[Optional[str], Optional[str]]:
    candidate_keys = {
        "segmentation_model",
        "model",
        "model_name",
        "model_id",
        "model_path",
        "checkpoint",
        "checkpoint_path",
        "weights",
        "weights_path",
        "ckpt",
    }

    if isinstance(node, dict):
        for key, value in node.items():
            key_lower = str(key).lower()
            child_path = f"{path}.{key}" if path else str(key)
            if isinstance(value, (str, int, float, bool)):
                if key_lower in candidate_keys or "model" in key_lower or "checkpoint" in key_lower:
                    value_str = str(value).strip()
                    if value_str:
                        return value_str, child_path
            model_value, source = _extract_model_from_parameters(value, child_path)
            if model_value:
                return model_value, source
    elif isinstance(node, list):
        for idx, value in enumerate(node):
            child_path = f"{path}[{idx}]" if path else f"[{idx}]"
            model_value, source = _extract_model_from_parameters(value, child_path)
            if model_value:
                return model_value, source

    return None, None


def _build_metadata_json(
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_filename: str,
    mapping_rows: List[Dict[str, Any]],
    generated_at: datetime,
    invocation_row: Optional[Dict[str, Any]],
    segmentation_rows_by_item: Dict[int, Dict[str, Any]],
    standardization_rows_by_item: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    dataset_id = int(dataset["id"])
    title = dataset.get("title") or f"Dataset {dataset_id}"
    year = generated_at.year
    access_date = generated_at.date().isoformat()
    invocation_parameters = (invocation_row or {}).get("parameters") or {}
    used_model, model_source = _extract_model_from_parameters(invocation_parameters)

    item_processing = []
    for item in dataset.get("dataset_items") or []:
        item_id = int(item["id"])
        seg_row = segmentation_rows_by_item.get(item_id) or {}
        std_row = standardization_rows_by_item.get(item_id) or {}
        item_processing.append(
            {
                "dataset_item_id": item_id,
                "segmentation_process_duration_minutes": seg_row.get("segmentation_process_duration_minutes"),
                "standardization_process_duration_minutes": std_row.get("standard_process_duration_minutes"),
                "coordinate_reference": std_row.get("coordinate_reference"),
            }
        )

    return {
        "schema_version": "1.0.0",
        "archive": {
            "name": archive_filename,
            "root_folder": _archive_root(dataset_id),
            "dataset_id": dataset_id,
            "generated_at_utc": generated_at.isoformat(),
        },
        "dataset": {
            "id": dataset_id,
            "uuid": dataset.get("uuid"),
            "title": title,
            "visibility": dataset.get("visibility"),
            "archived": bool(dataset.get("archived")),
            "dataset_url": f"https://3dtrees.earth/datasets/{dataset_id}",
        },
        "request": {
            "download_request_id": int(request_row["id"]),
            "include_raw": bool(request_row.get("include_raw")),
            "include_segmentation": bool(request_row.get("include_segmentation")),
        },
        "processing": {
            "galaxy_workflow": {
                "invocation_id": (invocation_row or {}).get("invocation_id"),
                "workflow_name": (invocation_row or {}).get("workflow_name"),
                "status": (invocation_row or {}).get("status"),
                "started_at": (invocation_row or {}).get("started_at"),
                "finished_at": (invocation_row or {}).get("finished_at"),
            },
            "segmentation": {
                "used_model": used_model,
                "model_source": model_source,
            },
            "items": item_processing,
        },
        "files": mapping_rows,
        "attribution": {
            "project": "3Dtrees",
            "required_fields": [
                "project",
                "dataset_id",
                "dataset_title",
                "access_date",
                "dataset_url",
            ],
            "dataset_id": dataset_id,
            "dataset_title": title,
            "access_date": access_date,
            "dataset_url": f"https://3dtrees.earth/datasets/{dataset_id}",
        },
        "citation": {
            "recommended": (
                f'3Dtrees ({year}). "{title}" (Dataset ID: {dataset_id}). '
                f"3dtrees.earth. Accessed {access_date}."
            ),
        },
    }


def _build_archive_sources(
    storage: StorageClient,
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_root: str,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[int], List[int]]:
    dataset_id = int(dataset["id"])
    include_raw = bool(request_row.get("include_raw"))
    include_seg = bool(request_row.get("include_segmentation"))
    items = dataset.get("dataset_items") or []

    sources: List[Dict[str, Any]] = []
    mapping_rows: List[Dict[str, Any]] = []
    missing_segmentation: List[int] = []
    missing_raw: List[int] = []

    for item in items:
        item_id = int(item["id"])
        original_name = item.get("file_name") or ""
        base_name = _artifact_base_name(dataset_id, item_id)

        if include_raw:
            raw_key = item.get("bucket_path")
            if raw_key:
                if not storage.file_exists(raw_key, bucket=storage.bucket_name_raw):
                    missing_raw.append(item_id)
                else:
                    raw_ext = Path(raw_key).suffix or Path(original_name).suffix or ".laz"
                    arcname = f"{archive_root}/data/raw/{base_name}_raw{raw_ext}"
                    sources.append(
                        {
                            "bucket": storage.bucket_name_raw,
                            "key": raw_key,
                            "arcname": arcname,
                        }
                    )
                    mapping_rows.append(
                        {
                            "dataset_item_id": item_id,
                            "artifact_type": "raw",
                            "archive_path": arcname,
                            "source_bucket": storage.bucket_name_raw,
                            "source_key": raw_key,
                            "original_filename": original_name,
                        }
                    )

        if include_seg:
            seg_key = _resolve_segmentation_key(storage, dataset_id, item_id)
            if not seg_key:
                missing_segmentation.append(item_id)
            else:
                seg_ext = Path(seg_key).suffix or ".laz"
                arcname = f"{archive_root}/data/segmentation/{base_name}_segmentation{seg_ext}"
                sources.append(
                    {
                        "bucket": storage.bucket_name_products,
                        "key": seg_key,
                        "arcname": arcname,
                    }
                )
                mapping_rows.append(
                    {
                        "dataset_item_id": item_id,
                        "artifact_type": "segmentation",
                        "archive_path": arcname,
                        "source_bucket": storage.bucket_name_products,
                        "source_key": seg_key,
                        "original_filename": original_name,
                    }
                )

    return sources, mapping_rows, missing_segmentation, missing_raw


def _build_download_email_subject(archive_filename: str) -> str:
    return f"Your 3Dtrees download is ready: {archive_filename}"


def _build_download_email_html(
    *,
    archive_filename: str,
    signed_url: str,
    dataset_id: int,
    dataset_title: str,
    expires_at: datetime,
) -> str:
    expires_text = expires_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    dataset_url = f"https://3dtrees.earth/datasets/{dataset_id}"
    return (
        "<div style=\"font-family: Arial, sans-serif; line-height: 1.5; color: #111827;\">"
        "<h2 style=\"margin-bottom: 8px;\">Your 3Dtrees download is ready</h2>"
        "<p style=\"margin-top: 0;\">"
        f"Dataset <strong>{dataset_id}</strong> ({dataset_title}) has been packaged for download."
        "</p>"
        "<div style=\"margin: 20px 0;\">"
        f"<a href=\"{signed_url}\" "
        "style=\"background:#2563eb;color:#ffffff;text-decoration:none;padding:10px 14px;"
        "border-radius:6px;display:inline-block;\">"
        "Download archive"
        "</a>"
        "</div>"
        "<p style=\"margin: 0;\">"
        f"<strong>Archive:</strong> {archive_filename}<br>"
        f"<strong>Expires:</strong> {expires_text}"
        "</p>"
        "<p style=\"margin-top: 12px;\">"
        f"Dataset page: <a href=\"{dataset_url}\">{dataset_url}</a>"
        "</p>"
        "<hr style=\"border:none;border-top:1px solid #e5e7eb;margin:20px 0;\">"
        "<p style=\"font-size: 12px; color: #6b7280; margin: 0;\">"
        "This is an automated message from 3Dtrees."
        "</p>"
        "</div>"
    )


def _build_download_email_text(
    *,
    archive_filename: str,
    signed_url: str,
    dataset_id: int,
    dataset_title: str,
    expires_at: datetime,
) -> str:
    expires_text = expires_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    dataset_url = f"https://3dtrees.earth/datasets/{dataset_id}"
    return (
        "Your 3Dtrees download is ready.\n\n"
        f"Dataset: {dataset_id} ({dataset_title})\n"
        f"Archive: {archive_filename}\n"
        f"Expires: {expires_text}\n\n"
        f"Download: {signed_url}\n"
        f"Dataset page: {dataset_url}\n"
    )


def _send_brevo_email(
    to_email: str,
    archive_filename: str,
    signed_url: str,
    dataset_id: int,
    dataset_title: str,
    signed_url_expires_at: datetime,
) -> None:
    if not BREVO_API_KEY:
        raise RuntimeError("BREVO_API_KEY is not configured")

    subject = _build_download_email_subject(archive_filename)
    payload = {
        "sender": {"email": BREVO_SENDER_EMAIL, "name": BREVO_SENDER_NAME},
        "to": [{"email": to_email}],
        "subject": subject,
        "htmlContent": _build_download_email_html(
            archive_filename=archive_filename,
            signed_url=signed_url,
            dataset_id=dataset_id,
            dataset_title=dataset_title,
            expires_at=signed_url_expires_at,
        ),
        "textContent": _build_download_email_text(
            archive_filename=archive_filename,
            signed_url=signed_url,
            dataset_id=dataset_id,
            dataset_title=dataset_title,
            expires_at=signed_url_expires_at,
        ),
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY,
    }
    with httpx.Client(timeout=30.0) as client:
        response = client.post(BREVO_API_URL, headers=headers, json=payload)
        response.raise_for_status()


def _fail_request(
    supabase: SupabaseClient,
    request_id: int,
    failure_code: str,
    failure_reason: str,
) -> None:
    supabase.update_download_request(
        request_id,
        status="failed",
        failure_code=failure_code,
        failure_reason=failure_reason[:2000],
        finished_at=datetime.now(timezone.utc),
    )


def _process_download_request(supabase: SupabaseClient, storage: StorageClient, request_row: Dict[str, Any]) -> None:
    request_id = int(request_row["id"])
    dataset_id = int(request_row["dataset_id"])

    dataset = supabase.get_dataset_with_items(dataset_id)
    if not dataset:
        _fail_request(supabase, request_id, "dataset_not_found", f"Dataset {dataset_id} not found")
        return
    if dataset.get("archived"):
        _fail_request(supabase, request_id, "dataset_archived", f"Dataset {dataset_id} is archived")
        return

    if not request_row.get("include_raw") and not request_row.get("include_segmentation"):
        _fail_request(
            supabase,
            request_id,
            "invalid_request",
            "At least one artifact type must be requested",
        )
        return

    archive_root = _archive_root(dataset_id)
    sources, mapping_rows, missing_segmentation, missing_raw = _build_archive_sources(
        storage,
        dataset,
        request_row,
        archive_root=archive_root,
    )

    if request_row.get("include_raw") and missing_raw:
        missing = ", ".join(str(item_id) for item_id in missing_raw)
        _fail_request(
            supabase,
            request_id,
            "failed_missing_artifacts",
            f"Missing raw artifacts for dataset_item_ids: {missing}",
        )
        return

    if request_row.get("include_segmentation") and missing_segmentation:
        missing = ", ".join(str(item_id) for item_id in missing_segmentation)
        _fail_request(
            supabase,
            request_id,
            "failed_missing_artifacts",
            f"Missing segmentation artifacts for dataset_item_ids: {missing}",
        )
        return

    if not sources:
        _fail_request(supabase, request_id, "empty_archive", "No source files resolved for request")
        return

    now = datetime.now(timezone.utc)
    archive_filename = _archive_filename(dataset_id)
    archive_key = f"{DOWNLOAD_KEY_PREFIX}/{dataset_id}/{request_id}/{archive_filename}"
    archive_bucket = storage.bucket_name_download
    signed_url_expires_at = now + timedelta(seconds=DOWNLOAD_URL_EXPIRES_SECONDS)
    archive_expires_at = now + timedelta(days=DOWNLOAD_RETENTION_DAYS)
    dataset_item_ids = [int(item["id"]) for item in (dataset.get("dataset_items") or [])]
    invocation_row = supabase.get_current_workflow_invocation_for_dataset(dataset_id)
    segmentation_rows_by_item = (
        supabase.get_segmentation_rows_for_items(dataset_item_ids)
        if request_row.get("include_segmentation")
        else {}
    )
    standardization_rows_by_item = supabase.get_standardization_rows_for_items(dataset_item_ids)

    metadata_payload = _build_metadata_json(
        dataset=dataset,
        request_row=request_row,
        archive_filename=archive_filename,
        mapping_rows=mapping_rows,
        generated_at=now,
        invocation_row=invocation_row,
        segmentation_rows_by_item=segmentation_rows_by_item,
        standardization_rows_by_item=standardization_rows_by_item,
    )

    with tempfile.TemporaryDirectory(prefix="3dtrees-download-") as work_dir:
        work_path = Path(work_dir)
        archive_local_path = work_path / archive_filename
        staged_file_path = work_path / "staged.bin"

        try:
            with zipfile.ZipFile(
                archive_local_path,
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
                allowZip64=True,
            ) as zip_file:
                zip_file.writestr(
                    f"{archive_root}/README.md",
                    _build_readme(
                        dataset=dataset,
                        request_row=request_row,
                        archive_filename=archive_filename,
                        segmentation_model=(metadata_payload.get("processing") or {})
                        .get("segmentation", {})
                        .get("used_model"),
                        workflow_name=(metadata_payload.get("processing") or {})
                        .get("galaxy_workflow", {})
                        .get("workflow_name"),
                    ),
                )
                zip_file.writestr(
                    f"{archive_root}/LICENSE.txt",
                    _build_license_note(
                        dataset,
                        segmentation_model=(metadata_payload.get("processing") or {})
                        .get("segmentation", {})
                        .get("used_model"),
                    ),
                )
                zip_file.writestr(
                    f"{archive_root}/metadata.json",
                    json.dumps(metadata_payload, indent=2),
                )

                for source in sources:
                    if staged_file_path.exists():
                        staged_file_path.unlink()
                    storage.download_file(
                        key=source["key"],
                        file_path=staged_file_path,
                        bucket=source["bucket"],
                    )
                    zip_file.write(staged_file_path, arcname=source["arcname"])
                    staged_file_path.unlink(missing_ok=True)

            archive_size = archive_local_path.stat().st_size
            storage.upload_file(
                file_path=archive_local_path,
                key=archive_key,
                bucket=archive_bucket,
            )
            signed_url = storage.generate_presigned_download_url(
                key=archive_key,
                expires_in=DOWNLOAD_URL_EXPIRES_SECONDS,
                bucket=archive_bucket,
            )

            supabase.update_download_request(
                request_id,
                status="completed",
                archive_bucket=archive_bucket,
                archive_key=archive_key,
                archive_filename=archive_filename,
                archive_size_bytes=archive_size,
                signed_url=signed_url,
                signed_url_expires_at=signed_url_expires_at,
                expires_at=archive_expires_at,
                metadata=metadata_payload,
                finished_at=datetime.now(timezone.utc),
            )

            try:
                dataset_title = dataset.get("title") or f"Dataset {dataset_id}"
                _send_brevo_email(
                    to_email=request_row["requester_email"],
                    archive_filename=archive_filename,
                    signed_url=signed_url,
                    dataset_id=dataset_id,
                    dataset_title=dataset_title,
                    signed_url_expires_at=signed_url_expires_at,
                )
                supabase.update_download_request(
                    request_id,
                    email_sent_at=datetime.now(timezone.utc),
                )
            except Exception as email_error:
                logger.error(f"Email send failed for request {request_id}: {email_error}")
                supabase.update_download_request(
                    request_id,
                    status="failed_email",
                    failure_code="email_send_failed",
                    failure_reason=str(email_error),
                )

        except Exception as e:
            logger.error(f"Failed processing download request {request_id}: {e}")
            _fail_request(supabase, request_id, "archive_creation_failed", str(e))


def _cleanup_expired_archives(supabase: SupabaseClient, storage: StorageClient) -> int:
    now = datetime.now(timezone.utc)
    expired = supabase.list_expired_download_requests(now=now)
    cleaned = 0

    for row in expired:
        request_id = int(row["id"])
        archive_key = row.get("archive_key")
        archive_bucket = row.get("archive_bucket") or storage.bucket_name_download
        try:
            if archive_key:
                storage.delete_object(archive_key, bucket=archive_bucket)
            supabase.update_download_request(
                request_id,
                status="expired",
                signed_url=None,
                failure_code=None,
                failure_reason=None,
            )
            cleaned += 1
        except Exception as e:
            logger.warning(f"Failed cleanup for request {request_id}: {e}")

    return cleaned


def get_connected_clients() -> tuple[SupabaseClient, StorageClient]:
    supabase = SupabaseClient(SupabaseConfig())
    storage = StorageClient(StorageConfig())

    supabase.connect()
    if (not supabase.using_service_role) and supabase.email and supabase.password:
        try:
            supabase.authenticate_user(supabase.email, supabase.password)
        except Exception:
            logger.warning("Supabase user authentication failed; proceeding with current credentials")

    storage.connect()
    return supabase, storage


def run_once() -> Dict[str, Any]:
    stats = {"processed": 0, "cleaned": 0, "success": True}
    try:
        supabase, storage = get_connected_clients()
        stats["cleaned"] = _cleanup_expired_archives(supabase, storage)

        while True:
            claimed = supabase.claim_next_pending_download_request()
            if not claimed:
                break
            _process_download_request(supabase, storage, claimed)
            stats["processed"] += 1

        return stats
    except Exception as e:
        logger.error(f"Download worker cycle failed: {e}")
        stats["success"] = False
        stats["error"] = str(e)
        return stats


def run_continuous(interval: int = DOWNLOAD_WORKER_INTERVAL) -> None:
    cycle = 0
    while True:
        cycle += 1
        logger.info(f"Starting download worker cycle #{cycle}")
        stats = run_once()
        if stats.get("success"):
            logger.info(
                "Download cycle complete: processed=%s cleaned=%s",
                stats.get("processed", 0),
                stats.get("cleaned", 0),
            )
        else:
            logger.error("Download cycle failed: %s", stats.get("error"))
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="3Dtrees download worker")
    parser.add_argument("--continuous", action="store_true", help="Run in loop mode")
    parser.add_argument(
        "--interval",
        type=int,
        default=DOWNLOAD_WORKER_INTERVAL,
        help="Loop interval in seconds for continuous mode",
    )
    args = parser.parse_args()

    if args.continuous:
        run_continuous(interval=args.interval)
    else:
        stats = run_once()
        if not stats.get("success"):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
