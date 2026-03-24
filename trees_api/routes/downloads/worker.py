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
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from trees_api.core.config import StorageConfig, SupabaseConfig
from trees_api.integrations.notifications.client import BrevoEmailConfig
from trees_api.routes.downloads.support.archive_writer import write_download_archive
from trees_api.routes.downloads.support.metadata_enrichment import (
    build_bibtex_citation,
    build_datacite_payload,
    build_license_note,
    build_metadata_model,
    build_readme,
)
from trees_api.routes.downloads.support.notifier import send_download_ready_email
from trees_api.routes.downloads.support.source_resolution import (
    archive_filename,
    archive_root,
    build_archive_sources,
)
from trees_api.integrations.storage.client import StorageClient
from trees_api.integrations.supabase.client import SupabaseClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("download_worker")


DOWNLOAD_WORKER_INTERVAL = int(os.environ.get("DOWNLOAD_WORKER_INTERVAL", "15"))
DOWNLOAD_URL_EXPIRES_SECONDS = int(
    os.environ.get("DOWNLOAD_URL_EXPIRES_SECONDS", str(7 * 24 * 60 * 60))
)
DOWNLOAD_RETENTION_DAYS = int(os.environ.get("DOWNLOAD_RETENTION_DAYS", "7"))
DOWNLOAD_KEY_PREFIX = os.environ.get("DOWNLOAD_KEY_PREFIX", "downloads").strip("/") or "downloads"

BREVO_API_URL = os.environ.get("BREVO_API_URL", "https://api.brevo.com/v3/smtp/email")
BREVO_API_KEY = os.environ.get("BREVO_API_KEY", "")
BREVO_SENDER_EMAIL = os.environ.get("BREVO_SENDER_EMAIL", "no-reply@3dtrees.earth")
BREVO_SENDER_NAME = os.environ.get("BREVO_SENDER_NAME", "3Dtrees")
BREVO_REPLY_TO_EMAIL = os.environ.get("BREVO_REPLY_TO_EMAIL", "").strip() or None
BREVO_REPLY_TO_NAME = os.environ.get("BREVO_REPLY_TO_NAME", "").strip() or None


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


def _fail_request_safely(
    supabase: SupabaseClient,
    request_id: int,
    failure_code: str,
    failure_reason: str,
) -> None:
    try:
        _fail_request(
            supabase=supabase,
            request_id=request_id,
            failure_code=failure_code,
            failure_reason=failure_reason,
        )
    except Exception as update_error:
        logger.error(
            "Could not persist failure state for request %s: %s",
            request_id,
            update_error,
        )


def _safe_metadata_enrichment(
    supabase: SupabaseClient,
    dataset_id: int,
    dataset_item_ids: List[int],
    include_segmentation: bool,
) -> tuple[
    Optional[Dict[str, Any]],
    Dict[int, Dict[str, Any]],
    Dict[int, Dict[str, Any]],
    List[str],
]:
    warnings: List[str] = []
    invocation_row: Optional[Dict[str, Any]] = None
    segmentation_rows_by_item: Dict[int, Dict[str, Any]] = {}
    standardization_rows_by_item: Dict[int, Dict[str, Any]] = {}

    try:
        invocation_row = supabase.get_current_workflow_invocation_for_dataset(dataset_id)
    except Exception as error:
        logger.warning(
            "Workflow enrichment failed for dataset %s: %s", dataset_id, error
        )
        warnings.append(
            "workflow_enrichment_unavailable: could not load galaxy workflow invocation"
        )

    if include_segmentation:
        try:
            segmentation_rows_by_item = supabase.get_segmentation_rows_for_items(
                dataset_item_ids
            )
        except Exception as error:
            logger.warning(
                "Segmentation enrichment failed for dataset %s: %s", dataset_id, error
            )
            warnings.append(
                "segmentation_enrichment_unavailable: could not load segmentation rows"
            )

    try:
        standardization_rows_by_item = supabase.get_standardization_rows_for_items(
            dataset_item_ids
        )
    except Exception as error:
        logger.warning(
            "Standardization enrichment failed for dataset %s: %s", dataset_id, error
        )
        warnings.append(
            "standardization_enrichment_unavailable: could not load LAS rows"
        )

    return (
        invocation_row,
        segmentation_rows_by_item,
        standardization_rows_by_item,
        warnings,
    )


def _process_download_request(
    supabase: SupabaseClient,
    storage: StorageClient,
    request_row: Dict[str, Any],
) -> None:
    request_id = int(request_row["id"])
    dataset_id = int(request_row["dataset_id"])
    try:
        dataset = supabase.get_dataset_with_items(dataset_id)
        if not dataset:
            _fail_request_safely(
                supabase, request_id, "dataset_not_found", f"Dataset {dataset_id} not found"
            )
            return
        if dataset.get("archived"):
            _fail_request_safely(
                supabase, request_id, "dataset_archived", f"Dataset {dataset_id} is archived"
            )
            return

        if not request_row.get("include_raw") and not request_row.get("include_segmentation"):
            _fail_request_safely(
                supabase,
                request_id,
                "invalid_request",
                "At least one artifact type must be requested",
            )
            return

        dataset_item_ids = [int(item["id"]) for item in (dataset.get("dataset_items") or [])]
        (
            invocation_row,
            segmentation_rows_by_item,
            standardization_rows_by_item,
            enrichment_warnings,
        ) = _safe_metadata_enrichment(
            supabase=supabase,
            dataset_id=dataset_id,
            dataset_item_ids=dataset_item_ids,
            include_segmentation=bool(request_row.get("include_segmentation")),
        )

        archive_root_name = archive_root(dataset_id)
        (
            sources,
            mapping_rows,
            missing_segmentation,
            missing_raw,
        ) = build_archive_sources(
            storage=storage,
            dataset=dataset,
            request_row=request_row,
            archive_root_name=archive_root_name,
            segmentation_rows_by_item=segmentation_rows_by_item,
        )

        if request_row.get("include_raw") and missing_raw:
            missing = ", ".join(str(item_id) for item_id in missing_raw)
            _fail_request_safely(
                supabase,
                request_id,
                "failed_missing_artifacts",
                f"Missing raw artifacts for dataset_item_ids: {missing}",
            )
            return

        if request_row.get("include_segmentation") and missing_segmentation:
            missing = ", ".join(str(item_id) for item_id in missing_segmentation)
            _fail_request_safely(
                supabase,
                request_id,
                "failed_missing_artifacts",
                f"Missing segmentation artifacts for dataset_item_ids: {missing}",
            )
            return

        if not sources:
            _fail_request_safely(
                supabase,
                request_id,
                "empty_archive",
                "No source files resolved for request",
            )
            return

        now = datetime.now(timezone.utc)
        archive_file_name = archive_filename(dataset_id)
        archive_key = f"{DOWNLOAD_KEY_PREFIX}/{dataset_id}/{request_id}/{archive_file_name}"
        archive_bucket = storage.bucket_name_download
        signed_url_expires_at = now + timedelta(seconds=DOWNLOAD_URL_EXPIRES_SECONDS)
        archive_expires_at = now + timedelta(days=DOWNLOAD_RETENTION_DAYS)

        metadata_model = build_metadata_model(
            dataset=dataset,
            request_row=request_row,
            archive_file_name=archive_file_name,
            archive_root_name=archive_root_name,
            mapping_rows=mapping_rows,
            generated_at=now,
            invocation_row=invocation_row,
            segmentation_rows_by_item=segmentation_rows_by_item,
            standardization_rows_by_item=standardization_rows_by_item,
            warnings=enrichment_warnings,
        )
        metadata_payload = metadata_model.model_dump(mode="json")
        datacite_payload = build_datacite_payload(dataset=dataset, generated_at=now)
        bibtex_text = build_bibtex_citation(dataset=dataset, generated_at=now)

        readme_text = build_readme(
            dataset=dataset,
            request_row=request_row,
            archive_file_name=archive_file_name,
            segmentation_model=metadata_model.processing.segmentation.used_model,
            workflow_name=metadata_model.processing.galaxy_workflow.workflow_name,
            generated_at=now,
        )
        license_text = build_license_note(
            dataset=dataset,
            segmentation_model=metadata_model.processing.segmentation.used_model,
            generated_at=now,
            include_segmentation=bool(request_row.get("include_segmentation")),
        )

        with tempfile.TemporaryDirectory(prefix="3dtrees-download-") as work_dir:
            archive_local_path = Path(work_dir) / archive_file_name
            archive_size = write_download_archive(
                storage=storage,
                archive_local_path=archive_local_path,
                archive_root_name=archive_root_name,
                readme_text=readme_text,
                license_text=license_text,
                bibtex_text=bibtex_text,
                datacite_payload=datacite_payload,
                metadata_payload=metadata_payload,
                sources=sources,
            )

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
            archive_filename=archive_file_name,
            archive_size_bytes=archive_size,
            signed_url=signed_url,
            signed_url_expires_at=signed_url_expires_at,
            expires_at=archive_expires_at,
            metadata=metadata_payload,
            finished_at=datetime.now(timezone.utc),
        )

        try:
            dataset_title = dataset.get("title") or f"Dataset {dataset_id}"
            send_download_ready_email(
                config=BrevoEmailConfig(
                    api_url=BREVO_API_URL,
                    api_key=BREVO_API_KEY,
                    sender_email=BREVO_SENDER_EMAIL,
                    sender_name=BREVO_SENDER_NAME,
                    reply_to_email=BREVO_REPLY_TO_EMAIL,
                    reply_to_name=BREVO_REPLY_TO_NAME,
                ),
                to_email=request_row["requester_email"],
                archive_filename=archive_file_name,
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
            logger.error("Email send failed for request %s: %s", request_id, email_error)
            try:
                supabase.update_download_request(
                    request_id,
                    status="failed_email",
                    failure_code="email_send_failed",
                    failure_reason=str(email_error)[:2000],
                    finished_at=datetime.now(timezone.utc),
                )
            except Exception as update_error:
                logger.error(
                    "Could not persist failed_email state for request %s: %s",
                    request_id,
                    update_error,
                )

    except Exception as error:
        logger.exception(
            "Failed processing download request %s for dataset %s",
            request_id,
            dataset_id,
        )
        _fail_request_safely(
            supabase=supabase,
            request_id=request_id,
            failure_code="archive_creation_failed",
            failure_reason=str(error),
        )


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
        except Exception as error:
            logger.warning("Failed cleanup for request %s: %s", request_id, error)

    return cleaned


def get_connected_clients() -> tuple[SupabaseClient, StorageClient]:
    supabase = SupabaseClient(SupabaseConfig())
    storage = StorageClient(StorageConfig())

    supabase.connect()
    if (not supabase.using_service_role) and supabase.email and supabase.password:
        try:
            supabase.authenticate_user(supabase.email, supabase.password)
        except Exception:
            logger.warning(
                "Supabase user authentication failed; proceeding with current credentials"
            )

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
    except Exception as error:
        logger.error("Download worker cycle failed: %s", error)
        stats["success"] = False
        stats["error"] = str(error)
        return stats


def run_continuous(interval: int = DOWNLOAD_WORKER_INTERVAL) -> None:
    cycle = 0
    while True:
        cycle += 1
        logger.info("Starting download worker cycle #%s", cycle)
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


__all__ = [
    "DOWNLOAD_WORKER_INTERVAL",
    "DOWNLOAD_URL_EXPIRES_SECONDS",
    "DOWNLOAD_RETENTION_DAYS",
    "DOWNLOAD_KEY_PREFIX",
    "_process_download_request",
    "_safe_metadata_enrichment",
    "get_connected_clients",
    "run_once",
    "run_continuous",
    "main",
]


if __name__ == "__main__":
    main()

