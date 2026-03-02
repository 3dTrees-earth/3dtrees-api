from trees_api.routes.downloads.worker import (
    _process_download_request,
    _safe_metadata_enrichment,
)


class _NoopStorage:
    bucket_name_download = "downloads"


def test_process_download_request_sets_failed_on_unexpected_exception():
    class _FakeSupabase:
        def __init__(self):
            self.updates = []

        def get_dataset_with_items(self, dataset_id: int):
            raise RuntimeError("boom during dataset lookup")

        def update_download_request(self, request_id: int, **updates):
            self.updates.append({"id": request_id, **updates})
            return updates

    supabase = _FakeSupabase()
    request_row = {"id": 77, "dataset_id": 123, "include_raw": True, "include_segmentation": False}
    _process_download_request(supabase, _NoopStorage(), request_row)

    assert supabase.updates, "Expected terminal state update when processing fails"
    latest = supabase.updates[-1]
    assert latest["status"] == "failed"
    assert latest["failure_code"] == "archive_creation_failed"


def test_safe_metadata_enrichment_is_non_blocking():
    class _FailingSupabase:
        def get_current_workflow_invocation_for_dataset(self, dataset_id: int):
            raise RuntimeError("workflow lookup failed")

        def get_segmentation_rows_for_items(self, dataset_item_ids):
            raise RuntimeError("segmentation lookup failed")

        def get_standardization_rows_for_items(self, dataset_item_ids):
            raise RuntimeError("las lookup failed")

    invocation_row, segmentation_rows, standardization_rows, warnings = _safe_metadata_enrichment(
        supabase=_FailingSupabase(),
        dataset_id=9,
        dataset_item_ids=[1, 2, 3],
        include_segmentation=True,
    )

    assert invocation_row is None
    assert segmentation_rows == {}
    assert standardization_rows == {}
    assert len(warnings) == 3

