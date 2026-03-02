from trees_api.routes.downloads.support.source_resolution import (
    extract_storage_key_from_url,
    resolve_segmentation_key,
)


class _FakeStorage:
    bucket_name_products = "3dtrees-products"

    def __init__(self, existing_keys: set[str]):
        self._existing_keys = existing_keys

    def file_exists(self, key: str, bucket: str | None = None) -> bool:
        return bool(bucket) and key in self._existing_keys


def test_extract_storage_key_from_url_supports_common_formats():
    bucket = "3dtrees-products"

    assert (
        extract_storage_key_from_url(
            "s3://3dtrees-products/123/segmentation/88.laz", bucket
        )
        == "123/segmentation/88.laz"
    )
    assert (
        extract_storage_key_from_url(
            "http://localhost:9500/3dtrees-products/123/segmentation/88.laz", bucket
        )
        == "123/segmentation/88.laz"
    )
    assert (
        extract_storage_key_from_url(
            "https://3dtrees-products.s3.example.com/123/segmentation/88.laz", bucket
        )
        == "123/segmentation/88.laz"
    )
    assert extract_storage_key_from_url("123/segmentation/88.laz", bucket) == (
        "123/segmentation/88.laz"
    )


def test_resolve_segmentation_key_prefers_db_url_before_fallback():
    storage = _FakeStorage(
        existing_keys={
            "from/db/url/1.laz",
            "42/segmentation/5.laz",
        }
    )

    assert (
        resolve_segmentation_key(
            storage=storage,  # type: ignore[arg-type]
            dataset_id=42,
            dataset_item_id=5,
            segmentation_row={"url": "s3://3dtrees-products/from/db/url/1.laz"},
        )
        == "from/db/url/1.laz"
    )

    # If DB URL key does not exist, pattern fallback still resolves.
    assert (
        resolve_segmentation_key(
            storage=storage,  # type: ignore[arg-type]
            dataset_id=42,
            dataset_item_id=5,
            segmentation_row={"url": "s3://3dtrees-products/missing/seg.laz"},
        )
        == "42/segmentation/5.laz"
    )

