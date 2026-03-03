from datetime import datetime, timezone

from trees_api.app.connection_manager import ConnectionManager
from trees_api.core.config import AppConfig
from trees_api.routes.downloads.support.metadata_enrichment import (
    extract_model_from_parameters,
)
from trees_api.routes.downloads.support.notifier import _build_download_email_html


def test_download_email_html_escapes_user_content():
    html_body = _build_download_email_html(
        archive_filename='archive"name".zip',
        signed_url='https://example.com/download?q="x"&p=<y>',
        dataset_id=42,
        dataset_title='<img src=x onerror=alert(1)> "title"',
        expires_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert "<img" not in html_body
    assert "&lt;img src=x onerror=alert(1)&gt; &quot;title&quot;" in html_body
    assert "archive&quot;name&quot;.zip" in html_body
    assert 'href="https://example.com/download?q=&quot;x&quot;&amp;p=&lt;y&gt;"' in html_body


def test_extract_model_from_parameters_ignores_boolean_candidate_keys():
    parameters = {
        "use_model_cache": True,
        "model_path": "/models/segment-any-tree.ckpt",
    }

    model_value, source = extract_model_from_parameters(parameters)

    assert model_value == "/models/segment-any-tree.ckpt"
    assert source == "model_path"


def test_connection_manager_accepts_validated_config_after_singleton_init():
    manager = ConnectionManager()
    original_config = manager.config
    try:
        validated_config = AppConfig()
        validated_config.galaxy.url = "http://example.test"

        manager_reused = ConnectionManager(validated_config)

        assert manager_reused is manager
        assert manager.config is validated_config
    finally:
        manager.config = original_config
