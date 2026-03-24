from datetime import datetime, timezone

from trees_api.app.connection_manager import ConnectionManager
from trees_api.core.config import AppConfig
from trees_api.routes.downloads.support.metadata_enrichment import (
    build_bibtex_citation,
    build_datacite_payload,
    build_license_note,
    build_readme,
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


def test_build_datacite_payload_includes_uploaded_metadata():
    generated_at = datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc)
    dataset = {
        "id": 42,
        "title": "Black Forest TLS Plot",
        "description": "Dense terrestrial laser scanning point cloud.",
        "acquisition_date": "2025-10-04",
        "platform": "TLS",
        "sensor": "RIEGL VZ-400i",
        "scan_pattern": "multi-scan",
        "preprocessing_steps": ["registered", "noise filtered"],
        "coordinate_accuracy": "high",
        "coordinate_reference": "EPSG:25832",
        "point_attributes_description": "XYZ, intensity, return number",
        "dataset_authors": [
            {
                "first_name": "Ada",
                "last_name": "Lovelace",
                "organisation": "Forest Lab",
                "orcid": "0000-0001-2345-6789",
            }
        ],
        "dataset_funding": [
            {
                "funder_name": "DFG",
                "funder_identifier": "https://doi.org/10.13039/501100001659",
                "award_number": "AB-123",
                "award_title": "Forest structure",
            }
        ],
        "datacite": [{"doi": "10.1234/example.doi"}],
    }

    payload = build_datacite_payload(dataset, generated_at)

    assert payload["doi"] == "10.1234/example.doi"
    assert payload["creators"][0]["name"] == "Ada Lovelace"
    assert payload["creators"][0]["familyName"] == "Lovelace"
    assert payload["fundingReferences"][0]["funderName"] == "DFG"
    assert payload["dates"] == [{"date": "2025-10-04", "dateType": "Collected"}]
    assert payload["rightsList"][0]["rights"].startswith("Creative Commons Attribution 4.0")
    assert any(
        description["descriptionType"] == "TechnicalInfo"
        and "Coordinate reference: EPSG:25832" in description["description"]
        and "Preprocessing steps: registered, noise filtered" in description["description"]
        for description in payload["descriptions"]
    )


def test_build_readme_and_license_include_requested_citation_content():
    generated_at = datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc)
    dataset = {
        "id": 42,
        "title": "Black Forest TLS Plot",
        "dataset_authors": [{"first_name": "Ada", "last_name": "Lovelace"}],
        "datacite": [{"doi": "10.1234/example.doi"}],
    }
    request_row = {"id": 7, "include_raw": True, "include_segmentation": True}

    readme_text = build_readme(
        dataset=dataset,
        request_row=request_row,
        archive_file_name="3dt_42.zip",
        segmentation_model="segment-any-tree.ckpt",
        workflow_name="EndToEndPipeline",
        generated_at=generated_at,
    )
    license_text = build_license_note(
        dataset=dataset,
        segmentation_model="segment-any-tree.ckpt",
        generated_at=generated_at,
        include_segmentation=True,
    )

    assert "datacite.json" in readme_text
    assert "CITATION.bib" in readme_text
    assert "Plain-text citation (APA style)" in readme_text
    assert "cite both the dataset and the 3Dtrees processing platform" in readme_text
    assert "Creative Commons Attribution 4.0 International (CC BY 4.0)" in readme_text
    assert "Dataset citation" in license_text
    assert "3Dtrees citation:" in license_text


def test_build_readme_omits_3dtrees_citation_for_raw_only_downloads():
    generated_at = datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc)
    dataset = {
        "id": 42,
        "title": "Black Forest TLS Plot",
        "dataset_authors": [{"first_name": "Ada", "last_name": "Lovelace"}],
    }
    request_row = {"id": 7, "include_raw": True, "include_segmentation": False}

    readme_text = build_readme(
        dataset=dataset,
        request_row=request_row,
        archive_file_name="3dt_42.zip",
        segmentation_model=None,
        workflow_name=None,
        generated_at=generated_at,
    )
    license_text = build_license_note(
        dataset=dataset,
        segmentation_model=None,
        generated_at=generated_at,
        include_segmentation=False,
    )

    assert "Plain-text citation for 3Dtrees:" not in readme_text
    assert "cite both the dataset and the 3Dtrees processing platform" not in readme_text
    assert "3Dtrees citation:" not in license_text


def test_build_bibtex_citation_prefers_dataset_doi_url():
    generated_at = datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc)
    dataset = {
        "id": 42,
        "title": "Black Forest TLS Plot",
        "dataset_authors": [{"first_name": "Ada", "last_name": "Lovelace"}],
        "datacite": [{"doi": "https://doi.org/10.1234/example.doi"}],
    }

    bibtex = build_bibtex_citation(dataset, generated_at)

    assert "@dataset{3dtrees_dataset_42" in bibtex
    assert "author = {Lovelace, Ada}" in bibtex
    assert "doi = {10.1234/example.doi}" in bibtex
    assert "url = {https://doi.org/10.1234/example.doi}" in bibtex


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
