from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from trees_api.routes.downloads.support.schemas import (
    ArchiveInfo,
    AttributionInfo,
    CitationInfo,
    DatasetInfo,
    DownloadArchiveMetadata,
    FileMappingEntry,
    GalaxyWorkflowInfo,
    ItemProcessingInfo,
    ProcessingInfo,
    RequestInfo,
    SegmentationInfo,
)

PROJECT_NAME = "3Dtrees"
PROJECT_URL = "https://3dtrees.earth"
DATASET_KIND = "3D point cloud dataset"
DEFAULT_RIGHTS = "Creative Commons Attribution 4.0 International (CC BY 4.0)"
DEFAULT_RIGHTS_URI = "https://creativecommons.org/licenses/by/4.0/"


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dataset_url(dataset_id: Any) -> str:
    return f"{PROJECT_URL}/datasets/{dataset_id}"


def _normalize_doi(value: Optional[str]) -> Optional[str]:
    doi = _clean_text(value)
    if not doi:
        return None
    lowered = doi.lower()
    if lowered.startswith("https://doi.org/"):
        return doi[16:]
    if lowered.startswith("http://doi.org/"):
        return doi[15:]
    if lowered.startswith("doi:"):
        return doi[4:].strip()
    return doi


def _doi_url(doi: Optional[str]) -> Optional[str]:
    normalized = _normalize_doi(doi)
    if not normalized:
        return None
    return f"https://doi.org/{normalized}"


def _get_dataset_authors(dataset: Dict[str, Any]) -> List[Dict[str, Any]]:
    authors = dataset.get("dataset_authors") or []
    return [author for author in authors if isinstance(author, dict)]


def _get_dataset_funding(dataset: Dict[str, Any]) -> List[Dict[str, Any]]:
    funding = dataset.get("dataset_funding") or []
    return [row for row in funding if isinstance(row, dict)]


def _get_dataset_doi(dataset: Dict[str, Any]) -> Optional[str]:
    rows = dataset.get("datacite") or []
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                doi = _normalize_doi(row.get("doi"))
                if doi:
                    return doi
    return None


def _author_display_name(author: Dict[str, Any]) -> Optional[str]:
    first_name = _clean_text(author.get("first_name"))
    last_name = _clean_text(author.get("last_name"))
    organisation = _clean_text(author.get("organisation"))
    if first_name and last_name:
        return f"{first_name} {last_name}"
    if last_name:
        return last_name
    if first_name:
        return first_name
    return organisation


def _author_bibtex_name(author: Dict[str, Any]) -> Optional[str]:
    first_name = _clean_text(author.get("first_name"))
    last_name = _clean_text(author.get("last_name"))
    organisation = _clean_text(author.get("organisation"))
    if last_name and first_name:
        return f"{last_name}, {first_name}"
    if last_name:
        return last_name
    if first_name:
        return first_name
    return organisation


def _citation_creator_text(authors: List[Dict[str, Any]]) -> str:
    names = [name for name in (_author_display_name(author) for author in authors) if name]
    if not names:
        return PROJECT_NAME
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} & {names[1]}"
    return f"{', '.join(names[:-1])}, & {names[-1]}"


def build_dataset_citation_text(dataset: Dict[str, Any], generated_at: datetime) -> str:
    dataset_id = dataset.get("id")
    title = dataset.get("title") or f"Dataset {dataset_id}"
    doi = _get_dataset_doi(dataset)
    doi_url = _doi_url(doi)
    authors = _get_dataset_authors(dataset)
    creator_text = _citation_creator_text(authors)
    year = generated_at.year
    if doi_url:
        return f"{creator_text} ({year}). {title} [Dataset]. {PROJECT_NAME}. {doi_url}"
    access_date = generated_at.date().isoformat()
    return (
        f"{creator_text} ({year}). {title} [Dataset]. {PROJECT_NAME}. "
        f"Retrieved {access_date}, from {_dataset_url(dataset_id)}"
    )


def build_project_citation_text(generated_at: datetime) -> str:
    return f"{PROJECT_NAME}. ({generated_at.year}). {PROJECT_NAME} processing platform. {PROJECT_URL}"


def build_bibtex_citation(dataset: Dict[str, Any], generated_at: datetime) -> str:
    dataset_id = dataset.get("id")
    title = dataset.get("title") or f"Dataset {dataset_id}"
    doi = _get_dataset_doi(dataset)
    doi_url = _doi_url(doi)
    access_date = generated_at.date().isoformat()
    authors = _get_dataset_authors(dataset)
    author_value = " and ".join(
        name for name in (_author_bibtex_name(author) for author in authors) if name
    ) or PROJECT_NAME

    fields = [
        ("author", author_value),
        ("title", title),
        ("year", str(generated_at.year)),
        ("publisher", PROJECT_NAME),
        ("type", DATASET_KIND),
        ("url", doi_url or _dataset_url(dataset_id)),
        ("urldate", access_date),
    ]
    if doi:
        fields.append(("doi", doi))

    body = ",\n".join(f"  {key} = {{{value}}}" for key, value in fields)
    return f"@dataset{{3dtrees_dataset_{dataset_id},\n{body}\n}}\n"


def build_datacite_payload(
    dataset: Dict[str, Any],
    generated_at: datetime,
) -> Dict[str, Any]:
    dataset_id = dataset.get("id")
    title = dataset.get("title") or f"Dataset {dataset_id}"
    doi = _get_dataset_doi(dataset)
    dataset_url = _dataset_url(dataset_id)
    authors = _get_dataset_authors(dataset)
    funding_rows = _get_dataset_funding(dataset)

    creators: List[Dict[str, Any]] = []
    for author in authors:
        name = _author_display_name(author)
        if not name:
            continue
        creator: Dict[str, Any] = {"name": name}
        family_name = _clean_text(author.get("last_name"))
        given_name = _clean_text(author.get("first_name"))
        organisation = _clean_text(author.get("organisation"))
        orcid = _clean_text(author.get("orcid"))
        if family_name:
            creator["familyName"] = family_name
        if given_name:
            creator["givenName"] = given_name
        if organisation:
            creator["affiliation"] = [{"name": organisation}]
        if orcid:
            creator["nameIdentifiers"] = [
                {
                    "nameIdentifier": f"https://orcid.org/{orcid}",
                    "nameIdentifierScheme": "ORCID",
                    "schemeUri": "https://orcid.org",
                }
            ]
        creators.append(creator)

    if not creators:
        creators = [{"name": PROJECT_NAME}]

    descriptions: List[Dict[str, str]] = []
    description = _clean_text(dataset.get("description"))
    if description:
        descriptions.append(
            {"description": description, "descriptionType": "Abstract"}
        )

    technical_lines: List[str] = []
    metadata_fields = [
        ("Acquisition date", dataset.get("acquisition_date")),
        ("Platform", dataset.get("platform")),
        ("Sensor", dataset.get("sensor")),
        ("Scan pattern", dataset.get("scan_pattern")),
        ("Coordinate accuracy", dataset.get("coordinate_accuracy")),
        ("Coordinate reference", dataset.get("coordinate_reference")),
        ("Point attributes description", dataset.get("point_attributes_description")),
    ]
    for label, raw_value in metadata_fields:
        value = _clean_text(raw_value)
        if value:
            technical_lines.append(f"{label}: {value}")

    preprocessing_steps = dataset.get("preprocessing_steps")
    if isinstance(preprocessing_steps, list):
        cleaned_steps = [_clean_text(step) for step in preprocessing_steps]
        cleaned_steps = [step for step in cleaned_steps if step]
        if cleaned_steps:
            technical_lines.append(
                f"Preprocessing steps: {', '.join(cleaned_steps)}"
            )

    if technical_lines:
        descriptions.append(
            {
                "description": "\n".join(technical_lines),
                "descriptionType": "TechnicalInfo",
            }
        )

    subjects: List[Dict[str, str]] = []
    for label, raw_value in (
        ("Platform", dataset.get("platform")),
        ("Sensor", dataset.get("sensor")),
        ("Scan pattern", dataset.get("scan_pattern")),
    ):
        value = _clean_text(raw_value)
        if value:
            subjects.append({"subject": f"{label}: {value}"})

    funding_references: List[Dict[str, str]] = []
    for row in funding_rows:
        reference: Dict[str, str] = {}
        funder_name = _clean_text(row.get("funder_name"))
        funder_identifier = _clean_text(row.get("funder_identifier"))
        award_number = _clean_text(row.get("award_number"))
        award_title = _clean_text(row.get("award_title"))
        if not funder_name:
            continue
        reference["funderName"] = funder_name
        if funder_identifier:
            reference["funderIdentifier"] = funder_identifier
        if award_number:
            reference["awardNumber"] = award_number
        if award_title:
            reference["awardTitle"] = award_title
        funding_references.append(reference)

    payload: Dict[str, Any] = {
        "types": {
            "resourceType": DATASET_KIND,
            "resourceTypeGeneral": "Dataset",
        },
        "creators": creators,
        "titles": [{"title": title}],
        "publisher": PROJECT_NAME,
        "publicationYear": generated_at.year,
        "url": dataset_url,
        "rightsList": [
            {
                "rights": DEFAULT_RIGHTS,
                "rightsUri": DEFAULT_RIGHTS_URI,
            }
        ],
        "formats": ["application/zip"],
        "descriptions": descriptions,
    }
    if doi:
        payload["doi"] = doi
        payload["identifiers"] = [{"identifier": doi, "identifierType": "DOI"}]
    if subjects:
        payload["subjects"] = subjects
    if funding_references:
        payload["fundingReferences"] = funding_references
    acquisition_date = _clean_text(dataset.get("acquisition_date"))
    if acquisition_date:
        payload["dates"] = [{"date": acquisition_date, "dateType": "Collected"}]

    return payload


def extract_model_from_parameters(
    node: Any,
    path: str = "",
) -> Tuple[Optional[str], Optional[str]]:
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
            if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                if (
                    key_lower in candidate_keys
                    or "model" in key_lower
                    or "checkpoint" in key_lower
                ):
                    value_str = str(value).strip()
                    if value_str:
                        return value_str, child_path
            model_value, source = extract_model_from_parameters(value, child_path)
            if model_value:
                return model_value, source
    elif isinstance(node, list):
        for idx, value in enumerate(node):
            child_path = f"{path}[{idx}]" if path else f"[{idx}]"
            model_value, source = extract_model_from_parameters(value, child_path)
            if model_value:
                return model_value, source

    return None, None


def build_readme(
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_file_name: str,
    segmentation_model: Optional[str],
    workflow_name: Optional[str],
    generated_at: Optional[datetime] = None,
) -> str:
    title = dataset.get("title") or f"Dataset {dataset.get('id')}"
    generated_at = generated_at or datetime.now(timezone.utc)
    created = generated_at.isoformat()
    dataset_id = dataset.get("id")
    doi = _get_dataset_doi(dataset)
    include_raw = bool(request_row.get("include_raw"))
    include_segmentation = bool(request_row.get("include_segmentation"))
    raw_line = "yes" if include_raw else "no"
    segmentation_line = "yes" if include_segmentation else "no"
    model_line = segmentation_model or "not available in current DB metadata"
    workflow_line = workflow_name or "not available"
    dataset_citation = build_dataset_citation_text(dataset, generated_at)
    rights_line = f"{DEFAULT_RIGHTS} ({DEFAULT_RIGHTS_URI})"
    software_citation_block = ""
    if include_segmentation:
        software_citation_block = (
            "### Processed outputs\n"
            "This archive includes 3Dtrees-generated segmentation outputs. "
            "If you publish or otherwise reuse these derived results, cite both the dataset and the 3Dtrees processing platform.\n\n"
            "Plain-text citation for 3Dtrees:\n"
            f"{build_project_citation_text(generated_at)}\n\n"
        )
    return (
        "# 3Dtrees Dataset Download\n\n"
        "## Dataset\n"
        f"- Dataset ID: {dataset_id}\n"
        f"- Title: {title}\n"
        f"- DOI: {doi or 'not assigned'}\n"
        f"- Download request ID: {request_row.get('id')}\n"
        f"- Generated at (UTC): {created}\n"
        f"- Includes raw data: {raw_line}\n"
        f"- Includes segmentation data: {segmentation_line}\n\n"
        "## Processing summary\n"
        f"- Galaxy workflow: {workflow_line}\n"
        f"- Segmentation model used: {model_line}\n\n"
        "## Citation\n"
        "This archive includes machine-readable citation metadata in `datacite.json` and a BibTeX entry in `CITATION.bib`.\n\n"
        "### Dataset\n"
        "Plain-text citation (APA style):\n"
        f"{dataset_citation}\n\n"
        f"{software_citation_block}"
        "## License\n"
        f"- License: {rights_line}\n"
        "- Please retain attribution to the dataset creators and 3Dtrees when required by your use of the archive contents.\n\n"
        "## Archive structure\n"
        "- data/raw/: raw files if requested\n"
        "- data/segmentation/: segmentation files if requested\n"
        "- datacite.json: DataCite-style machine-readable citation and metadata export\n"
        "- CITATION.bib: BibTeX citation for the dataset\n"
        "- metadata.json: machine-readable metadata and filename mapping\n"
        "- LICENSE.txt: license notice, attribution, and citation guidance\n\n"
        "## Naming convention\n"
        f"- Archive filename: {archive_file_name}\n"
        "- Raw file: 3dtree_{dataset_id}_{item_id}_raw.{ext}\n"
        "- Segmentation file: 3dtree_{dataset_id}_{item_id}_segmentation.{ext}\n\n"
        "## Dataset page\n"
        f"- {_dataset_url(dataset_id)}\n"
    )


def build_license_note(
    dataset: Dict[str, Any],
    segmentation_model: Optional[str],
    generated_at: Optional[datetime] = None,
    include_segmentation: bool = False,
) -> str:
    generated_at = generated_at or datetime.now(timezone.utc)
    dataset_id = dataset.get("id")
    title = dataset.get("title") or f"Dataset {dataset_id}"
    access_date = generated_at.date().isoformat()
    model_line = segmentation_model or "not available in current DB metadata"
    dataset_citation = build_dataset_citation_text(dataset, generated_at)
    processed_note = ""
    if include_segmentation:
        processed_note = (
            "Processed outputs note\n"
            "This archive contains 3Dtrees-generated segmentation outputs. Reuse of those derived outputs should cite both the dataset and the 3Dtrees platform.\n"
            f"- 3Dtrees citation: {build_project_citation_text(generated_at)}\n\n"
        )
    return (
        "3Dtrees License, Citation, and Attribution\n"
        "==========================================\n\n"
        "License note\n"
        f"This archive is distributed under {DEFAULT_RIGHTS}.\n"
        f"License URL: {DEFAULT_RIGHTS_URI}\n"
        "Use the dataset page as the authoritative landing page for the dataset record.\n\n"
        "Attribution\n"
        "When reusing this data, include at minimum:\n"
        f"- Project: {PROJECT_NAME}\n"
        f"- Dataset ID: {dataset_id}\n"
        f"- Dataset title: {title}\n"
        f"- Access date: {access_date}\n"
        f"- URL: {_dataset_url(dataset_id)}\n\n"
        "Segmentation model note\n"
        f"- Model used (if available): {model_line}\n\n"
        "Dataset citation\n"
        f"{dataset_citation}\n\n"
        f"{processed_note}"
    )


def build_metadata_model(
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_file_name: str,
    archive_root_name: str,
    mapping_rows: List[Dict[str, Any]],
    generated_at: datetime,
    invocation_row: Optional[Dict[str, Any]],
    segmentation_rows_by_item: Dict[int, Dict[str, Any]],
    standardization_rows_by_item: Dict[int, Dict[str, Any]],
    warnings: Optional[List[str]] = None,
) -> DownloadArchiveMetadata:
    dataset_id = int(dataset["id"])
    title = dataset.get("title") or f"Dataset {dataset_id}"
    year = generated_at.year
    access_date = generated_at.date().isoformat()
    invocation_parameters = (invocation_row or {}).get("parameters") or {}
    used_model, model_source = extract_model_from_parameters(invocation_parameters)

    processing_items: List[ItemProcessingInfo] = []
    for item in dataset.get("dataset_items") or []:
        item_id = int(item["id"])
        seg_row = segmentation_rows_by_item.get(item_id) or {}
        std_row = standardization_rows_by_item.get(item_id) or {}
        processing_items.append(
            ItemProcessingInfo(
                dataset_item_id=item_id,
                segmentation_process_duration_minutes=seg_row.get(
                    "segmentation_process_duration_minutes"
                ),
                standardization_process_duration_minutes=std_row.get(
                    "standard_process_duration_minutes"
                ),
                coordinate_reference=std_row.get("coordinate_reference"),
            )
        )

    return DownloadArchiveMetadata(
        schema_version="1.0.0",
        archive=ArchiveInfo(
            name=archive_file_name,
            root_folder=archive_root_name,
            dataset_id=dataset_id,
            generated_at_utc=generated_at.isoformat(),
        ),
        dataset=DatasetInfo(
            id=dataset_id,
            uuid=dataset.get("uuid"),
            title=title,
            visibility=dataset.get("visibility"),
            archived=bool(dataset.get("archived")),
            dataset_url=_dataset_url(dataset_id),
        ),
        request=RequestInfo(
            download_request_id=int(request_row["id"]),
            include_raw=bool(request_row.get("include_raw")),
            include_segmentation=bool(request_row.get("include_segmentation")),
        ),
        processing=ProcessingInfo(
            galaxy_workflow=GalaxyWorkflowInfo(
                invocation_id=(invocation_row or {}).get("invocation_id"),
                workflow_name=(invocation_row or {}).get("workflow_name"),
                status=(invocation_row or {}).get("status"),
                started_at=(invocation_row or {}).get("started_at"),
                finished_at=(invocation_row or {}).get("finished_at"),
            ),
            segmentation=SegmentationInfo(
                used_model=used_model,
                model_source=model_source,
            ),
            items=processing_items,
            warnings=warnings or [],
        ),
        files=[FileMappingEntry.model_validate(row) for row in mapping_rows],
        attribution=AttributionInfo(
            project="3Dtrees",
            required_fields=[
                "project",
                "dataset_id",
                "dataset_title",
                "access_date",
                "dataset_url",
            ],
            dataset_id=dataset_id,
            dataset_title=title,
            access_date=access_date,
            dataset_url=_dataset_url(dataset_id),
        ),
        citation=CitationInfo(
            recommended=build_dataset_citation_text(dataset, generated_at)
        ),
    )


__all__ = [
    "extract_model_from_parameters",
    "build_bibtex_citation",
    "build_datacite_payload",
    "build_dataset_citation_text",
    "build_readme",
    "build_license_note",
    "build_project_citation_text",
    "build_metadata_model",
]

