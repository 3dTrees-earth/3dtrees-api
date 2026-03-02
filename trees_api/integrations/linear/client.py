"""
Linear API client for automated bug reporting.
"""

from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("uvicorn")

LINEAR_API_URL = "https://api.linear.app/graphql"
REQUEST_TIMEOUT = 10

TOOL_LABEL_MAPPING = {
    "3dtrees_sat": "bdeabf60-bdc9-4649-95be-67c80c21b013",
    "3dtrees_overviews": "929bdf62-9c9d-425e-8bd1-347b19701486",
    "3dtrees_py3dtiles": "cb5d12b4-4f77-4155-b9cc-6824085ae076",
    "3dtrees_standardization": "19f5804a-4d34-4cb0-9138-9566e3ccb19b",
    "3dtrees_tile_merge": "2dac5f17-1f5d-46e6-bb5b-971abdd46596",
}

BUG_LABEL_ID = "3cd77898-47b3-488e-8509-e51da0cba52f"
TRIAGE_STATUS_ID = "e1dd7922-10cb-44aa-bad6-91cfd2ed6091"

EXIT_CODE_MEANINGS = {
    1: "Application error",
    2: "Misuse of shell command",
    126: "Command not executable",
    127: "Command not found",
    128: "Invalid exit argument",
    130: "Terminated by Ctrl+C (SIGINT)",
    137: "Killed (SIGKILL) - likely OOM",
    139: "Segmentation fault (SIGSEGV)",
    143: "Terminated (SIGTERM)",
}


@dataclass
class FailedJob:
    tool_id: str
    tool_name: str
    exit_code: Optional[int]
    stderr: str
    stdout: str
    job_messages: List[str]


class LinearClient:
    """Resilient Linear API client for automated bug reporting."""

    def __init__(self, config):
        self.api_key = config.api_key
        self.team_id = config.team_id
        self.enabled = config.is_configured()
        if not self.enabled:
            logger.info("Linear client initialized but disabled (no API key or LINEAR_ENABLED=false)")

    def _make_request(self, query: str, variables: Dict[str, Any]) -> Optional[Dict]:
        if not self.enabled:
            return None
        try:
            response = requests.post(
                LINEAR_API_URL,
                headers={"Authorization": self.api_key, "Content-Type": "application/json"},
                json={"query": query, "variables": variables},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                logger.error("Linear API error: %s", data["errors"])
                return None
            return data.get("data")
        except requests.Timeout:
            logger.error("Linear API request timed out")
            return None
        except requests.RequestException as error:
            logger.error("Linear API request failed: %s", error)
            return None
        except Exception as error:
            logger.error("Unexpected error calling Linear API: %s", error)
            return None

    def _check_existing_issue(self, invocation_id: str) -> Optional[str]:
        query = """
        query SearchIssues($filter: IssueFilter) {
            issues(filter: $filter, first: 1) {
                nodes {
                    id
                    identifier
                    title
                }
            }
        }
        """
        variables = {
            "filter": {
                "team": {"id": {"eq": self.team_id}},
                "description": {"contains": invocation_id},
            }
        }
        result = self._make_request(query, variables)
        if result and result.get("issues", {}).get("nodes"):
            existing = result["issues"]["nodes"][0]
            logger.info("Issue already exists for invocation %s: %s", invocation_id, existing["identifier"])
            return existing["identifier"]
        return None

    def _get_tool_label(self, tool_id: str) -> Optional[str]:
        tool_name = tool_id.split("/")[-2] if "/" in tool_id else tool_id
        return TOOL_LABEL_MAPPING.get(tool_name)

    def _format_exit_code(self, exit_code: Optional[int]) -> str:
        if exit_code is None:
            return "unknown"
        meaning = EXIT_CODE_MEANINGS.get(exit_code, "")
        if meaning:
            return f"{exit_code} ({meaning})"
        return str(exit_code)

    def _build_issue_description(
        self,
        dataset_id: int,
        invocation_id: str,
        workflow_name: str,
        failed_jobs: List[FailedJob],
        messages: List[Dict],
    ) -> str:
        lines = [
            "## Automated Bug Report",
            "",
            f"**Dataset ID:** {dataset_id}",
            f"**Invocation ID:** `{invocation_id}`",
            f"**Workflow:** {workflow_name}",
            "",
        ]
        for index, job in enumerate(failed_jobs, 1):
            lines.extend(
                [
                    f"## Failed Tool {index}: {job.tool_name}",
                    "",
                    f"**Tool ID:** `{job.tool_id}`",
                    f"**Exit Code:** {self._format_exit_code(job.exit_code)}",
                    "",
                ]
            )
            if job.stderr:
                stderr_truncated = job.stderr[:1500]
                if len(job.stderr) > 1500:
                    stderr_truncated += "\n... (truncated)"
                lines.extend(["### Error Output (stderr)", "```", stderr_truncated, "```", ""])
            if job.job_messages:
                lines.extend(["### Job Messages", ""])
                for msg in job.job_messages[:5]:
                    lines.append(f"- {msg}")
                lines.append("")
        if messages:
            lines.extend(["## Workflow Messages", ""])
            for msg in messages[:10]:
                lines.append(f"- Step {msg.get('workflow_step_id', '?')}: {msg.get('reason', 'unknown')}")
            lines.append("")
        lines.extend(
            [
                "## Links",
                "",
                f"- [Dataset on 3DTrees](https://3dtrees.earth/datasets/{dataset_id})",
                f"- [Galaxy EU History](https://usegalaxy.eu/histories/view?id={invocation_id})",
                "",
                "---",
                "*This issue was created automatically by the status pooler.*",
            ]
        )
        return "\n".join(lines)

    def create_workflow_failure_issue(
        self,
        dataset_id: int,
        invocation_id: str,
        workflow_name: str,
        failed_jobs: List[FailedJob],
        messages: List[Dict],
    ) -> Optional[str]:
        if not self.enabled:
            logger.debug("Linear issue creation skipped (disabled)")
            return None
        try:
            existing = self._check_existing_issue(invocation_id)
            if existing:
                return existing
            if failed_jobs:
                first_tool = failed_jobs[0].tool_name
                exit_code = failed_jobs[0].exit_code
                title = f"🤖 Dataset {dataset_id}: {first_tool} failed"
                if exit_code is not None:
                    title += f" (exit {exit_code})"
                if len(failed_jobs) > 1:
                    title += f" +{len(failed_jobs) - 1} more"
            else:
                title = f"🤖 Dataset {dataset_id}: Workflow failed"
            description = self._build_issue_description(
                dataset_id=dataset_id,
                invocation_id=invocation_id,
                workflow_name=workflow_name,
                failed_jobs=failed_jobs,
                messages=messages,
            )
            label_ids = [BUG_LABEL_ID]
            for job in failed_jobs:
                tool_label = self._get_tool_label(job.tool_id)
                if tool_label and tool_label not in label_ids:
                    label_ids.append(tool_label)
            query = """
            mutation IssueCreate($input: IssueCreateInput!) {
                issueCreate(input: $input) {
                    success
                    issue {
                        id
                        identifier
                        url
                    }
                }
            }
            """
            variables = {
                "input": {
                    "teamId": self.team_id,
                    "title": title,
                    "description": description,
                    "stateId": TRIAGE_STATUS_ID,
                    "labelIds": label_ids,
                }
            }
            result = self._make_request(query, variables)
            if result and result.get("issueCreate", {}).get("success"):
                issue = result["issueCreate"]["issue"]
                logger.info("Created Linear issue %s for dataset %s", issue["identifier"], dataset_id)
                return issue["identifier"]
            logger.error("Failed to create Linear issue for dataset %s", dataset_id)
            return None
        except Exception as error:
            logger.error("Unexpected error creating Linear issue: %s", error)
            return None


def create_linear_client_if_enabled():
    try:
        from trees_api.core.config import LinearConfig

        config = LinearConfig()
        if config.is_configured():
            logger.info("Linear client initialized and enabled")
            return LinearClient(config)
        logger.info("Linear integration disabled (api_key=%s, enabled=%s)", bool(config.api_key), config.enabled)
        return None
    except Exception as error:
        logger.warning("Could not initialize Linear client: %s", error)
        return None


__all__ = ["LinearClient", "FailedJob", "create_linear_client_if_enabled"]

