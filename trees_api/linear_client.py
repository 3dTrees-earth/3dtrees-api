"""
Linear API client for automated bug reporting.

This module provides a resilient client for creating Linear issues when
Galaxy workflows fail. It includes:
- Duplicate detection by invocation_id
- Tool-to-label mapping
- Graceful error handling (never blocks workflow sync)
- Timeout protection

Usage:
    from trees_api.linear_client import LinearClient
    from trees_api.config import LinearConfig
    
    config = LinearConfig()
    if config.is_configured():
        client = LinearClient(config)
        client.create_workflow_failure_issue(...)
"""

import logging
import requests
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger("uvicorn")

# Linear API endpoint
LINEAR_API_URL = "https://api.linear.app/graphql"

# Request timeout in seconds
REQUEST_TIMEOUT = 10

# Tool ID to Label ID mapping
# Labels from Linear: Bug, Tool → SAT, Tool → 3D Tiles, etc.
TOOL_LABEL_MAPPING = {
    "3dtrees_sat": "bdeabf60-bdc9-4649-95be-67c80c21b013",  # Tool → SAT
    "3dtrees_overviews": "929bdf62-9c9d-425e-8bd1-347b19701486",  # Tool → Overviews
    "3dtrees_py3dtiles": "cb5d12b4-4f77-4155-b9cc-6824085ae076",  # Tool → 3D Tiles
    "3dtrees_standardization": "19f5804a-4d34-4cb0-9138-9566e3ccb19b",  # Tool → Standardization
    "3dtrees_tile_merge": "2dac5f17-1f5d-46e6-bb5b-971abdd46596",  # Tool → Tiling/Merging
}

# Bug label ID (always applied)
BUG_LABEL_ID = "3cd77898-47b3-488e-8509-e51da0cba52f"

# Triage status ID
TRIAGE_STATUS_ID = "e1dd7922-10cb-44aa-bad6-91cfd2ed6091"

# Exit code meanings
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
    """Represents a failed job with diagnostic information."""
    tool_id: str
    tool_name: str
    exit_code: Optional[int]
    stderr: str
    stdout: str
    job_messages: List[str]


class LinearClient:
    """
    Resilient Linear API client for automated bug reporting.
    
    All methods are designed to fail gracefully - they log errors
    but never raise exceptions that could block workflow sync.
    """
    
    def __init__(self, config):
        """
        Initialize Linear client.
        
        Args:
            config: LinearConfig instance with api_key and team_id
        """
        self.api_key = config.api_key
        self.team_id = config.team_id
        self.enabled = config.is_configured()
        
        if not self.enabled:
            logger.info("Linear client initialized but disabled (no API key or LINEAR_ENABLED=false)")
    
    def _make_request(self, query: str, variables: Dict[str, Any]) -> Optional[Dict]:
        """
        Make a GraphQL request to Linear API with error handling.
        
        Returns:
            Response data dict, or None on error
        """
        if not self.enabled:
            return None
            
        try:
            response = requests.post(
                LINEAR_API_URL,
                headers={
                    "Authorization": self.api_key,
                    "Content-Type": "application/json",
                },
                json={"query": query, "variables": variables},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            
            data = response.json()
            if "errors" in data:
                logger.error(f"Linear API error: {data['errors']}")
                return None
            
            return data.get("data")
            
        except requests.Timeout:
            logger.error("Linear API request timed out")
            return None
        except requests.RequestException as e:
            logger.error(f"Linear API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling Linear API: {e}")
            return None
    
    def _check_existing_issue(self, invocation_id: str) -> Optional[str]:
        """
        Check if an issue already exists for this invocation.
        
        Returns:
            Issue identifier (e.g., "3DT-123") if exists, None otherwise
        """
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
            logger.info(f"Issue already exists for invocation {invocation_id}: {existing['identifier']}")
            return existing["identifier"]
        
        return None
    
    def _get_tool_label(self, tool_id: str) -> Optional[str]:
        """Get the Linear label ID for a tool."""
        # Extract tool name from full tool_id
        # e.g., "toolshed.g2.bx.psu.edu/.../3dtrees_overviews/1.2.0" -> "3dtrees_overviews"
        tool_name = tool_id.split("/")[-2] if "/" in tool_id else tool_id
        return TOOL_LABEL_MAPPING.get(tool_name)
    
    def _format_exit_code(self, exit_code: Optional[int]) -> str:
        """Format exit code with meaning."""
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
        """Build markdown description for the issue."""
        lines = [
            "## Automated Bug Report",
            "",
            f"**Dataset ID:** {dataset_id}",
            f"**Invocation ID:** `{invocation_id}`",
            f"**Workflow:** {workflow_name}",
            "",
        ]
        
        # Failed jobs section
        for i, job in enumerate(failed_jobs, 1):
            lines.extend([
                f"## Failed Tool {i}: {job.tool_name}",
                "",
                f"**Tool ID:** `{job.tool_id}`",
                f"**Exit Code:** {self._format_exit_code(job.exit_code)}",
                "",
            ])
            
            if job.stderr:
                stderr_truncated = job.stderr[:1500]
                if len(job.stderr) > 1500:
                    stderr_truncated += "\n... (truncated)"
                lines.extend([
                    "### Error Output (stderr)",
                    "```",
                    stderr_truncated,
                    "```",
                    "",
                ])
            
            if job.job_messages:
                lines.extend([
                    "### Job Messages",
                    "",
                ])
                for msg in job.job_messages[:5]:
                    lines.append(f"- {msg}")
                lines.append("")
        
        # Galaxy messages section
        if messages:
            lines.extend([
                "## Workflow Messages",
                "",
            ])
            for msg in messages[:10]:
                reason = msg.get("reason", "unknown")
                step_id = msg.get("workflow_step_id", "?")
                lines.append(f"- Step {step_id}: {reason}")
            lines.append("")
        
        # Links section
        lines.extend([
            "## Links",
            "",
            f"- [Dataset on 3DTrees](https://3dtrees.earth/datasets/{dataset_id})",
            f"- [Galaxy EU History](https://usegalaxy.eu/histories/view?id={invocation_id})",
            "",
            "---",
            "*This issue was created automatically by the status pooler.*",
        ])
        
        return "\n".join(lines)
    
    def create_workflow_failure_issue(
        self,
        dataset_id: int,
        invocation_id: str,
        workflow_name: str,
        failed_jobs: List[FailedJob],
        messages: List[Dict],
    ) -> Optional[str]:
        """
        Create a Linear issue for a workflow failure.
        
        This method is resilient - it will log errors but never raise
        exceptions that could block the status sync.
        
        Args:
            dataset_id: The dataset ID that failed
            invocation_id: Galaxy invocation ID
            workflow_name: Name of the workflow
            failed_jobs: List of FailedJob objects with diagnostic info
            messages: Galaxy workflow messages
            
        Returns:
            Issue identifier (e.g., "3DT-123") if created, None otherwise
        """
        if not self.enabled:
            logger.debug("Linear issue creation skipped (disabled)")
            return None
        
        try:
            # Check for existing issue
            existing = self._check_existing_issue(invocation_id)
            if existing:
                return existing
            
            # Build title
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
            
            # Build description
            description = self._build_issue_description(
                dataset_id=dataset_id,
                invocation_id=invocation_id,
                workflow_name=workflow_name,
                failed_jobs=failed_jobs,
                messages=messages,
            )
            
            # Collect labels
            label_ids = [BUG_LABEL_ID]  # Always add Bug label
            for job in failed_jobs:
                tool_label = self._get_tool_label(job.tool_id)
                if tool_label and tool_label not in label_ids:
                    label_ids.append(tool_label)
            
            # Create issue
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
                logger.info(f"Created Linear issue {issue['identifier']} for dataset {dataset_id}")
                return issue["identifier"]
            else:
                logger.error(f"Failed to create Linear issue for dataset {dataset_id}")
                return None
                
        except Exception as e:
            # Never let Linear errors block workflow sync
            logger.error(f"Unexpected error creating Linear issue: {e}")
            return None


def create_linear_client_if_enabled():
    """
    Factory function to create a LinearClient if enabled.
    
    Returns:
        LinearClient instance if configured and enabled, None otherwise
    """
    try:
        from trees_api.config import LinearConfig
        config = LinearConfig()
        if config.is_configured():
            return LinearClient(config)
        else:
            logger.debug("Linear integration not configured or disabled")
            return None
    except Exception as e:
        logger.warning(f"Could not initialize Linear client: {e}")
        return None
