#!/usr/bin/env python3

import argparse
import json
import os
import sys
import textwrap
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_ALLOWED_LABELS = [
    "delta-datalayout",
    "delta-ddl",
    "delta-dml",
    "delta-docs",
    "delta-kernel",
    "delta-metadata",
    "delta-spark",
    "delta-standalone",
    "delta-table-maintenance",
    "delta-uniform",
    "dsv2",
    "flink",
    "infra",
    "kernel",
    "kernel-api-change",
    "kernel-spark",
    "oss-infra",
    "uc-spark",
]

DEFAULT_LABEL_COLORS = {
    "delta-datalayout": "0fbab0",
    "delta-ddl": "ff742c",
    "delta-dml": "b072bf",
    "delta-docs": "1014ab",
    "delta-kernel": "082b79",
    "delta-metadata": "d264d0",
    "delta-spark": "0b6019",
    "delta-standalone": "0F0E77",
    "delta-table-maintenance": "a69ce8",
    "delta-uniform": "c88dc3",
    "dsv2": "aaaaaa",
    "flink": "0e8a16",
    "infra": "BF3F7D",
    "kernel": "0e8a16",
    "kernel-api-change": "0BE0CE",
    "kernel-spark": "4B61F7",
    "oss-infra": "cc3f4d",
    "uc-spark": "a1c10a",
}

MAX_FILES = int(os.getenv("AI_LABEL_MAX_FILES", "100"))
MAX_COMMITS = int(os.getenv("AI_LABEL_MAX_COMMITS", "100"))
MAX_FILE_PATCH_CHARS = int(os.getenv("AI_LABEL_MAX_FILE_PATCH_CHARS", "4000"))
MAX_TOTAL_PATCH_CHARS = int(os.getenv("AI_LABEL_MAX_TOTAL_PATCH_CHARS", "120000"))


def parse_args():
    parser = argparse.ArgumentParser(description="Classify a GitHub PR and apply AI labels.")
    parser.add_argument("--repo", default=os.getenv("GITHUB_REPOSITORY"))
    parser.add_argument("--pr-number", default=os.getenv("PR_NUMBER"))
    parser.add_argument("--dry-run", action="store_true", default=is_truthy(os.getenv("DRY_RUN")))
    return parser.parse_args()


def is_truthy(value):
    return str(value).lower() in {"1", "true", "yes", "y", "on"}


def require_env(name):
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def github_request(token, method, path, data=None):
    url = f"https://api.github.com{path}"
    body = None if data is None else json.dumps(data).encode("utf-8")
    request = urllib.request.Request(url, data=body, method=method)
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    if body is not None:
        request.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(request) as response:
            response_body = response.read().decode("utf-8")
            if not response_body:
                return None
            return json.loads(response_body)
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {path} failed: {error.code} {error_body}") from error


def github_paginated(token, path, limit):
    results = []
    page = 1
    while len(results) < limit:
        separator = "&" if "?" in path else "?"
        page_path = f"{path}{separator}per_page=100&page={page}"
        batch = github_request(token, "GET", page_path)
        if not batch:
            break
        results.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return results[:limit]


def label_allowlist():
    configured_labels = os.getenv("AI_LABEL_ALLOWLIST")
    if not configured_labels:
        return DEFAULT_ALLOWED_LABELS
    return [label.strip() for label in configured_labels.split(",") if label.strip()]


def label_prefixes():
    configured_prefixes = os.getenv("AI_LABEL_PREFIXES", "ai:")
    return [prefix.strip() for prefix in configured_prefixes.split(",") if prefix.strip()]


def truncate_text(value, max_chars):
    if value is None:
        return ""
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "\n...[truncated]..."


def build_pr_context(token, repo, pr_number):
    owner_repo = urllib.parse.quote(repo, safe="/")
    pull = github_request(token, "GET", f"/repos/{owner_repo}/pulls/{pr_number}")
    issue = github_request(token, "GET", f"/repos/{owner_repo}/issues/{pr_number}")
    commits = github_paginated(token, f"/repos/{owner_repo}/pulls/{pr_number}/commits", MAX_COMMITS)
    files = github_paginated(token, f"/repos/{owner_repo}/pulls/{pr_number}/files", MAX_FILES)

    file_summaries = []
    patch_chars = 0
    for changed_file in files:
        patch = changed_file.get("patch") or ""
        remaining_patch_chars = max(0, MAX_TOTAL_PATCH_CHARS - patch_chars)
        patch = truncate_text(patch, min(MAX_FILE_PATCH_CHARS, remaining_patch_chars))
        patch_chars += len(patch)
        file_summaries.append(
            {
                "filename": changed_file.get("filename"),
                "status": changed_file.get("status"),
                "additions": changed_file.get("additions"),
                "deletions": changed_file.get("deletions"),
                "changes": changed_file.get("changes"),
                "patch": patch,
            }
        )

    return {
        "number": pull.get("number"),
        "title": pull.get("title"),
        "body": truncate_text(pull.get("body"), 12000),
        "base": pull.get("base", {}).get("ref"),
        "head": pull.get("head", {}).get("ref"),
        "draft": pull.get("draft"),
        "changed_file_count": pull.get("changed_files"),
        "existing_labels": [label["name"] for label in issue.get("labels", [])],
        "commits": [
            {
                "sha": commit.get("sha"),
                "message": commit.get("commit", {}).get("message", ""),
            }
            for commit in commits
        ],
        "files": file_summaries,
        "limits": {
            "max_files": MAX_FILES,
            "max_commits": MAX_COMMITS,
            "max_file_patch_chars": MAX_FILE_PATCH_CHARS,
            "max_total_patch_chars": MAX_TOTAL_PATCH_CHARS,
            "included_files": len(file_summaries),
            "included_commits": len(commits),
        },
    }


def classify_with_openai(pr_context, allowed_labels):
    api_key = require_env("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions")
    system_prompt = textwrap.dedent(
        """
        You classify GitHub pull requests. Return strict JSON only.
        Choose zero or more labels from the allowlist. Do not invent labels.

        Guidance:
        - Prefer the most specific Delta classification labels that describe the changed area.
        - Use delta-docs for documentation-only or documentation-heavy changes.
        - Use delta-dml for writes, updates, deletes, merge, or other DML behavior.
        - Use delta-ddl for table/schema creation, alteration, replacement, or drop behavior.
        - Use delta-datalayout for clustering, partitioning, layout, statistics, or data skipping.
        - Use delta-metadata for transaction log, metadata, protocol, checkpoint, or commit changes.
        - Use delta-table-maintenance for VACUUM, OPTIMIZE, compaction, cleanup, or repair behavior.
        - Use delta-uniform for UniForm/Iceberg/Hudi interoperability.
        - Use delta-spark for general Delta Spark behavior that is not covered by a narrower label.
        - Use dsv2 for DataSource V2 behavior.
        - Use flink for Flink connector changes.
        - Use kernel, delta-kernel, kernel-api-change, or kernel-spark for Delta Kernel areas.
        - Use delta-standalone for standalone connector changes.
        - Use uc-spark for Unity Catalog Spark integration.
        - Use infra or oss-infra for CI, release, build, packaging, dependency, or repository automation changes.
        """
    ).strip()
    user_prompt = json.dumps(
        {
            "allowed_labels": allowed_labels,
            "required_schema": {
                "labels": ["one or more allowed labels"],
                "reason": "short reason for audit logs",
            },
            "pull_request": pr_context,
        },
        indent=2,
        sort_keys=True,
    )
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(base_url, data=json.dumps(body).encode("utf-8"), method="POST")
    request.add_header("Authorization", f"Bearer {api_key}")
    request.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(request) as response:
            response_body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI request failed: {error.code} {error_body}") from error

    message = response_body["choices"][0]["message"]["content"]
    return parse_json_response(message)


def parse_json_response(value):
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        start = value.find("{")
        end = value.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(value[start : end + 1])


def create_missing_label(token, repo, label):
    owner_repo = urllib.parse.quote(repo, safe="/")
    data = {
        "name": label,
        "color": DEFAULT_LABEL_COLORS.get(label, "ededed"),
        "description": "Applied by AI PR Labeler",
    }
    try:
        github_request(token, "POST", f"/repos/{owner_repo}/labels", data)
    except RuntimeError as error:
        if "422" not in str(error):
            raise


def remove_label(token, repo, pr_number, label):
    owner_repo = urllib.parse.quote(repo, safe="/")
    label_path = urllib.parse.quote(label, safe="")
    github_request(token, "DELETE", f"/repos/{owner_repo}/issues/{pr_number}/labels/{label_path}")


def add_labels(token, repo, pr_number, labels):
    owner_repo = urllib.parse.quote(repo, safe="/")
    github_request(token, "POST", f"/repos/{owner_repo}/issues/{pr_number}/labels", {"labels": labels})


def selected_labels(classification, allowed_labels):
    allowed = set(allowed_labels)
    labels = classification.get("labels", [])
    if not isinstance(labels, list):
        raise RuntimeError("AI response field 'labels' must be a list.")
    return sorted({label for label in labels if isinstance(label, str) and label in allowed})


def stale_ai_labels(existing_labels, desired_labels, allowed_labels):
    prefixes = tuple(label_prefixes())
    allowed = set(allowed_labels)
    desired = set(desired_labels)
    return sorted(
        label
        for label in existing_labels
        if (label in allowed or (prefixes and label.startswith(prefixes))) and label not in desired
    )


def main():
    args = parse_args()
    if not args.repo:
        print("Missing repository. Set GITHUB_REPOSITORY or pass --repo.", file=sys.stderr)
        return 1
    if not args.pr_number:
        print("Missing PR number. Set PR_NUMBER or pass --pr-number.", file=sys.stderr)
        return 1

    token = require_env("GITHUB_TOKEN")
    allowed_labels = label_allowlist()
    pr_context = build_pr_context(token, args.repo, args.pr_number)
    classification = classify_with_openai(pr_context, allowed_labels)
    desired_labels = selected_labels(classification, allowed_labels)
    existing_labels = pr_context["existing_labels"]
    labels_to_add = sorted(set(desired_labels) - set(existing_labels))
    labels_to_remove = stale_ai_labels(existing_labels, desired_labels, allowed_labels)
    reason = classification.get("reason", "")

    print(f"AI label reason: {reason}")
    print(f"Desired labels: {', '.join(desired_labels) if desired_labels else '(none)'}")
    print(f"Labels to add: {', '.join(labels_to_add) if labels_to_add else '(none)'}")
    print(f"Labels to remove: {', '.join(labels_to_remove) if labels_to_remove else '(none)'}")

    if args.dry_run:
        print("DRY_RUN enabled; no labels changed.")
        return 0

    if is_truthy(os.getenv("AI_CREATE_MISSING_LABELS", "false")):
        for label in labels_to_add:
            create_missing_label(token, args.repo, label)

    for label in labels_to_remove:
        remove_label(token, args.repo, args.pr_number, label)

    if labels_to_add:
        add_labels(token, args.repo, args.pr_number, labels_to_add)

    return 0


if __name__ == "__main__":
    sys.exit(main())
