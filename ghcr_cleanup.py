#!/usr/bin/env python3
"""
GHCR Package Cleanup Service
Cleans up unused Docker images from GitHub Container Registry
"""

import os
import sys
import json
import logging
import time
import re
import base64
from datetime import datetime, timezone
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict

import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import schedule


# Configure JSON logging for Loki/Grafana
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add extra fields if present
        if hasattr(record, 'package'):
            log_obj['package'] = record.package
        if hasattr(record, 'organization'):
            log_obj['organization'] = record.organization
        if hasattr(record, 'repository'):
            log_obj['repository'] = record.repository
        if hasattr(record, 'action'):
            log_obj['action'] = record.action
        if hasattr(record, 'digest'):
            log_obj['digest'] = record.digest
        if hasattr(record, 'tag'):
            log_obj['tag'] = record.tag
        if hasattr(record, 'reason'):
            log_obj['reason'] = record.reason

        return json.dumps(log_obj)


# Setup logging
logger = logging.getLogger('ghcr_cleanup')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

# Prometheus metrics
cleanup_runs_total = Counter('ghcr_cleanup_runs_total', 'Total number of cleanup runs')
cleanup_errors_total = Counter('ghcr_cleanup_errors_total', 'Total number of cleanup errors', ['error_type'])
packages_processed = Counter('ghcr_packages_processed_total', 'Total packages processed')
digests_deleted = Counter('ghcr_digests_deleted_total', 'Digests deleted', ['package', 'organization'])
tags_deleted = Counter('ghcr_tags_deleted_total', 'Tagged versions deleted', ['package', 'organization'])
cleanup_duration = Histogram('ghcr_cleanup_duration_seconds', 'Cleanup duration')
active_packages = Gauge('ghcr_active_packages', 'Number of active packages', ['organization'])
package_versions = Gauge('ghcr_package_versions', 'Number of versions per package', ['package', 'organization'])


@dataclass
class PackageVersion:
    id: int
    name: str  # This is the digest
    tags: List[str]
    created_at: str
    updated_at: str


class GHCRClient:
    """Client for interacting with GitHub Container Registry and API"""

    def __init__(self, token: str):
        self.token = token
        self.github_api_base = "https://api.github.com"
        self.ghcr_base = "https://ghcr.io"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {token}',
            'X-GitHub-Api-Version': '2022-11-28'
        })

    def list_user_packages(self, username: str, package_type: str = "container") -> List[Dict]:
        """List all packages for a user"""
        packages = []
        page = 1

        while True:
            url = f"{self.github_api_base}/users/{username}/packages"
            params = {'package_type': package_type, 'per_page': 100, 'page': page}

            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            packages.extend(data)
            page += 1

        return packages

    def list_user_repositories(self, username: str) -> List[Dict]:
        """List all repositories for a user with jenkins-kubernetes topic"""
        repos = []
        page = 1

        while True:
            url = f"{self.github_api_base}/users/{username}/repos"
            params = {'per_page': 100, 'page': page}

            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            # Filter to repos with jenkins-kubernetes topic
            for repo in data:
                if 'jenkins-kubernetes' in repo.get('topics', []):
                    repos.append(repo)

            page += 1

        return repos

    def get_package_env(self, owner: str, repo: str) -> Optional[Dict[str, str]]:
        """Get package.env file from repository"""
        try:
            url = f"{self.github_api_base}/repos/{owner}/{repo}/contents/package.env"
            response = self.session.get(url)
            response.raise_for_status()

            content = base64.b64decode(response.json()['content']).decode('utf-8')

            # Parse env file
            env_vars = {}
            for line in content.split('\n'):
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value

            return env_vars
        except Exception as e:
            logger.debug(f"Could not get package.env for {owner}/{repo}: {e}")
            return None

    def list_package_versions(self, owner: str, package_type: str, package_name: str) -> List[PackageVersion]:
        """List all versions of a package"""
        versions = []
        page = 1

        while True:
            url = f"{self.github_api_base}/users/{owner}/packages/{package_type}/{package_name}/versions"
            params = {'per_page': 100, 'page': page}

            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            for v in data:
                tags = v.get('metadata', {}).get('container', {}).get('tags', [])
                versions.append(PackageVersion(
                    id=v['id'],
                    name=v['name'],
                    tags=tags,
                    created_at=v['created_at'],
                    updated_at=v['updated_at']
                ))

            page += 1

        return versions

    def delete_package_version(self, owner: str, package_type: str, package_name: str, version_id: int) -> bool:
        """Delete a package version"""
        url = f"{self.github_api_base}/users/{owner}/packages/{package_type}/{package_name}/versions/{version_id}"

        response = self.session.delete(url)
        return response.status_code == 204

    def list_repository_branches(self, owner: str, repo: str) -> List[str]:
        """List all branches in a repository"""
        branches = []
        page = 1

        while True:
            url = f"{self.github_api_base}/repos/{owner}/{repo}/branches"
            params = {'per_page': 100, 'page': page}

            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            branches.extend([b['name'] for b in data])
            page += 1

        return branches

    def list_repository_tags(self, owner: str, repo: str) -> List[str]:
        """List all git tags in a repository"""
        tags = []
        page = 1

        while True:
            url = f"{self.github_api_base}/repos/{owner}/{repo}/tags"
            params = {'per_page': 100, 'page': page}

            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            tags.extend([t['name'] for t in data])
            page += 1

        return tags

    def list_repository_pull_requests(self, owner: str, repo: str) -> List[int]:
        """List all open pull request numbers"""
        prs = []
        page = 1

        while True:
            url = f"{self.github_api_base}/repos/{owner}/{repo}/pulls"
            params = {'state': 'open', 'per_page': 100, 'page': page}

            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            prs.extend([pr['number'] for pr in data])
            page += 1

        return prs

    def get_ghcr_token(self, owner: str, package_name: str) -> str:
        """Get a bearer token for GHCR access"""
        scope = f"repository:{owner}/{package_name}:pull"
        url = f"{self.ghcr_base}/token"

        response = requests.get(
            url,
            params={'scope': scope},
            headers={'Authorization': f'Bearer {self.token}'}
        )
        response.raise_for_status()

        return response.json()['token']

    def get_manifest(self, owner: str, package_name: str, token: str, digest_or_tag: str) -> Dict:
        """Get manifest information from GHCR"""
        accept_header = ', '.join([
            'application/vnd.oci.image.manifest.v1+json',
            'application/vnd.oci.image.index.v1+json',
            'application/vnd.docker.distribution.manifest.v2+json',
            'application/vnd.docker.distribution.manifest.list.v2+json',
            'application/vnd.docker.distribution.manifest.v1+json'
        ])

        url = f"{self.ghcr_base}/v2/{owner}/{package_name}/manifests/{digest_or_tag}"
        response = requests.get(
            url,
            headers={
                'Accept': accept_header,
                'Authorization': f'Bearer {token}'
            }
        )
        response.raise_for_status()

        return response.json()


class GHCRCleanup:
    """Main cleanup orchestrator"""

    def __init__(self, client: GHCRClient, dry_run: bool = False):
        self.client = client
        self.dry_run = dry_run
        self.pr_pattern = re.compile(r'^pr-(\d+)$')
        self.arch_pattern = re.compile(r'^(.+)-(amd64|arm64|arm|386|ppc64le|s390x|riscv64)$')

    def build_package_repo_mapping(self, username: str) -> Dict[str, str]:
        """Build a mapping of package names to repository names"""
        mapping = {}

        try:
            repos = self.client.list_user_repositories(username)
            logger.info(f"Found {len(repos)} repositories with jenkins-kubernetes topic",
                        extra={'organization': username, 'action': 'list_repos'})

            for repo in repos:
                repo_name = repo['name']
                package_env = self.client.get_package_env(username, repo_name)

                if package_env and 'PACKAGE_NAME' in package_env:
                    package_name = package_env['PACKAGE_NAME']
                    mapping[package_name] = repo_name
                    logger.debug(f"Mapped package '{package_name}' to repo '{repo_name}'",
                                 extra={'organization': username, 'package': package_name,
                                        'repository': repo_name})
                else:
                    # Fallback: assume package name matches repo name
                    mapping[repo_name] = repo_name
                    logger.debug(f"Using fallback mapping for '{repo_name}'",
                                 extra={'organization': username, 'package': repo_name})

        except Exception as e:
            logger.warning(f"Error building package-repo mapping: {e}",
                           extra={'organization': username, 'action': 'build_mapping'})

        return mapping

    def get_active_refs(self, owner: str, repo: str) -> Tuple[Set[str], Set[str]]:
        """Get all active references (branches, tags, PRs)"""
        active_refs = {'latest'}
        base_refs = set()  # Track base tags (without arch suffixes)

        try:
            branches = self.client.list_repository_branches(owner, repo)
            active_refs.update(branches)
            base_refs.update(branches)
            logger.info(f"Found {len(branches)} active branches",
                        extra={'organization': owner, 'repository': repo, 'action': 'list_branches'})
        except Exception as e:
            logger.error(f"Failed to list branches: {e}",
                         extra={'organization': owner, 'repository': repo, 'action': 'list_branches'})

        try:
            git_tags = self.client.list_repository_tags(owner, repo)
            active_refs.update(git_tags)
            base_refs.update(git_tags)
            logger.info(f"Found {len(git_tags)} git tags",
                        extra={'organization': owner, 'repository': repo, 'action': 'list_tags'})
        except Exception as e:
            logger.error(f"Failed to list tags: {e}",
                         extra={'organization': owner, 'repository': repo, 'action': 'list_tags'})

        try:
            prs = self.client.list_repository_pull_requests(owner, repo)
            pr_tags = [f'pr-{pr}' for pr in prs]
            active_refs.update(pr_tags)
            base_refs.update(pr_tags)
            logger.info(f"Found {len(prs)} open pull requests",
                        extra={'organization': owner, 'repository': repo, 'action': 'list_prs'})
        except Exception as e:
            logger.error(f"Failed to list pull requests: {e}",
                         extra={'organization': owner, 'repository': repo, 'action': 'list_prs'})

        # Add architecture-specific variants for all base refs
        # e.g., if "main" exists, also accept "main-amd64", "main-arm64"
        arch_suffixes = ['amd64', 'arm64', 'arm', '386', 'ppc64le', 's390x', 'riscv64']
        for base_ref in base_refs:
            for arch in arch_suffixes:
                active_refs.add(f'{base_ref}-{arch}')

        return active_refs, base_refs

    def get_referenced_digests(self, owner: str, package_name: str,
                               versions: List[PackageVersion], active_refs: Set[str]) -> Set[str]:
        """Get all digests referenced by manifest lists with valid tags"""
        referenced_digests = set()

        try:
            token = self.client.get_ghcr_token(owner, package_name)

            # Only check versions that have tags we want to keep
            for version in versions:
                valid_tags = [tag for tag in version.tags if tag in active_refs]

                if not valid_tags:
                    continue

                # Check each valid tag
                for tag in valid_tags:
                    try:
                        manifest = self.client.get_manifest(owner, package_name, token, tag)

                        media_type = manifest.get('mediaType', '')
                        if media_type in ['application/vnd.docker.distribution.manifest.list.v2+json',
                                          'application/vnd.oci.image.index.v1+json']:
                            # This is a manifest list - add all referenced digests
                            for m in manifest.get('manifests', []):
                                digest = m.get('digest')
                                if digest:
                                    referenced_digests.add(digest)
                                    logger.debug(f"Tag '{tag}' references digest {digest}",
                                                 extra={'organization': owner, 'package': package_name,
                                                        'tag': tag, 'digest': digest})

                    except Exception as e:
                        logger.warning(f"Failed to get manifest for tag '{tag}': {e}",
                                       extra={'organization': owner, 'package': package_name, 'tag': tag})

            logger.info(f"Found {len(referenced_digests)} digests referenced by manifest lists",
                        extra={'organization': owner, 'package': package_name,
                               'action': 'find_referenced_digests'})

        except Exception as e:
            logger.error(f"Failed to get referenced digests: {e}",
                         extra={'organization': owner, 'package': package_name,
                                'action': 'find_referenced_digests'})

        return referenced_digests

    def cleanup_package(self, owner: str, package_type: str, package_name: str, repo: str) -> Dict:
        """Clean up a single package"""
        stats = {
            'total_versions': 0,
            'stale_tags_deleted': 0,
            'unreferenced_digests_deleted': 0,
            'errors': 0
        }

        logger.info(f"Starting cleanup for package {package_name} (repo: {repo})",
                    extra={'organization': owner, 'package': package_name,
                           'repository': repo, 'action': 'start_cleanup'})

        try:
            # Get all package versions
            versions = self.client.list_package_versions(owner, package_type, package_name)
            stats['total_versions'] = len(versions)
            package_versions.labels(package=package_name, organization=owner).set(len(versions))

            logger.info(f"Found {len(versions)} versions",
                        extra={'organization': owner, 'package': package_name,
                               'repository': repo, 'action': 'list_versions'})

            # Get active references
            active_refs, base_refs = self.get_active_refs(owner, repo)
            logger.info(f"Active references: {len(active_refs)} (base refs: {len(base_refs)})",
                        extra={'organization': owner, 'package': package_name,
                               'repository': repo, 'action': 'get_active_refs',
                               'count': len(active_refs)})

            # Get referenced digests (digests in manifest lists with valid tags)
            referenced_digests = self.get_referenced_digests(owner, package_name, versions, active_refs)

            # Phase 1: Delete versions with stale tags
            # Do this first so we don't protect digests from stale tags
            versions_to_delete_stale_tags = []
            for version in versions:
                has_valid_tag = False
                stale_tag = None

                for tag in version.tags:
                    if tag in active_refs:
                        has_valid_tag = True
                        break
                    else:
                        # Check if this is an arch-specific tag without a base
                        match = self.arch_pattern.match(tag)
                        if match:
                            base_tag = match.group(1)
                            if base_tag not in base_refs:
                                # Base tag doesn't exist, this arch tag is stale
                                stale_tag = tag
                        else:
                            stale_tag = tag

                if not has_valid_tag and stale_tag:
                    versions_to_delete_stale_tags.append((version, stale_tag, 'stale_tag'))

            logger.info(f"Found {len(versions_to_delete_stale_tags)} versions with stale tags",
                        extra={'organization': owner, 'package': package_name,
                               'repository': repo, 'action': 'identify_stale_tags'})

            for version, tag, reason in versions_to_delete_stale_tags:
                if self.dry_run:
                    logger.info(f"[DRY RUN] Would delete version {version.id} (tag: {tag}, digest: {version.name})",
                                extra={'organization': owner, 'package': package_name,
                                       'repository': repo, 'action': 'delete_version',
                                       'digest': version.name, 'tag': tag, 'reason': reason})
                else:
                    try:
                        success = self.client.delete_package_version(owner, package_type, package_name, version.id)
                        if success:
                            stats['stale_tags_deleted'] += 1
                            tags_deleted.labels(package=package_name, organization=owner).inc()
                            logger.info(f"Deleted version {version.id} (tag: {tag}, digest: {version.name})",
                                        extra={'organization': owner, 'package': package_name,
                                               'repository': repo, 'action': 'delete_version',
                                               'digest': version.name, 'tag': tag, 'reason': reason})
                        else:
                            stats['errors'] += 1
                            logger.error(f"Failed to delete version {version.id}",
                                         extra={'organization': owner, 'package': package_name,
                                                'repository': repo, 'action': 'delete_version',
                                                'digest': version.name})
                    except Exception as e:
                        stats['errors'] += 1
                        cleanup_errors_total.labels(error_type='delete_stale_tag').inc()
                        logger.error(f"Error deleting version {version.id}: {e}",
                                     extra={'organization': owner, 'package': package_name,
                                            'repository': repo, 'action': 'delete_version',
                                            'digest': version.name})

            # Phase 2: Delete unreferenced digests
            # Refresh version list after phase 1 deletions
            if not self.dry_run and versions_to_delete_stale_tags:
                time.sleep(1)  # Brief delay to let GitHub API catch up
                versions = self.client.list_package_versions(owner, package_type, package_name)

            versions_to_delete_unreferenced = []
            for version in versions:
                # Skip if it has any tags (those are handled in phase 1)
                if version.tags:
                    continue

                # Skip if it's referenced by a manifest list
                if version.name in referenced_digests:
                    continue

                versions_to_delete_unreferenced.append((version, 'unreferenced_digest'))

            logger.info(f"Found {len(versions_to_delete_unreferenced)} unreferenced digests",
                        extra={'organization': owner, 'package': package_name,
                               'repository': repo, 'action': 'identify_unreferenced'})

            for version, reason in versions_to_delete_unreferenced:
                if self.dry_run:
                    logger.info(f"[DRY RUN] Would delete version {version.id} (digest: {version.name})",
                                extra={'organization': owner, 'package': package_name,
                                       'repository': repo, 'action': 'delete_version',
                                       'digest': version.name, 'reason': reason})
                else:
                    try:
                        success = self.client.delete_package_version(owner, package_type, package_name, version.id)
                        if success:
                            stats['unreferenced_digests_deleted'] += 1
                            digests_deleted.labels(package=package_name, organization=owner).inc()
                            logger.info(f"Deleted version {version.id} (digest: {version.name})",
                                        extra={'organization': owner, 'package': package_name,
                                               'repository': repo, 'action': 'delete_version',
                                               'digest': version.name, 'reason': reason})
                        else:
                            stats['errors'] += 1
                            logger.error(f"Failed to delete version {version.id}",
                                         extra={'organization': owner, 'package': package_name,
                                                'repository': repo, 'action': 'delete_version',
                                                'digest': version.name})
                    except Exception as e:
                        stats['errors'] += 1
                        cleanup_errors_total.labels(error_type='delete_unreferenced').inc()
                        logger.error(f"Error deleting version {version.id}: {e}",
                                     extra={'organization': owner, 'package': package_name,
                                            'repository': repo, 'action': 'delete_version',
                                            'digest': version.name})

            logger.info(f"Cleanup complete for package {package_name}",
                        extra={'organization': owner, 'package': package_name,
                               'repository': repo, 'action': 'complete_cleanup', 'stats': stats})

        except Exception as e:
            stats['errors'] += 1
            cleanup_errors_total.labels(error_type='package_cleanup').inc()
            logger.error(f"Error during package cleanup: {e}",
                         extra={'organization': owner, 'package': package_name,
                                'repository': repo, 'action': 'cleanup_error'})

        return stats

    @cleanup_duration.time()
    def run_cleanup(self, username: str, single_package: Optional[str] = None):
        """Run cleanup for all packages or a single package"""
        cleanup_runs_total.inc()

        logger.info(f"Starting cleanup run for user {username}",
                    extra={'organization': username, 'action': 'start_run',
                           'single_package': single_package})

        try:
            # Build package to repository mapping
            package_repo_mapping = self.build_package_repo_mapping(username)

            # List all packages
            packages = self.client.list_user_packages(username, package_type="container")
            active_packages.labels(organization=username).set(len(packages))

            logger.info(f"Found {len(packages)} packages",
                        extra={'organization': username, 'action': 'list_packages',
                               'count': len(packages)})

            # Filter to single package if specified
            if single_package:
                packages = [pkg for pkg in packages if pkg['name'] == single_package]
                if not packages:
                    logger.warning(f"Package '{single_package}' not found",
                                   extra={'organization': username, 'action': 'filter_packages',
                                          'single_package': single_package})
                    return
                logger.info(f"Filtered to single package: {single_package}",
                            extra={'organization': username, 'action': 'filter_packages',
                                   'single_package': single_package})

            total_stats = defaultdict(int)

            for pkg in packages:
                packages_processed.inc()

                package_name = pkg['name']
                # Get repository name from mapping, fallback to package name
                repo_name = package_repo_mapping.get(package_name, package_name)

                logger.info(f"Processing package: {package_name} -> repo: {repo_name}",
                            extra={'organization': username, 'package': package_name,
                                   'repository': repo_name, 'action': 'process_package',
                                   'created_at': pkg.get('created_at'),
                                   'updated_at': pkg.get('updated_at')})

                stats = self.cleanup_package(username, "container", package_name, repo_name)

                for key, value in stats.items():
                    total_stats[key] += value

            logger.info(f"Cleanup run complete",
                        extra={'organization': username, 'action': 'complete_run',
                               'stats': dict(total_stats), 'single_package': single_package})

        except Exception as e:
            cleanup_errors_total.labels(error_type='run_cleanup').inc()
            logger.error(f"Error during cleanup run: {e}",
                         extra={'organization': username, 'action': 'run_error'})


def main():
    # Configuration from environment variables
    github_token = os.getenv('GITHUB_TOKEN')
    github_username = os.getenv('GITHUB_USERNAME')
    single_package = os.getenv('SINGLE_PACKAGE')
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    metrics_port = int(os.getenv('METRICS_PORT', '8000'))
    schedule_time = os.getenv('SCHEDULE_TIME', '02:00')  # Default 2 AM

    if not github_token:
        logger.error("GITHUB_TOKEN environment variable is required")
        sys.exit(1)

    if not github_username:
        logger.error("GITHUB_USERNAME environment variable is required")
        sys.exit(1)

    logger.info("Starting GHCR Cleanup Service",
                extra={'action': 'startup', 'dry_run': dry_run,
                       'metrics_port': metrics_port, 'schedule_time': schedule_time,
                       'single_package': single_package})

    # Start Prometheus metrics server
    start_http_server(metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}",
                extra={'action': 'metrics_started', 'port': metrics_port})

    # Initialize client and cleanup
    client = GHCRClient(github_token)
    cleanup = GHCRCleanup(client, dry_run=dry_run)

    # If single package is specified, run immediately and then continue with scheduled runs
    if single_package:
        logger.info(f"Single package mode enabled for: {single_package}",
                    extra={'action': 'single_package_mode', 'package': single_package})
        logger.info("Running immediate cleanup for single package",
                    extra={'action': 'immediate_run', 'package': single_package})
        cleanup.run_cleanup(github_username, single_package=single_package)
        logger.info("Single package cleanup complete, continuing with scheduled mode",
                    extra={'action': 'continue_scheduled', 'package': single_package})
    else:
        # Run immediately on startup for all packages
        logger.info("Running initial cleanup for all packages",
                    extra={'action': 'initial_run'})
        cleanup.run_cleanup(github_username)

    # Schedule daily cleanup (always for all packages)
    schedule.every().day.at(schedule_time).do(cleanup.run_cleanup, username=github_username)

    # Keep running and execute scheduled tasks
    logger.info("Entering scheduler loop",
                extra={'action': 'scheduler_start'})

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == '__main__':
    main()