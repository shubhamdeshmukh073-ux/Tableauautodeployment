#!/usr/bin/env python3
"""
Automates exporting a single Tableau workbook, zipping it and uploading to Nexus, and pushing the .twb/.twbx to GitHub (with a PR).
"""
import os
import sys
import zipfile
import tempfile
import logging
from pathlib import Path

import requests
from github import Github
from dotenv import load_dotenv

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('automate_workbook_export')

# --- Load Environment Variables ---
if os.path.exists('.env'):
    load_dotenv('.env')

# Required for Tableau: source/target (target can be blank)
TABLEAU_SOURCE_SERVER = os.environ.get('TABLEAU_SOURCE_SERVER')
TABLEAU_SOURCE_SITE = os.environ.get('TABLEAU_SOURCE_SITE', '')
TABLEAU_SOURCE_TOKEN_NAME = os.environ.get('TABLEAU_SOURCE_TOKEN_NAME')
TABLEAU_SOURCE_TOKEN_VALUE = os.environ.get('TABLEAU_SOURCE_TOKEN_VALUE')
TABLEAU_SOURCE_USERNAME = os.environ.get('TABLEAU_SOURCE_USERNAME')
TABLEAU_SOURCE_PASSWORD = os.environ.get('TABLEAU_SOURCE_PASSWORD')
TABLEAU_API_VERSION = os.environ.get('TABLEAU_API_VERSION')

# Required for Nexus
NEXUS_URL = os.environ.get('NEXUS_URL')  # e.g. 'https://nexus.example.com/repository/your-repo/'
NEXUS_USERNAME = os.environ.get('NEXUS_USERNAME')
NEXUS_PASSWORD = os.environ.get('NEXUS_PASSWORD')

# Required for GitHub
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GITHUB_REPO = os.environ.get('GITHUB_REPO')  # e.g. 'yourorg/yourrepo'

# --- Import TableauMigrator ---
import importlib.util
spec = importlib.util.spec_from_file_location('tableau_migration', 'tableau_migration.py')
tableau_migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tableau_migration)
TableauMigrator = tableau_migration.TableauMigrator


def download_tableau_workbook(migrator, workbook_name, source_project_id_or_name, download_dir):
    '''Downloads one workbook by name from Tableau using TableauMigrator.'''
    migrator.connect_to_source()
    # If project argument looks like an ID, prefer ID, else treat as name
    project_id = None
    if len(source_project_id_or_name) > 20:  # likely a UUID
        project_id = source_project_id_or_name
    else:
        # Find by project name
        projects = migrator.list_projects(migrator.source_server)
        for p in projects:
            if p.name.lower() == source_project_id_or_name.lower():
                project_id = p.id
                break
        if not project_id:
            raise Exception(f"Project '{source_project_id_or_name}' not found on Tableau server")
    # Find workbook by name
    workbook = migrator.find_workbook_by_name(migrator.source_server, workbook_name, project_id=project_id)
    if not workbook:
        raise Exception(f"Workbook '{workbook_name}' not found in project '{source_project_id_or_name}'")
    # Download using existing logic, but publish to dummy target project to trigger just the download
    logger.info(f"Downloading workbook '{workbook_name}' (ID: {workbook.id})...")
    temp_project_id = project_id      # Not used as we don't migrate, just for context
    # Migrate workbook logic downloads and uploads: but here we want just the download
    # So extract code from migrate_workbook for download only
    # First: try both twbx and twb
    root_name = workbook.name
    safe_filename = root_name.replace(' ', '_')
    for ext in ['.twbx', '.twb']:
        candidate = os.path.join(download_dir, safe_filename + ext)
        try:
            migrator.source_server.workbooks.download(workbook.id, candidate, include_extract=migrator.include_extract)
            if os.path.exists(candidate) and os.path.getsize(candidate) > 0:
                logger.info(f"Downloaded to: {candidate}")
                return candidate
        except Exception as e:
            logger.debug(f"Failed to download as {ext}: {e}")
    raise Exception("Unable to download workbook in either .twbx or .twb format.")


def zip_file(input_filepath, zip_path=None):
    if not zip_path:
        zip_path = input_filepath + '.zip'
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(input_filepath, arcname=os.path.basename(input_filepath))
    logger.info(f"Zipped file at: {zip_path}")
    return zip_path


def upload_to_nexus(zip_path, nexus_url, nexus_username, nexus_password):
    """Upload zip file to Nexus raw or Maven repo."""
    filename = os.path.basename(zip_path)
    upload_url = nexus_url.rstrip('/') + '/' + filename
    logger.info(f"Uploading {filename} to Nexus at {upload_url}")
    with open(zip_path, 'rb') as f:
        response = requests.put(
            upload_url,
            auth=(nexus_username, nexus_password),
            headers={'Content-Type': 'application/zip'},
            data=f
        )
    if response.status_code in (200, 201, 204):
        logger.info("Upload to Nexus succeeded.")
    else:
        logger.error(f"Nexus upload failed! Status {response.status_code}: {response.text}")
        raise Exception(f"Nexus upload failed: {response.text}")


def push_to_github_and_pr(repo_name, token, local_file, base_branch='main'):
    g = Github(token)
    repo = g.get_repo(repo_name)
    # Create a branch off main
    sb = repo.get_branch(base_branch)
    from datetime import datetime
    new_branch_name = f'tableau-wb-{Path(local_file).stem}-{datetime.utcnow().strftime("%Y%m%d%H%M%S")}'
    repo.create_git_ref(ref=f'refs/heads/{new_branch_name}', sha=sb.commit.sha)
    logger.info(f"Created branch {new_branch_name}")
    # Add the file (commit)
    with open(local_file, 'rb') as f:
        content = f.read()
    repo.create_or_update_file(
        path=f"{os.path.basename(local_file)}",
        message=f"Add Tableau workbook {os.path.basename(local_file)}",
        content=content,
        branch=new_branch_name
    )
    logger.info(f"Committed workbook to {new_branch_name}")
    # Create PR
    pr = repo.create_pull(
        title=f"Add Tableau workbook {os.path.basename(local_file)}",
        body=f"Automated Tableau workbook upload.",
        head=new_branch_name,
        base=base_branch
    )
    logger.info(f"PR created: {pr.html_url}")
    return pr.html_url


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Automate Tableau workbook export to Nexus and GitHub.")
    parser.add_argument('--workbook-name', required=True, help='Name of the Tableau workbook to download (case sensitive)')
    parser.add_argument('--source-project', required=True, help='Source Tableau project name or ID (must be precise)')
    parser.add_argument('--download-dir', default=None, help='Optional directory for downloads')
    args = parser.parse_args()

    # Safety checks for env/config
    required_vars = [TABLEAU_SOURCE_SERVER, NEXUS_URL, NEXUS_USERNAME, NEXUS_PASSWORD, GITHUB_TOKEN, GITHUB_REPO]
    if not all(required_vars):
        logger.error("Missing required environment/config variables. Please check .env file and usage comments.")
        sys.exit(1)

    # Prepare download directory
    download_dir = args.download_dir or tempfile.mkdtemp()

    # Initialize TableauMigrator for source only
    migrator = TableauMigrator(
        source_server=TABLEAU_SOURCE_SERVER,
        target_server=None,
        source_site=TABLEAU_SOURCE_SITE,
        target_site=None,
        logger=logger,
        source_token_name=TABLEAU_SOURCE_TOKEN_NAME,
        source_token_value=TABLEAU_SOURCE_TOKEN_VALUE,
        source_username=TABLEAU_SOURCE_USERNAME,
        source_password=TABLEAU_SOURCE_PASSWORD,
        verify_ssl=True,
        api_version=TABLEAU_API_VERSION,
        download_dir=download_dir
    )
    try:
        # 1. Download workbook
        local_workbook = download_tableau_workbook(migrator, args.workbook_name, args.source_project, download_dir)
        # 2. Zip the file
        zip_path = zip_file(local_workbook)
        # 3. Upload zip to Nexus
        upload_to_nexus(zip_path, NEXUS_URL, NEXUS_USERNAME, NEXUS_PASSWORD)
        # 4. Push workbook file to GitHub & create PR
        pr_url = push_to_github_and_pr(GITHUB_REPO, GITHUB_TOKEN, local_workbook)
        logger.info(f"SUCCESS: GitHub PR created at {pr_url}")
    finally:
        if not args.download_dir and os.path.isdir(download_dir):
            import shutil
            shutil.rmtree(download_dir)

if __name__ == "__main__":
    main()
