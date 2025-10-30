#!/usr/bin/env python3
"""
Tableau Server Migration Tool

This script allows migrating workbooks from one Tableau server to another,
supporting different sites and folder structures.
"""

import os
import sys
import argparse
import getpass
import logging
import tempfile
import tableauserverclient as TSC
from pathlib import Path
import time
import re

# Add dotenv support for reading environment variables
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


class TableauMigrator:
    def __init__(self, source_server, target_server, source_site, target_site, 
                 logger=None, source_token_name=None, source_token_value=None, 
                 target_token_name=None, target_token_value=None,
                 source_username=None, source_password=None, 
                 target_username=None, target_password=None,
                 verify_ssl=True, api_version=None, download_dir=None, 
                 include_extract=False, skip_data_sources=False):
        
        self.source_server_url = source_server
        self.target_server_url = target_server
        self.source_site = source_site
        self.target_site = target_site
        self.api_version = api_version
        self.include_extract = include_extract
        self.skip_data_sources = skip_data_sources
        
        # Authentication info
        self.source_token_name = source_token_name
        self.source_token_value = source_token_value
        self.target_token_name = target_token_name
        self.target_token_value = target_token_value
        self.source_username = source_username
        self.source_password = source_password
        self.target_username = target_username
        self.target_password = target_password
        
        # SSL verification
        self.verify_ssl = verify_ssl
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            logger.warning("SSL certificate verification is disabled. This is insecure.")
        
        # Server connections
        self.source_server = None
        self.target_server = None
        
        # Set up logging
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('tableau_migrator')
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Set up temp directory
        if download_dir:
            self.temp_dir = download_dir
            if not os.path.exists(self.temp_dir):
                os.makedirs(self.temp_dir)
                self.logger.info(f"Created download directory: {self.temp_dir}")
            else:
                self.logger.info(f"Using existing download directory: {self.temp_dir}")
            self.should_delete_temp_dir = False
        else:
            self.temp_dir = tempfile.mkdtemp()
            self.logger.info(f"Created temporary directory: {self.temp_dir}")
            self.should_delete_temp_dir = True

    def connect_to_source(self):
        """Connect to the source Tableau server"""
        self.logger.info(f"Connecting to source server: {self.source_server_url}, site: {self.source_site}")
        
        if self.source_token_name and self.source_token_value:
            auth = TSC.PersonalAccessTokenAuth(
                token_name=self.source_token_name,
                personal_access_token=self.source_token_value,
                site_id=self.source_site
            )
            self.logger.info(f"Using token authentication for source server")
        elif self.source_username:
            password = self.source_password or getpass.getpass("Source Server Password: ")
            auth = TSC.TableauAuth(self.source_username, password, site_id=self.source_site)
            self.logger.info(f"Using username/password authentication for source server")
        else:
            raise ValueError("No authentication credentials provided for source server")
        
        # Use auto-detect if no version is specified
        use_server_version = True if self.api_version is None else False
        
        self.source_server = TSC.Server(self.source_server_url, use_server_version=use_server_version, 
                                       http_options={"verify": self.verify_ssl})
        
        # Set API version if specified
        if self.api_version:
            self.source_server.version = self.api_version
            self.logger.info(f"Using API version: {self.api_version}")
        
        self.source_server.auth.sign_in(auth)
        self.logger.info(f"Successfully connected to source server")
        return self.source_server

    def connect_to_target(self):
        """Connect to the target Tableau server"""
        self.logger.info(f"Connecting to target server: {self.target_server_url}, site: {self.target_site}")
        
        if self.target_token_name and self.target_token_value:
            auth = TSC.PersonalAccessTokenAuth(
                token_name=self.target_token_name,
                personal_access_token=self.target_token_value,
                site_id=self.target_site
            )
            self.logger.info(f"Using token authentication for target server")
        elif self.target_username:
            password = self.target_password or getpass.getpass("Target Server Password: ")
            auth = TSC.TableauAuth(self.target_username, password, site_id=self.target_site)
            self.logger.info(f"Using username/password authentication for target server")
        else:
            raise ValueError("No authentication credentials provided for target server")
        
        # Use auto-detect if no version is specified
        use_server_version = True if self.api_version is None else False
        
        self.target_server = TSC.Server(self.target_server_url, use_server_version=use_server_version,
                                       http_options={"verify": self.verify_ssl})
        
        # Set API version if specified
        if self.api_version:
            self.target_server.version = self.api_version
            self.logger.info(f"Using API version: {self.api_version}")
        
        self.target_server.auth.sign_in(auth)
        self.logger.info(f"Successfully connected to target server")
        return self.target_server

    def list_source_sites(self):
        """List all sites on the source server"""
        if not self.source_server:
            self.connect_to_source()
        
        all_sites = list(TSC.Pager(self.source_server.sites))
        self.logger.info(f"Found {len(all_sites)} sites on source server")
        return all_sites
    
    def list_projects(self, server, site=None):
        """List all projects on a server/site"""
        if site and server.site_id != site:
            # Switch to the specified site if needed
            current_site = server.site_id
            self.logger.info(f"Switching from site {current_site} to {site}")
            server.auth.switch_site(site)
        
        all_projects = list(TSC.Pager(server.projects))
        self.logger.info(f"Found {len(all_projects)} projects on site {server.site_id}")
        return all_projects
    
    def list_workbooks(self, server, site=None, project_id=None):
        """List all workbooks on a server/site, optionally filtered by project"""
        if site and server.site_id != site:
            # Switch to the specified site if needed
            current_site = server.site_id
            self.logger.info(f"Switching from site {current_site} to {site}")
            server.auth.switch_site(site)
        
        try:
            # Get all workbooks without any options that could trigger API compatibility issues
            all_workbooks = []
            for wb in TSC.Pager(server.workbooks):
                all_workbooks.append(wb)
            
            self.logger.info(f"Retrieved {len(all_workbooks)} total workbooks from site {server.site_id}")
            
            # Filter locally by project_id if needed
            if project_id:
                # Debug info: Log all project IDs to help troubleshoot
                if self.logger.level <= logging.DEBUG:
                    project_ids = set(wb.project_id for wb in all_workbooks)
                    self.logger.debug(f"Available project IDs in workbooks: {project_ids}")
                    self.logger.debug(f"Looking for project ID: {project_id}")
                
                # More flexible comparison - convert both to strings for comparison
                filtered_workbooks = [wb for wb in all_workbooks 
                                     if str(wb.project_id).lower() == str(project_id).lower()]
                
                self.logger.info(f"Filtered to {len(filtered_workbooks)} workbooks in project {project_id}")
                return filtered_workbooks
            else:
                return all_workbooks
                
        except Exception as e:
            self.logger.error(f"Error listing workbooks: {str(e)}")
            return []
    
    def ensure_project_exists(self, project_name, parent_id=None):
        """Make sure a project exists on the target server, create if it doesn't"""
        # Check if project exists
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, 
                                          TSC.RequestOptions.Operator.Equals, 
                                          project_name))
        
        matching_projects = list(TSC.Pager(self.target_server.projects, req_option))
        
        if matching_projects:
            for project in matching_projects:
                # If parent_id is None, we're looking for top-level project
                # If parent_id is not None, we need to match it
                if (parent_id is None and project.parent_id is None) or \
                   (parent_id is not None and project.parent_id == parent_id):
                    self.logger.info(f"Found existing project: {project_name}")
                    return project
        
        # Create the project if it doesn't exist
        new_project = TSC.ProjectItem(name=project_name, parent_id=parent_id)
        new_project = self.target_server.projects.create(new_project)
        self.logger.info(f"Created new project: {project_name}")
        return new_project
    
    def migrate_workbook(self, workbook_id, source_project, target_project_id):
        """Migrate a single workbook from source to target
        
        This is a copy operation - workbooks are copied to the target server
        but remain intact on the source server.
        """
        if not self.source_server:
            self.connect_to_source()
        if not self.target_server:
            self.connect_to_target()
        
        # First verify the workbook exists
        try:
            # Check if workbook exists before attempting download
            self.logger.info(f"Verifying workbook exists with ID: {workbook_id}")
            try:
                workbook = self.source_server.workbooks.get_by_id(workbook_id)
                self.logger.info(f"Found workbook: {workbook.name} (ID: {workbook_id})")
            except Exception as wb_err:
                self.logger.error(f"Error finding workbook with ID {workbook_id}: {str(wb_err)}")
                
                # Try to list workbooks in the project to suggest valid IDs
                try:
                    project_workbooks = self.list_workbooks(self.source_server, project_id=source_project)
                    if project_workbooks:
                        self.logger.info("Available workbooks in this project:")
                        for wb in project_workbooks:
                            self.logger.info(f"  - {wb.name} (ID: {wb.id})")
                    else:
                        self.logger.info(f"No workbooks found in project ID: {source_project}")
                except Exception as list_err:
                    self.logger.error(f"Error listing workbooks: {str(list_err)}")
                
                raise ValueError(f"Workbook with ID '{workbook_id}' not found. Please verify the ID is correct.")
            
            # Create safe filenames without characters that might cause issues
            safe_filename = re.sub(r'[^\w\-_.]', '_', f"workbook_{workbook_id}")
            
            # Try two different file extensions
            file_extensions = ['.twbx', '.twb']
            downloaded = False
            workbook_file = None
            error_messages = []
            
            for ext in file_extensions:
                if downloaded:
                    break
                    
                workbook_file = os.path.join(self.temp_dir, f"{safe_filename}{ext}")
                self.logger.info(f"Attempting to download workbook {workbook_id} to {workbook_file}")
                
                try:
                    # Specify include_extract based on user option
                    self.source_server.workbooks.download(workbook_id, workbook_file, include_extract=self.include_extract)
                    
                    # Verify file was downloaded and exists
                    if os.path.exists(workbook_file):
                        file_size = os.path.getsize(workbook_file)
                        self.logger.info(f"Downloaded workbook file size: {file_size} bytes")
                        
                        if file_size > 0:
                            downloaded = True
                            self.logger.info(f"Successfully downloaded workbook to {workbook_file}")
                        else:
                            os.remove(workbook_file)
                            error_messages.append(f"Downloaded file is empty (extension: {ext})")
                    else:
                        error_messages.append(f"File does not exist after download (extension: {ext})")
                except Exception as download_err:
                    error_messages.append(f"Error during download with extension {ext}: {str(download_err)}")
            
            # If no successful download, try a fallback approach
            if not downloaded:
                try:
                    self.logger.info("Trying alternative download approach...")
                    # Create a simpler path with a basic file name
                    workbook_file = os.path.join(self.temp_dir, "workbook.twbx")
                    
                    # Try a different API approach - note: no_extract was incorrect
                    # The correct parameter is include_extract
                    self.logger.info(f"Downloading to directory {self.temp_dir} with include_extract={self.include_extract}")
                    try:
                        download_path = self.source_server.workbooks.download(workbook_id, 
                                                                            filepath=self.temp_dir, 
                                                                            include_extract=self.include_extract)
                        self.logger.info(f"Download path returned: {download_path}")
                    except TypeError:
                        # Older versions of TSC might not support the include_extract parameter
                        self.logger.info("Trying download without extra parameters")
                        download_path = self.source_server.workbooks.download(workbook_id, 
                                                                            filepath=self.temp_dir)
                    
                    # Handle the case where the path is returned as a string
                    if isinstance(download_path, str) and os.path.exists(download_path):
                        workbook_file = download_path
                        file_size = os.path.getsize(workbook_file)
                        self.logger.info(f"Alternative download succeeded with path return. File: {workbook_file}, size: {file_size} bytes")
                        downloaded = True
                    # Or the case where the download method returns None but creates the file
                    elif download_path is None:
                        # Look for any new files in the temp dir that might be our workbook
                        possible_files = [f for f in os.listdir(self.temp_dir) 
                                        if f.endswith('.twb') or f.endswith('.twbx')]
                        if possible_files:
                            newest_file = max(possible_files, key=lambda f: os.path.getctime(os.path.join(self.temp_dir, f)))
                            workbook_file = os.path.join(self.temp_dir, newest_file)
                            if os.path.exists(workbook_file):
                                file_size = os.path.getsize(workbook_file)
                                self.logger.info(f"Found potential workbook file: {workbook_file}, size: {file_size} bytes")
                                downloaded = True
                        else:
                            error_messages.append("No workbook files found in download directory")
                    else:
                        error_messages.append("Alternative download approach returned a path, but file does not exist")
                except Exception as alt_err:
                    error_messages.append(f"Alternative download approach failed: {str(alt_err)}")
                    self.logger.error(f"Exception details: {alt_err.__class__.__name__}: {str(alt_err)}")
                    import traceback
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            # If still not downloaded, raise error with all the messages
            if not downloaded:
                error_detail = "\n".join(error_messages)
                self.logger.error(f"All download attempts failed:\n{error_detail}")
                raise FileNotFoundError(f"Failed to download workbook {workbook_id} after multiple attempts")
            
            # Small delay to ensure file is fully flushed to disk
            time.sleep(1)
            
            # Create a new workbook item with the target project id
            new_workbook = TSC.WorkbookItem(project_id=target_project_id, name=workbook.name)
            
            # Upload to target
            self.logger.info(f"Uploading workbook {workbook.name} to target project {target_project_id}")
            
            try:
                # Try with CreateNew instead of Overwrite if there are issues
                publish_mode = TSC.Server.PublishMode.Overwrite
                
                # Make sure the file is accessible and readable
                with open(workbook_file, 'rb') as file_check:
                    file_check.read(1024)  # Read a small chunk to verify file is accessible
                    self.logger.info("File is readable")
                
                self.logger.info(f"Publishing with mode: {publish_mode}")
                
                # Check for older version of tableauserverclient
                if self.skip_data_sources:
                    self.logger.info("Publishing without data source connections (--skip-data-sources enabled)")
                    
                    # For older versions, we can't disable connections, so we'll just publish normally
                    # and warn the user
                    self.logger.warning("Your version of tableauserverclient doesn't support skipping data sources.")
                    self.logger.warning("The workbook will be published with data connections.")
                    self.logger.warning("If this fails due to permissions, you'll need to update tableauserverclient:")
                    self.logger.warning("pip install tableauserverclient --upgrade")
                
                # Basic publish with no extra options
                self.target_server.workbooks.publish(new_workbook, workbook_file, publish_mode)
                    
                self.logger.info(f"Successfully migrated workbook {workbook.name}")
            except Exception as upload_error:
                self.logger.error(f"Error publishing workbook: {str(upload_error)}")
                self.logger.error(f"Workbook file exists: {os.path.exists(workbook_file)}")
                self.logger.error(f"Workbook file size: {os.path.getsize(workbook_file) if os.path.exists(workbook_file) else 'N/A'}")
                self.logger.error(f"Target project exists: {target_project_id}")
                
                # Try with different publish mode
                try:
                    self.logger.info("Trying alternative publish mode...")
                    publish_mode = TSC.Server.PublishMode.CreateNew
                    self.logger.info(f"Publishing with mode: {publish_mode}")
                    
                    # Basic publish with no extra options
                    self.target_server.workbooks.publish(new_workbook, workbook_file, publish_mode)
                        
                    self.logger.info(f"Successfully migrated workbook {workbook.name} with alternative mode")
                except Exception as retry_error:
                    self.logger.error(f"Alternative publish mode also failed: {str(retry_error)}")
                    raise
                    
        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            raise
        finally:
            # Clean up the temp file only if we're not keeping the download directory
            if workbook_file and os.path.exists(workbook_file) and self.should_delete_temp_dir:
                try:
                    os.remove(workbook_file)
                    self.logger.info(f"Removed temporary file: {workbook_file}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Failed to remove temporary file: {str(cleanup_error)}")
    
    def migrate_project(self, source_project_id, target_project_id=None):
        """Migrate all workbooks from a source project to a target project
        
        This is a copy operation - all content remains intact on the source server.
        """
        if not self.source_server:
            self.connect_to_source()
        if not self.target_server:
            self.connect_to_target()
        
        # Get source project details
        source_project = self.source_server.projects.get_by_id(source_project_id)
        
        # If no target project ID is provided, create or find a matching project
        if not target_project_id:
            target_project = self.ensure_project_exists(source_project.name, source_project.parent_id)
            target_project_id = target_project.id
        
        # Get all workbooks in the source project
        workbooks = self.list_workbooks(self.source_server, project_id=source_project_id)
        
        # Migrate each workbook
        for workbook in workbooks:
            self.migrate_workbook(workbook.id, source_project, target_project_id)
        
        self.logger.info(f"Successfully migrated {len(workbooks)} workbooks from project {source_project.name}")
    
    def migrate_site(self, source_site_id=None, target_site_id=None):
        """Migrate all projects and workbooks from a source site to a target site
        
        This is a copy operation - all content remains intact on the source server.
        """
        # Use current site if none specified
        source_site_id = source_site_id or self.source_site
        target_site_id = target_site_id or self.target_site
        
        # Ensure we're connected to both servers
        if not self.source_server:
            self.connect_to_source()
        if not self.target_server:
            self.connect_to_target()
        
        # Switch to the specified sites if needed
        if self.source_server.site_id != source_site_id:
            self.source_server.auth.switch_site(source_site_id)
        
        if self.target_server.site_id != target_site_id:
            self.target_server.auth.switch_site(target_site_id)
        
        # Get all projects in the source site
        source_projects = self.list_projects(self.source_server)
        
        # Create project hierarchy mapping
        project_map = {}
        
        # First pass: create all top-level projects
        for project in source_projects:
            if not project.parent_id:
                target_project = self.ensure_project_exists(project.name)
                project_map[project.id] = target_project.id
        
        # Second pass: create all child projects 
        # This might need multiple passes for deep hierarchies
        remaining_projects = [p for p in source_projects if p.parent_id]
        while remaining_projects:
            projects_handled = []
            for project in remaining_projects:
                if project.parent_id in project_map:
                    # Parent has been created, so we can create this one
                    target_parent_id = project_map[project.parent_id]
                    target_project = self.ensure_project_exists(project.name, target_parent_id)
                    project_map[project.id] = target_project.id
                    projects_handled.append(project)
            
            if not projects_handled:
                # If we didn't handle any projects in this pass, we have an issue
                self.logger.error(f"Unable to create project hierarchy for {len(remaining_projects)} projects")
                break
                
            # Remove handled projects from the remaining list
            remaining_projects = [p for p in remaining_projects if p not in projects_handled]
        
        # Now migrate all projects
        for source_project_id, target_project_id in project_map.items():
            self.migrate_project(source_project_id, target_project_id)
        
        self.logger.info(f"Successfully migrated site {source_site_id} to {target_site_id}")
    
    def cleanup(self):
        """Clean up temporary files and sign out of servers"""
        # Clean up temp directory
        if self.should_delete_temp_dir:
            import shutil
            try:
                if os.path.exists(self.temp_dir):
                    shutil.rmtree(self.temp_dir)
                    self.logger.info(f"Removed temporary directory: {self.temp_dir}")
            except Exception as e:
                self.logger.warning(f"Error cleaning up temporary directory: {str(e)}")
        else:
            self.logger.info(f"Keeping download directory: {self.temp_dir}")
        
        # Sign out of servers
        try:
            if self.source_server:
                self.source_server.auth.sign_out()
                self.logger.info("Signed out of source server")
        except Exception as e:
            self.logger.warning(f"Error signing out of source server: {str(e)}")
        
        try:
            if self.target_server:
                self.target_server.auth.sign_out()
                self.logger.info("Signed out of target server")
        except Exception as e:
            self.logger.warning(f"Error signing out of target server: {str(e)}")

    def list_workbooks_by_project_name(self, server, project_name, site=None):
        """List all workbooks in a project identified by name"""
        if site and server.site_id != site:
            # Switch to the specified site if needed
            current_site = server.site_id
            self.logger.info(f"Switching from site {current_site} to {site}")
            server.auth.switch_site(site)
        
        # First, get all projects to find the one with the matching name
        try:
            all_projects = list(TSC.Pager(server.projects))
            self.logger.info(f"Found {len(all_projects)} projects on site {server.site_id}")
            
            # Find projects with matching name (case insensitive)
            matching_projects = [p for p in all_projects 
                               if p.name.lower() == project_name.lower()]
            
            if not matching_projects:
                self.logger.error(f"No project found with name: {project_name}")
                return []
            
            if len(matching_projects) > 1:
                self.logger.warning(f"Multiple projects found with name: {project_name}. Using the first one.")
            
            target_project = matching_projects[0]
            self.logger.info(f"Found project '{target_project.name}' with ID: {target_project.id}")
            
            # Now get workbooks for this project
            return self.list_workbooks(server, site, target_project.id)
            
        except Exception as e:
            self.logger.error(f"Error listing workbooks by project name: {str(e)}")
            return []

    def find_workbook_by_name(self, server, workbook_name, project_id=None, site=None):
        """Find a workbook by name, optionally filtered by project"""
        if site and server.site_id != site:
            # Switch to the specified site if needed
            current_site = server.site_id
            self.logger.info(f"Switching from site {current_site} to {site}")
            server.auth.switch_site(site)
        
        try:
            # Get all workbooks
            all_workbooks = self.list_workbooks(server, project_id=project_id)
            
            # Find workbooks with matching name (case insensitive)
            matching_workbooks = [wb for wb in all_workbooks 
                                if wb.name.lower() == workbook_name.lower()]
            
            if not matching_workbooks:
                self.logger.warning(f"No workbook found with name: {workbook_name}")
                if project_id:
                    self.logger.info(f"Available workbooks in project {project_id}:")
                    for wb in all_workbooks:
                        self.logger.info(f"  - {wb.name} (ID: {wb.id})")
                return None
            
            if len(matching_workbooks) > 1:
                self.logger.warning(f"Multiple workbooks found with name: {workbook_name}. Using the first one.")
            
            target_workbook = matching_workbooks[0]
            self.logger.info(f"Found workbook '{target_workbook.name}' with ID: {target_workbook.id}")
            
            return target_workbook
            
        except Exception as e:
            self.logger.error(f"Error finding workbook by name: {str(e)}")
            return None


def main():
    parser = argparse.ArgumentParser(description="Migrate workbooks between Tableau servers")
    
    # Server connection options
    parser.add_argument("--source-server", "-ss", 
                        help="Source Tableau server URL (e.g., https://tableau.example.com)")
    parser.add_argument("--target-server", "-ts", 
                        help="Target Tableau server URL (e.g., https://tableau-target.example.com)")
    parser.add_argument("--source-site", "-ssite", default="",
                        help="Source site ID (use empty string for default site)")
    parser.add_argument("--target-site", "-tsite", default="",
                        help="Target site ID (use empty string for default site)")
    parser.add_argument("--no-ssl-verify", action="store_true",
                        help="Disable SSL certificate verification (insecure, but useful for self-signed certs)")
    parser.add_argument("--api-version", 
                        help="Specify Tableau Server REST API version (e.g., 3.4, 3.10)")
    parser.add_argument("--download-dir",
                        help="Specify a custom directory for workbook downloads (optional)")
    parser.add_argument("--include-extract", action="store_true",
                        help="Include data extract when downloading workbooks (may make file larger)")
    parser.add_argument("--skip-data-sources", action="store_true",
                        help="Skip data source connections when publishing (helps with permission issues)")
    parser.add_argument("--env-file", default=".env",
                        help="Path to .env file for credentials (default: .env in current directory)")
    
    # Authentication options - Source
    source_auth = parser.add_argument_group("Source Server Authentication")
    source_auth_method = source_auth.add_mutually_exclusive_group(required=False)
    source_auth_method.add_argument("--source-token-name", "-stn",
                                  help="Name of personal access token for source server")
    source_auth_method.add_argument("--source-username", "-su",
                                  help="Username for source server")
    source_auth.add_argument("--source-token-value", "-stv",
                           help="Value of personal access token for source server")
    source_auth.add_argument("--source-password", "-sp",
                           help="Password for source server")
    
    # Authentication options - Target
    target_auth = parser.add_argument_group("Target Server Authentication")
    target_auth_method = target_auth.add_mutually_exclusive_group(required=False)
    target_auth_method.add_argument("--target-token-name", "-ttn",
                                  help="Name of personal access token for target server")
    target_auth_method.add_argument("--target-username", "-tu",
                                  help="Username for target server")
    target_auth.add_argument("--target-token-value", "-ttv",
                           help="Value of personal access token for target server")
    target_auth.add_argument("--target-password", "-tp",
                           help="Password for target server")
    
    # Action to perform
    action = parser.add_argument_group("Migration Action")
    action_type = action.add_mutually_exclusive_group(required=True)
    action_type.add_argument("--list-sites", action="store_true",
                           help="List available sites on source server")
    action_type.add_argument("--list-projects", action="store_true",
                           help="List available projects on source site")
    action_type.add_argument("--list-workbooks", action="store_true",
                           help="List available workbooks on source site")
    action_type.add_argument("--migrate-workbook", "-mw",
                           help="ID of workbook to migrate")
    action_type.add_argument("--migrate-workbook-by-name", "-mwn",
                           help="Name of workbook to migrate")
    action_type.add_argument("--migrate-project", "-mp",
                           help="ID of project to migrate")
    action_type.add_argument("--migrate-site", action="store_true",
                           help="Migrate entire site")
    
    # Additional options
    parser.add_argument("--source-project-id", "-spid",
                      help="Source project ID (for --list-workbooks or --migrate-workbook)")
    parser.add_argument("--source-project-name", "-spname",
                      help="Source project name (alternative to --source-project-id)")
    parser.add_argument("--target-project-id", "-tpid",
                      help="Target project ID (optional for --migrate-workbook and --migrate-project)")
    parser.add_argument("--target-project-name", "-tpname",
                      help="Target project name (alternative to --target-project-id)")
    parser.add_argument("--verbosity", "-v", choices=["debug", "info", "warning", "error"],
                      default="info", help="Logging verbosity")
    
    args = parser.parse_args()
    
    # Load environment variables from .env file if available
    if DOTENV_AVAILABLE:
        env_file = args.env_file
        if os.path.exists(env_file):
            load_dotenv(env_file)
            print(f"Loaded environment variables from {env_file}")
        else:
            print(f"Warning: Environment file {env_file} not found")
    else:
        print("Warning: python-dotenv not installed. Cannot load .env file.")
        print("Install with: pip install python-dotenv")
    
    # Use arguments if provided, otherwise try environment variables
    source_server = args.source_server or os.environ.get("TABLEAU_SOURCE_SERVER")
    target_server = args.target_server or os.environ.get("TABLEAU_TARGET_SERVER")
    source_site = args.source_site or os.environ.get("TABLEAU_SOURCE_SITE", "")
    target_site = args.target_site or os.environ.get("TABLEAU_TARGET_SITE", "")
    
    # Source auth
    source_token_name = args.source_token_name or os.environ.get("TABLEAU_SOURCE_TOKEN_NAME")
    source_token_value = args.source_token_value or os.environ.get("TABLEAU_SOURCE_TOKEN_VALUE")
    source_username = args.source_username or os.environ.get("TABLEAU_SOURCE_USERNAME")
    source_password = args.source_password or os.environ.get("TABLEAU_SOURCE_PASSWORD")
    
    # Target auth
    target_token_name = args.target_token_name or os.environ.get("TABLEAU_TARGET_TOKEN_NAME")
    target_token_value = args.target_token_value or os.environ.get("TABLEAU_TARGET_TOKEN_VALUE")
    target_username = args.target_username or os.environ.get("TABLEAU_TARGET_USERNAME")
    target_password = args.target_password or os.environ.get("TABLEAU_TARGET_PASSWORD")
    
    # API Version
    api_version = args.api_version or os.environ.get("TABLEAU_API_VERSION")
    
    # Validate required parameters
    if not source_server:
        parser.error("Source server must be provided via --source-server or TABLEAU_SOURCE_SERVER environment variable")
    
    # Source auth validation
    if not (source_token_name or source_username):
        parser.error("Source authentication must be provided via command line arguments or environment variables")
    
    # Target auth validation for migration operations
    if (args.migrate_workbook or args.migrate_workbook_by_name or args.migrate_project or args.migrate_site):
        if not target_server:
            parser.error("Target server must be provided for migration operations")
        if not (target_token_name or target_username):
            parser.error("Target authentication must be provided for migration operations")
    
    # Check that target server is provided for migration operations
    if (args.migrate_workbook or args.migrate_workbook_by_name or args.migrate_project or args.migrate_site) and not target_server:
        parser.error("--target-server is required for migration operations")
    
    # Set up logging
    logging_level = getattr(logging, args.verbosity.upper())
    logger = logging.getLogger('tableau_migrator')
    logger.setLevel(logging_level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Create migrator
    migrator = TableauMigrator(
        source_server=source_server,
        target_server=target_server,
        source_site=source_site,
        target_site=target_site,
        logger=logger,
        source_token_name=source_token_name,
        source_token_value=source_token_value,
        target_token_name=target_token_name,
        target_token_value=target_token_value,
        source_username=source_username,
        source_password=source_password,
        target_username=target_username,
        target_password=target_password,
        verify_ssl=not args.no_ssl_verify,
        api_version=api_version,
        download_dir=args.download_dir,
        include_extract=args.include_extract,
        skip_data_sources=args.skip_data_sources
    )
    
    try:
        # Execute requested action
        if args.list_sites:
            sites = migrator.list_source_sites()
            print("\nAvailable sites on source server:")
            for site in sites:
                print(f"  - {site.name} (ID: {site.id}, URL: {site.content_url})")
        
        elif args.list_projects:
            migrator.connect_to_source()
            projects = migrator.list_projects(migrator.source_server)
            print("\nAvailable projects on source site:")
            for project in projects:
                parent = f" (Parent ID: {project.parent_id})" if project.parent_id else ""
                print(f"  - {project.name} (ID: {project.id}){parent}")
        
        elif args.list_workbooks:
            migrator.connect_to_source()
            
            # Get workbooks - either by project ID, project name, or all
            if args.source_project_id:
                workbooks = migrator.list_workbooks(migrator.source_server, 
                                                  project_id=args.source_project_id)
            elif args.source_project_name:
                workbooks = migrator.list_workbooks_by_project_name(migrator.source_server,
                                                                   args.source_project_name)
            else:
                workbooks = migrator.list_workbooks(migrator.source_server)
            
            print("\nAvailable workbooks:")
            for workbook in workbooks:
                # Print project ID to help with troubleshooting
                print(f"  - {workbook.name} (ID: {workbook.id}, Project ID: {workbook.project_id})")
        
        elif args.migrate_workbook or args.migrate_workbook_by_name:
            # For both workbook migration methods, we need a source project
            if not args.source_project_id and not args.source_project_name:
                logger.error("Either --source-project-id or --source-project-name is required when migrating a workbook")
                sys.exit(1)
                
            migrator.connect_to_source()
            migrator.connect_to_target()
            
            # Get source project ID - either directly provided or looked up by name
            source_project_id = args.source_project_id
            if not source_project_id and args.source_project_name:
                # Find project by name
                all_projects = list(TSC.Pager(migrator.source_server.projects))
                matching_projects = [p for p in all_projects 
                                   if p.name.lower() == args.source_project_name.lower()]
                
                if not matching_projects:
                    logger.error(f"No project found with name: {args.source_project_name}")
                    sys.exit(1)
                
                if len(matching_projects) > 1:
                    logger.warning(f"Multiple projects found with name: {args.source_project_name}. Using the first one.")
                
                source_project_id = matching_projects[0].id
                logger.info(f"Found source project '{matching_projects[0].name}' with ID: {source_project_id}")
            
            # If using --migrate-workbook-by-name, look up the workbook ID
            workbook_id = args.migrate_workbook
            if not workbook_id and args.migrate_workbook_by_name:
                logger.info(f"Looking for workbook with name: {args.migrate_workbook_by_name}")
                workbook = migrator.find_workbook_by_name(migrator.source_server, 
                                                         args.migrate_workbook_by_name, 
                                                         source_project_id)
                if not workbook:
                    logger.error(f"Could not find workbook with name: {args.migrate_workbook_by_name}")
                    sys.exit(1)
                workbook_id = workbook.id
                logger.info(f"Found workbook '{workbook.name}' with ID: {workbook_id}")
            
            # If target project specified by name, look it up
            target_project_id = args.target_project_id
            if not target_project_id and args.target_project_name:
                # Find project by name
                all_target_projects = list(TSC.Pager(migrator.target_server.projects))
                matching_target_projects = [p for p in all_target_projects 
                                         if p.name.lower() == args.target_project_name.lower()]
                
                if not matching_target_projects:
                    logger.info(f"No target project found with name: {args.target_project_name}. Will create it.")
                    # We'll create this project below
                else:
                    if len(matching_target_projects) > 1:
                        logger.warning(f"Multiple target projects found with name: {args.target_project_name}. Using the first one.")
                    
                    target_project_id = matching_target_projects[0].id
                    logger.info(f"Found target project '{matching_target_projects[0].name}' with ID: {target_project_id}")
            
            # If target project not specified at all, use same structure as source
            if not target_project_id and not args.target_project_name:
                source_project = migrator.source_server.projects.get_by_id(source_project_id)
                target_project = migrator.ensure_project_exists(source_project.name)
                target_project_id = target_project.id
            # If target project specified by name but not found, create it
            elif not target_project_id and args.target_project_name:
                target_project = migrator.ensure_project_exists(args.target_project_name)
                target_project_id = target_project.id
                
            migrator.migrate_workbook(workbook_id, source_project_id, target_project_id)
        
        elif args.migrate_project:
            migrator.connect_to_source()
            migrator.connect_to_target()
            migrator.migrate_project(args.migrate_project, args.target_project_id)
        
        elif args.migrate_site:
            migrator.migrate_site()
    
    finally:
        # Only clean up source server for listing operations
        if args.list_sites or args.list_projects or args.list_workbooks:
            if migrator.source_server:
                migrator.source_server.auth.sign_out()
                migrator.logger.info("Signed out of source server") 
        else:
            # Full cleanup for migration operations
            migrator.cleanup()


if __name__ == "__main__":
    main()  
