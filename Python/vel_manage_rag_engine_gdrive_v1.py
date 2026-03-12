# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import io
import logging
import pathlib
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.auth import default, exceptions

import vertexai
from vertexai.preview import rag

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# Manually changeable variables
CORPUS_DISPLAY_NAME = "vel_google_drive_folder_upload_v1"
GOOGLE_DRIVE_FOLDER_ID = "1fRCZgWM7_NcRclZmMw7ltDwumrnKeFon" # <--- IMPORTANT: SET THIS
TEMP_DIR = "/tmp/rag_downloads"

# Environment-dependent variables
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
GOOGLE_CLOUD_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
# --- End Configuration ---


# --- Logging Setup ---
def setup_logging():
    """Configures the root logger for the script."""
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Optional: Integrate with Google Cloud Logging
    try:
        import google.cloud.logging
        client = google.cloud.logging.Client()
        client.setup_logging(log_level=logging.INFO)
        logging.info("Successfully integrated with Google Cloud Logging.")
    except Exception as e:
        logging.debug(f"Could not integrate with Google Cloud Logging: {e}")
        logging.info("Running with standard Python console logging.")

    return logging.getLogger(__name__)

logger = setup_logging()
# --- End Logging Setup ---


def get_drive_service():
    """Builds and returns an authenticated Google Drive service client."""
    logger.info("Authenticating with Google Drive API...")
    try:
        creds, project = default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
        logger.info(f"Authenticated using Application Default Credentials for project: {project or GOOGLE_CLOUD_PROJECT}")
        return build('drive', 'v3', credentials=creds)
    except exceptions.DefaultCredentialsError:
        logger.critical(
            "Authentication failed. Please configure Application Default Credentials "
            "(e.g., by running 'gcloud auth application-default login')."
        )
        exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during authentication: {e}", exc_info=True)
        exit(1)


def list_drive_files(drive_service, folder_id: str) -> list:
    """Lists all files within a specified Google Drive folder."""
    logger.info(f"Listing files in Google Drive folder ID: {folder_id}")
    files = []
    page_token = None
    try:
        while True:
            response = drive_service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token
            ).execute()
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        logger.info(f"Found {len(files)} file(s) in Drive folder.")
        return files
    except HttpError as error:
        logger.error(f"Failed to list Drive files: {error}", exc_info=True)
        return []


def export_google_doc(drive_service, doc_id: str, doc_name: str, temp_dir: str) -> str | None:
    """Exports a Google Doc to a local PDF file."""
    # Sanitize filename and create the full path
    safe_filename = "".join(c for c in doc_name if c.isalnum() or c in (' ', '.', '_')).rstrip()
    output_path = pathlib.Path(temp_dir) / f"{safe_filename}.pdf"
    
    logger.info(f"Exporting Google Doc '{doc_name}' (ID: {doc_id}) to '{output_path}'")
    try:
        request = drive_service.files().export_media(fileId=doc_id, mimeType='application/pdf')
        with io.FileIO(output_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logger.debug(f"Download progress for {doc_id}: {int(status.progress() * 100)}%")
        logger.info(f"Successfully exported '{doc_name}' to '{output_path}'.")
        return str(output_path)
    except HttpError as error:
        logger.error(f"Failed to export Google Doc '{doc_name}': {error}", exc_info=True)
        return None


def get_or_create_corpus(display_name: str) -> rag.RagCorpus:
    """Retrieves an existing RAG corpus by display name or creates it if not found."""
    logger.info(f"Checking for RAG corpus with display name: '{display_name}'")
    for corpus in rag.list_corpora():
        if corpus.display_name == display_name:
            logger.info(f"Found existing corpus '{display_name}' (ID: {corpus.name}).")
            return corpus
    
    logger.info(f"Corpus '{display_name}' not found. Creating a new one.")
    try:
        new_corpus = rag.create_corpus(display_name=display_name)
        logger.info(f"Successfully created new corpus '{display_name}' (ID: {new_corpus.name}).")
        return new_corpus
    except Exception as e:
        logger.critical(f"Failed to create corpus '{display_name}': {e}", exc_info=True)
        exit(1)


def get_rag_file_mapping(corpus_name: str) -> dict:
    """Returns a dictionary mapping display_name to the file object for a given corpus."""
    logger.info(f"Fetching file list for corpus: {corpus_name}")
    try:
        files = rag.list_files(corpus_name=corpus_name)
        mapping = {f.display_name: f for f in files}
        logger.info(f"Found {len(mapping)} files in the corpus.")
        return mapping
    except Exception as e:
        logger.error(f"Could not list files in corpus '{corpus_name}': {e}", exc_info=True)
        return {}


def sync_drive_folder_to_corpus(drive_service, corpus: rag.RagCorpus, folder_id: str, temp_dir: str):
    """
    Synchronizes files from a Google Drive folder to a RAG corpus.
    - Uploads new files from Drive.
    - Deletes files from the corpus that are no longer in Drive.
    """
    logger.info(f"Starting sync from Drive folder '{folder_id}' to corpus '{corpus.display_name}'.")
    
    # 1. Get current state of both Drive folder and RAG corpus
    drive_files = list_drive_files(drive_service, folder_id)
    drive_file_names = {f['name'] for f in drive_files}
    
    rag_file_mapping = get_rag_file_mapping(corpus.name)
    rag_file_display_names = set(rag_file_mapping.keys())

    # 2. Identify files to delete from the corpus
    files_to_delete = rag_file_display_names - drive_file_names
    if files_to_delete:
        logger.info(f"Found {len(files_to_delete)} file(s) to delete from the corpus: {files_to_delete}")
        for display_name in files_to_delete:
            file_to_delete = rag_file_mapping[display_name]
            try:
                logger.info(f"Deleting file: '{display_name}' (ID: {file_to_delete.name})")
                rag.delete_file(name=file_to_delete.name)
            except Exception as e:
                logger.error(f"Failed to delete file '{display_name}': {e}", exc_info=True)
    else:
        logger.info("No files need to be deleted from the corpus.")

    # 3. Identify files to upload to the corpus
    files_to_upload = [f for f in drive_files if f['name'] not in rag_file_display_names]
    if files_to_upload:
        logger.info(f"Found {len(files_to_upload)} new file(s) to upload from Drive.")
        # Ensure the temporary directory exists
        pathlib.Path(temp_dir).mkdir(parents=True, exist_ok=True)

        for drive_file in files_to_upload:
            doc_id = drive_file['id']
            doc_name = drive_file['name']
            
            # Export the file locally
            local_path = export_google_doc(drive_service, doc_id, doc_name, temp_dir)
            
            if local_path:
                # Upload the exported file to the RAG corpus
                try:
                    logger.info(f"Uploading '{local_path}' to corpus with display name '{doc_name}'.")
                    rag.upload_file(
                        corpus_name=corpus.name,
                        path=local_path,
                        display_name=doc_name,
                        description=f"Uploaded from Google Drive file ID: {doc_id}"
                    )
                    logger.info(f"Successfully uploaded '{doc_name}'.")
                except Exception as e:
                    logger.error(f"Failed to upload file '{local_path}': {e}", exc_info=True)
                finally:
                    # Clean up the temporary file
                    try:
                        os.remove(local_path)
                        logger.info(f"Removed temporary file: '{local_path}'")
                    except OSError as e:
                        logger.warning(f"Could not remove temporary file '{local_path}': {e}")
    else:
        logger.info("No new files from Drive need to be uploaded.")

    logger.info("Sync process completed.")


def main():
    """Main execution function."""
    logger.info("Starting RAG corpus management script.")

    if not GOOGLE_CLOUD_PROJECT:
        logger.critical("GOOGLE_CLOUD_PROJECT environment variable not set. Exiting.")
        exit(1)
    
    if "replace_with_your_folder_id" in GOOGLE_DRIVE_FOLDER_ID:
        logger.critical("Please set the GOOGLE_DRIVE_FOLDER_ID variable in the script. Exiting.")
        exit(1)

    # Initialize Vertex AI
    vertexai.init(project=GOOGLE_CLOUD_PROJECT, location=GOOGLE_CLOUD_LOCATION)

    # Get or create the target RAG corpus
    corpus = get_or_create_corpus(CORPUS_DISPLAY_NAME)

    # Get the Google Drive service client
    drive_service = get_drive_service()

    # Synchronize the contents of the Drive folder with the corpus
    sync_drive_folder_to_corpus(drive_service, corpus, GOOGLE_DRIVE_FOLDER_ID, TEMP_DIR)

    # List final files in the corpus for verification
    logger.info(f"Final check: Listing files in corpus '{corpus.display_name}':")
    final_files = rag.list_files(corpus_name=corpus.name)
    if final_files:
        for f in final_files:
            logger.info(f"  - File: {f.display_name} (ID: {f.name})")
    else:
        logger.info("Corpus is empty.")

    logger.info("Script finished.")


if __name__ == "__main__":
    main()
