# /home/sreekiran_reghunadh/RAG-data/create_and_upload_data_RAG_corpus.py

import os
import io
import logging
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.auth import default # For Application Default Credentials

# --- Configure Logging ---
# Create a logger instance
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Set the default logging level

# Create a console handler and set its format
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
if not logger.handlers: # Avoid adding duplicate handlers if script is run multiple times
    logger.addHandler(handler)

# Optional: Integrate with Google Cloud Logging if running in a GCP environment
# You might need to install 'google-cloud-logging': pip install google-cloud-logging
try:
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging(log_level=logging.INFO) # Captures INFO and higher to Cloud Logging
    logger.info("Integrated with Google Cloud Logging.")
except Exception as e:
    logger.debug(f"Could not integrate with Google Cloud Logging: {e}")
    logger.info("Running with standard Python console logging.")

# --- End Logging Configuration ---


# --- Load Environment Variables ---
load_dotenv()

GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
# GOOGLE_APPLICATION_CREDENTIALS is usually picked up automatically by google.auth.default()
# but explicitly getting it can be useful for debugging or fallback logic.
# SERVICE_ACCOUNT_KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not GOOGLE_CLOUD_PROJECT:
    logger.critical("GOOGLE_CLOUD_PROJECT not found in .env file or environment. Exiting.")
    exit(1) # Exit if critical environment variable is missing
else:
    logger.info(f"Loaded GOOGLE_CLOUD_PROJECT: {GOOGLE_CLOUD_PROJECT}")
# --- End Environment Variable Loading ---


def get_drive_service():
    """Builds and returns a Google Drive service."""
    logger.info("Attempting to get Google Drive service credentials.")
    creds = None
    try:
        # Use Application Default Credentials
        # This will automatically pick up credentials from your environment
        # and should use GOOGLE_CLOUD_PROJECT for the quota project.
        creds, project = default()
        logger.info(f"Authenticated using Application Default Credentials with project: {project or GOOGLE_CLOUD_PROJECT}")
    except Exception as e:
        logger.error(f"Error getting default credentials: {e}", exc_info=True)
        # Fallback for local development if GOOGLE_APPLICATION_CREDENTIALS is explicitly set
        service_account_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if service_account_key_path and os.path.exists(service_account_key_path):
            logger.info("Attempting to authenticate using GOOGLE_APPLICATION_CREDENTIALS service account key.")
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(
                service_account_key_path,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            logger.info("Authenticated using service account key from file.")
        else:
            logger.critical("No valid Google Cloud credentials found. Please set GOOGLE_APPLICATION_CREDENTIALS "
                            "or ensure ADC is configured. Exiting.")
            exit(1) # Exit if authentication fails

    return build('drive', 'v3', credentials=creds)

def export_google_doc_locally(drive_service, doc_id, output_path, mime_type='application/pdf'):
    """Exports a Google Doc to a local file."""
    logger.info(f"Starting export of Google Doc ID: {doc_id} to {output_path} as {mime_type}.")
    try:
        request = drive_service.files().export_media(fileId=doc_id, mimeType=mime_type)
        fh = io.FileIO(output_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.debug(f"Download progress for {doc_id}: {int(status.progress() * 100)}%.")
        logger.info(f"Successfully exported {doc_id} to {output_path}.")
        return output_path
    except HttpError as error:
        logger.error(f"HttpError during Google Doc export: {error}", exc_info=True)
        if error.resp.status == 403:
            logger.error("This is likely a 403 Forbidden error. Common causes:")
            logger.error("1. Google Drive API is not enabled in your Google Cloud project.")
            logger.error("2. The service account/user running this script lacks Viewer access to the Google Doc.")
            logger.error("3. GOOGLE_CLOUD_PROJECT (quota project) might not be correctly set or picked up.")
        raise # Re-raise the exception after logging details
    except Exception as e:
        logger.error(f"An unexpected error occurred during Google Doc export: {e}", exc_info=True)
        raise

def list_corpus_files(rag_client, corpus_name):
    """Lists files within a given RAG corpus."""
    logger.info(f"Attempting to list files in RAG corpus: {corpus_name}")
    try:
        
        files = list(rag_client.list_files(corpus_name=corpus_name))
        if files:
            logger.info(f"Found {len(files)} files in corpus '{corpus_name}':")
            for f in files:
                logger.info(f"  - File: {f.display_name} (ID: {f.name})")
        else:
            logger.info(f"No files found in corpus '{corpus_name}'.")
        return files
    except RuntimeError as e:
        logger.critical(f"RuntimeError in list_corpus_files: {e}", exc_info=True)
        logger.critical("This often indicates issues with authentication/service account setup for Vertex AI RAG.")
        exit(1) # Exit if this critical RAG operation fails
    except Exception as e:
        logger.error(f"An unexpected error occurred while listing RAG corpus files: {e}", exc_info=True)
        raise

def create_rag_corpus(rag_client, display_name):
    """Creates a new RAG corpus."""
    logger.info(f"Attempting to create RAG corpus with display name: '{display_name}'")
    try:
        corpus = rag_client.create_corpus(display_name=display_name)
        logger.info(f"Created new corpus with display name '{corpus.display_name}' (ID: {corpus.name}).")
        return corpus
    except Exception as e:
        logger.error(f"Failed to create RAG corpus '{display_name}': {e}", exc_info=True)
        raise

def main():
    logger.info("Starting RAG data creation and upload script.")

    # Initialize Google Drive service
    drive_service = get_drive_service()

    import vertexai
    vertexai.init(project=GOOGLE_CLOUD_PROJECT, location=os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1"))
    from vertexai.preview.rag import rag_data as rag_client

    # Example: Create a new corpus (or get an existing one)
    corpus_display_name = "vel_automated-RAG-corpus_1"
    try:
        # Check if corpus already exists to avoid creation error
        # (This is a simplified check, a more robust solution would list and match)
        corpus = None
        for c in rag_client.list_corpora():
            if c.display_name == corpus_display_name:
                corpus = c
                logger.info(f"Found existing corpus '{corpus_display_name}' (ID: {corpus.name}).")
                break
        if not corpus:
            corpus = create_rag_corpus(rag_client, corpus_display_name)
    except Exception as e:
        logger.critical(f"Fatal error with RAG corpus management: {e}", exc_info=True)
        exit(1)

    # Example: Export a Google Doc (change the link for testing other docs)
    google_doc_id = "1H-JGRDZt9LAJ7rxyE1cJrCaNkPakI-O7xMjgXCiMm-U"
    local_output_file = "/tmp/exported_doc.pdf"

    try:
        local_file_path = export_google_doc_locally(
            drive_service,
            google_doc_id,
            local_output_file,
            mime_type='application/pdf'
        )
        logger.info(f"Exported doc saved to: {local_file_path}")
    except Exception as e:
        logger.critical(f"Failed to export Google Doc: {e}", exc_info=True)
        exit(1)

    try:
        rag_client.upload_file(corpus_name=corpus.name, path=local_output_file, display_name="IIS_from_python")
        logger.info(f"Simulating upload of {local_output_file} to RAG corpus '{corpus.display_name}'.")
        logger.info("File upload to RAG corpus simulation successful.")
    except Exception as e:
        logger.error(f"Error uploading {local_output_file} to RAG corpus: {e}", exc_info=True)

    # Example: List files in the corpus after potential upload
    list_corpus_files(rag_client, corpus.name)

    logger.info("Script finished.")

if __name__ == "__main__":
    main()
