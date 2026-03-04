### work in progress -> authentication to be setup correctly (.env file to be added by the user)

import io
import os
import tempfile

import vertexai
from dotenv import load_dotenv, set_key
from google.api_core.exceptions import ResourceExhausted
from google.auth import default
from vertexai.preview import rag
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Load environment variables from .env file
load_dotenv()

# --- Please fill in your configurations ---
# Retrieve the PROJECT_ID from the environmental variables.
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
if not PROJECT_ID:
    raise ValueError(
        "GOOGLE_CLOUD_PROJECT environment variable not set. Please set it in your .env file."
    )
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION")
if not LOCATION:
    raise ValueError(
        "GOOGLE_CLOUD_LOCATION environment variable not set. Please set it in your .env file."
    )
QUOTA_PROJECT_ID = os.getenv("GOOGLE_CLOUD_QUOTA_PROJECT")
if not QUOTA_PROJECT_ID:
    raise ValueError(
        "GOOGLE_CLOUD_QUOTA_PROJECT environment variable not set. Please set it in your .env file."
    )
CORPUS_DISPLAY_NAME = "vel_automated-RAG-corpus_1"
CORPUS_DESCRIPTION = "Testing corpus programmatically uploaded from Google Doc link (IIS doc) using Python"
# The URL of the Google Doc to import.
GOOGLE_DOC_URL = "https://docs.google.com/document/d/1H-JGRDZt9LAJ7rxyE1cJrCaNkPakI-O7xMjgXCiMm-U/edit?tab=t.5xo7802riwbz#heading=h.voq3a2n94cpe"  # e.g., "https://docs.google.com/document/d/12345..."

ENV_FILE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", ".env")
)


# --- Start of the script ---
def initialize_vertex_ai():
    credentials, _ = default()
    vertexai.init(
        project=PROJECT_ID, location=LOCATION, credentials=credentials
    )


def create_or_get_corpus():
    """Creates a new corpus or retrieves an existing one."""
    embedding_model_config = rag.EmbeddingModelConfig(
        publisher_model="publishers/google/models/text-embedding-004"
    )
    existing_corpora = rag.list_corpora()
    corpus = None
    for existing_corpus in existing_corpora:
        if existing_corpus.display_name == CORPUS_DISPLAY_NAME:
            corpus = existing_corpus
            print(
                f"Found existing corpus with display name '{CORPUS_DISPLAY_NAME}'"
            )
            break
    if corpus is None:
        corpus = rag.create_corpus(
            display_name=CORPUS_DISPLAY_NAME,
            description=CORPUS_DESCRIPTION,
            embedding_model_config=embedding_model_config,
        )
        print(f"Created new corpus with display name '{CORPUS_DISPLAY_NAME}'")
    return corpus


def export_google_doc_locally(doc_url: str, temp_dir: str) -> str:
    """Exports a Google Doc as a PDF and saves it to a local temporary directory.

    Args:
        doc_url: The URL of the Google Doc.
        temp_dir: The temporary directory to save the file in.

    Returns:
        The local path of the saved PDF file.
    """
    print(f"Exporting Google Doc: {doc_url}")

    # Authenticate and build the Drive API service
    credentials, _ = default()
    drive_service = build("drive", "v3", credentials=credentials)

    # Extract the Document ID from the URL
    doc_id = doc_url.split("/d/")[1].split("/")[0]
    file_path = os.path.join(temp_dir, f"{doc_id}.pdf")

    # Export the Google Doc as a PDF
    request = drive_service.files().export_media(
        fileId=doc_id, mimeType="application/pdf"
    )
    with open(file_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    print(f"Successfully exported Google Doc to {file_path}")
    return file_path


def upload_file_to_corpus(corpus_name, file_path, display_name):
    """Uploads a local file to the specified corpus."""
    print(f"Uploading {file_path} to corpus...")
    try:
        rag.upload_file(
            corpus_name=corpus_name,
            path=file_path,
            display_name=display_name,
            description="File uploaded from a Google Doc.",
        )
        print(f"Successfully uploaded file: {display_name}")
        return True
    except ResourceExhausted as e:
        print(f"Error importing files: {e}")
        print(
            "\nThis error suggests that you have exceeded the API quota for the embedding model."
        )
        print("This is common for new Google Cloud projects.")
        print(
            "Please see the 'Troubleshooting' section in the README.md for instructions on how to request a quota increase."
        )
        return None
    except Exception as e:
        print(f"Error uploading file {display_name}: {e}")
        return False


def update_env_file(corpus_name, env_file_path):
    """Updates the .env file with the corpus name."""
    try:
        set_key(env_file_path, "RAG_CORPUS", corpus_name)
        print(f"Updated RAG_CORPUS in {env_file_path} to {corpus_name}")
    except Exception as e:
        print(f"Error updating .env file: {e}")


def list_corpus_files(corpus_name):
    """Lists files in the specified corpus."""
    files = list(rag.list_files(corpus_name=corpus_name))
    print(f"Total files in corpus: {len(files)}")
    for file in files:
        print(f"File: {file.display_name} - {file.name}")


def main():
    initialize_vertex_ai()
    corpus = create_or_get_corpus()

    # Update the .env file with the corpus name
    update_env_file(corpus.name, ENV_FILE_PATH)

    # Create a temporary directory to store the downloaded PDF
    with tempfile.TemporaryDirectory() as temp_dir:
        # Export the Google Doc to the temporary local directory
        local_file_path = export_google_doc_locally(
            doc_url=GOOGLE_DOC_URL, temp_dir=temp_dir
        )
        print(f"File downloaded from Google Cloud, Local file path: {local_file_path}")

        # Upload the local file to the corpus
        upload_file_to_corpus(
            corpus_name=corpus.name,
            file_path=local_file_path,
            display_name=os.path.basename(local_file_path),
        )
    # List all files in the corpus
    list_corpus_files(corpus_name=corpus.name)


if __name__ == "__main__":
    main()
