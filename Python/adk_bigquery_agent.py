import os
import google.auth
from google.adk.agents import Agent
from google.adk.apps import App
from google.adk.tools.bigquery import BigQueryCredentialsConfig
from google.adk.tools.bigquery import BigQueryToolset
from google.adk.tools.bigquery.config import BigQueryToolConfig
from google.adk.tools.bigquery.config import WriteMode

# --- 1. Environment Configuration ---
# The Starter Pack container might not have these set, so we force them here
# to ensure the "Exact Same Logic" works in the cloud.
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "1"

# Default to us-central1 if not set in the environment
if "GOOGLE_CLOUD_LOCATION" not in os.environ:
    os.environ["GOOGLE_CLOUD_LOCATION"] = "us-central1"

PROJECT_ID = google.auth.default()[1]
MODEL_NAME = "gemini-2.0-flash"

print(f"🚀 Initializing BigQuery Agent for project: {PROJECT_ID}")

# --- 2. Tool Configuration ---
# Blocked mode prevents accidental data modification
tool_config = BigQueryToolConfig(write_mode=WriteMode.BLOCKED)

# --- 3. Credentials Setup ---
# We use Application Default Credentials (ADC).
# In Cloud Run, this automatically uses the service account.
application_default_credentials, _ = google.auth.default()
credentials_config = BigQueryCredentialsConfig(
    credentials=application_default_credentials
)

# --- 4. Instantiate Toolset ---
bigquery_toolset = BigQueryToolset(
    credentials_config=credentials_config, 
    bigquery_tool_config=tool_config
)

# --- 5. Define the Agent ---
# The Starter Pack expects a variable named 'root_agent' (or 'agent') to be exported.
root_agent = Agent(
    name="BigQuery_Agent",
    model=MODEL_NAME,
    description="Agent to answer questions about BigQuery data and execute SQL.",
    instruction="""
        You are a data science agent with access to BigQuery tools.
        1. Always list datasets or tables first if you don't know the exact names.
        2. Use the tools provided to query data.
        3. If a query fails, try to correct the SQL syntax.
    """,
    tools=[bigquery_toolset],
)
app = App(
    root_agent=root_agent,
    name="app",
)
