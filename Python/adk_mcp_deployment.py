from google.adk import Agent
from google.adk.apps import App
from google.adk.tools.toolbox_toolset import ToolboxToolset
from toolbox_adk import CredentialStrategy

# Your deployed MCP Server URL
TOOLBOX_URL = "https://mcp-server-55231573316.us-central1.run.app"

# Configure the toolset with Workload Identity
# This automatically uses the "Cloud Run Invoker" permission we just verified!
toolset = ToolboxToolset(
    server_url=TOOLBOX_URL,
    credentials=CredentialStrategy.workload_identity(target_audience=TOOLBOX_URL)
)

# Define the Agent
root_agent = Agent(
    name='bigquery_agent',
    model='gemini-2.0-flash-001',
    instruction="You are a helpful assistant that can access BigQuery data using the provided tools. Always use the tools to answer questions about datasets and tables.",
    tools=[toolset],
)

# Initialize the App
app = App(root_agent=root_agent, name="my_agent")
