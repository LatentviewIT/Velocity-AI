import os
import vertexai
from vertexai.preview import reasoning_engines
from langchain_google_vertexai import ChatVertexAI
from google.cloud import bigquery

# Initialize Google Cloud clients
PROJECT_ID = "project-nirvana-405904"
LOCATION = "us-central1"

# You need a GCS bucket to stage the code.
# E.g., export STAGING_BUCKET="gs://my-project-nirvana-staging-bucket"
# Or fallback to a generic name based on project
STAGING_BUCKET = os.environ.get("STAGING_BUCKET", f"gs://{PROJECT_ID}-reasoning-engine-staging")

print(f"Initializing Vertex AI for project {PROJECT_ID} in {LOCATION}...")
print(f"Using staging bucket: {STAGING_BUCKET}")
vertexai.init(project=PROJECT_ID, location=LOCATION, staging_bucket=STAGING_BUCKET)

def run_bigquery_analysis(question: str) -> str:
    """Takes a natural language question about the vel_csv_signals_validation_002 table, 
    generates a BigQuery SQL query to answer it, executes the query, and synthesizes 
    a conversational response based on the output data."""

    llm = ChatVertexAI(model_name="gemini-2.0-flash-001", temperature=0)
    
    # 1. Use Gemini to generate the SQL query
    sql_generation_prompt = f"""
    You are an expert BigQuery SQL data analyst.
    Write a Google Standard SQL query to answer the user's question based on the following table schema.
    
    Table Name: `{PROJECT_ID}.vel_csv_schema.vel_csv_signals_validation_002`
    
    Schema:
    - transcript_id (STRING REQUIRED)
    - transcript_is_valid (BOOLEAN NULLABLE)
    - extraction_quality_score (FLOAT NULLABLE)
    - completeness_score (FLOAT NULLABLE)
    - missed_signals_detected (BOOLEAN NULLABLE)
    - missed_signals_summary (STRING REPEATED)
    - validation_report (JSON NULLABLE)
    - audit_timestamp (TIMESTAMP NULLABLE)
    - spm_name (STRING NULLABLE)
    - creator_id (STRING NULLABLE)
    - creator_region (STRING NULLABLE)
    
    CRITICAL INSTRUCTIONS:
    - Only output valid Google Standard SQL.
    - DO NOT include markdown formatting like ```sql or ```. Only the raw SQL string.
    - Do not invent any columns not in the schema.
    
    User Question: {question}
    """
    
    sql_response = llm.invoke(sql_generation_prompt)
    
    # Clean up formatting if the LLM adds markdown backticks despite instructions
    sql_query = sql_response.content.strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:]
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3]
    sql_query = sql_query.strip()
    
    print(f"Generated SQL: {sql_query}")
    
    # 2. Execute the query
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query_job = client.query(sql_query)
        results = query_job.result()
        
        # Format the results
        output = []
        for row in results:
            output.append(dict(row.items()))
            
        if not output:
            data_result_str = "The query executed successfully but returned zero rows."
        else:
            data_result_str = str(output)
            
    except Exception as e:
        data_result_str = f"Error executing BigQuery SQL: {str(e)}\nAttempted query was: {sql_query}"

    # 3. Synthesize the final conversational response
    synthesis_prompt = f"""
    You are a helpful data assistant for the `vel_csv_signals_validation_002` dataset.
    The user asked: "{question}"
    
    We ran a BigQuery analysis to get the raw data which resulted in:
    {data_result_str}
    
    Please present a comprehensive, natural language answer based on the raw data. 
    Explain what the data means gracefully.
    """
    
    final_response = llm.invoke(synthesis_prompt)
    return final_response.content


# Define the Reasoning Engine Agent Class
class BQAgentTest:
    def __init__(self):
        # Expose dependencies cleanly to remote container deserialization
        global ChatVertexAI, bigquery, run_bigquery_analysis
        print("Initializing BQAgentTest...")

    def set_up(self):
        """Called by Reasoning Engine before first execution."""
        pass

    def stream_query(self, session_id: str, query: str):
        """Standard method expected by many ADK / frontend integrations."""
        return self.query(session_id, query)

    def query(self, session_id: str, query: str):
        """The main method to answer natural language queries."""
        print(f"Processing query for session {session_id}: {query}")
        
        try:
            answer = run_bigquery_analysis(query)
            return {
                "output": {
                    "text": answer
                }
            }
        except Exception as e:
            return {
                "output": {
                    "text": f"Agent encountered an error: {str(e)}"
                }
            }

# --- Deployment Logic (Runs locally to deploy the class) ---
if __name__ == "__main__":
    print("WARNING: This will deploy the agent to GCP Vertex AI.")
    print("Ensure you have `google-cloud-aiplatform` and `langchain-google-vertexai` installed.")
    print("---------------------------------------------------------")
    
    # Instantiate the agent locally
    agent_instance = BQAgentTest()
    
    # Deploy to Reasoning Engine
    print("\nDeploying to Reasoning Engine. This can take several minutes...")
    
    remote_agent = reasoning_engines.ReasoningEngine.create(
        reasoning_engine=agent_instance,
        display_name="bq-agent-test",
        description="Agent that queries vel_csv_signals_validation_002 table via BigQuery",
        requirements=[
            "google-cloud-aiplatform",
            "google-cloud-bigquery",
            "langchain-google-vertexai",
        ]
    )
    
    print("\n================== DEPLOYMENT SUCCESS ==================")
    print("Save this Reasoning Engine ID:")
    print(remote_agent.resource_name)
    print("========================================================")
