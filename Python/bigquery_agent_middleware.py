from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import vertexai
from vertexai import agent_engines

# --- 1. Configuration ---
PROJECT_ID = "55231573316"
LOCATION = "us-central1"
RESOURCE_ID = "4055046162237882368"
AGENT_RESOURCE_NAME = f"projects/{PROJECT_ID}/locations/{LOCATION}/reasoningEngines/{RESOURCE_ID}"

vertexai.init(project=PROJECT_ID, location=LOCATION)

app = FastAPI(title="Vertex AI Agent API")

# Store sessions in memory for the POC
sessions = {}

# --- 2. Request/Response Models ---
class CreateSessionRequest(BaseModel):
    user_id: str

class QueryRequest(BaseModel):
    user_id: str
    session_id: str
    message: str

class QueryResponse(BaseModel):
    content: str
    session_id: str
    user_id: str

# --- 3. CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 4. Endpoints ---

@app.post("/sessions")
async def create_session(request: CreateSessionRequest):
    """Initializes a session on the remote agent"""
    try:
        # Get the remote app proxy
        remote_app = agent_engines.get(AGENT_RESOURCE_NAME)
        
        # ADK requires a session to be created remotely for stateful conversations
        remote_session = await remote_app.async_create_session(user_id=request.user_id)
        
        # Store locally so our /query endpoint knows it's valid
        sessions[remote_session['id']] = {
            "user_id": request.user_id,
            "session_id": remote_session['id']
        }
        
        return {
            "session_id": remote_session['id'],
            "user_id": request.user_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query", response_model=QueryResponse)
async def query_agent(request: QueryRequest):
    """Uses your exact logic to stream and aggregate the response"""
    try:
        # 1. Verify session exists
        if request.session_id not in sessions:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # 2. Get the remote app proxy
        remote_app = agent_engines.get(AGENT_RESOURCE_NAME)
        
        # 3. Stream and collect the response
        final_text = ""
        async for event in remote_app.async_stream_query(
            user_id=request.user_id,
            session_id=request.session_id,
            message=request.message,
        ):
            content = event.get("content")
            if content and content.get("role") == "model":
                for part in content.get("parts", []):
                    if "text" in part:
                        final_text += part["text"]
        
        return QueryResponse(
            content=final_text,
            session_id=request.session_id,
            user_id=request.user_id
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)



## Dockerfile#############################################
###############################################
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set host and port for Cloud Run
ENV PORT=8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

############ requirments.txt###########################
fastapi
uvicorn
google-cloud-aiplatform[reasoningengine,adk]
python-dotenv
