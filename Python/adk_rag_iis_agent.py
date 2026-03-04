# ruff: noqa
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

import datetime
from zoneinfo import ZoneInfo

from google.adk.agents import Agent
from google.adk.apps import App
from google.adk.models import Gemini
from google.genai import types

import vertexai
from vertexai.preview import rag
from google.adk import Agent
from google.adk.apps import App

# --- Configuration ---
PROJECT_ID = "project-nirvana-405904"
RAG_REGION = "europe-west2"       # Where your Data is (London)
MODEL_REGION = "us-central1"      # Where the Model is (US)
RAG_CORPUS_RESOURCE = "projects/project-nirvana-405904/locations/europe-west2/ragCorpora/6917529027641081856"

# 1. Global Init: Set to US-CENTRAL1 so the Agent can find the Model
vertexai.init(project=PROJECT_ID, location=MODEL_REGION)

# 2. Define the Retrieval Tool with "Region Hopping"
def search_rag_corpus(query: str) -> str:
    """
    Searches the RAG corpus for relevant documentation.
    """
    try:
        # --- REGION SWITCH START ---
        # Temporarily point Vertex AI to London to find the RAG Corpus
        vertexai.init(project=PROJECT_ID, location=RAG_REGION)
        
        response = rag.retrieval_query(
            rag_resources=[
                rag.RagResource(
                    rag_corpus=RAG_CORPUS_RESOURCE,
                )
            ],
            text=query,
            similarity_top_k=5,
            vector_distance_threshold=0.5,
        )
        
        # Format the output
        if not response.contexts.contexts:
            result = "No relevant information found in the RAG corpus."
        else:
            formatted_context = "\n\n".join(
                [f"Source {i+1}:\n{ctx.text}" for i, ctx in enumerate(response.contexts.contexts)]
            )
            result = f"Found the following relevant information:\n{formatted_context}"
            
        return result

    except Exception as e:
        return f"Error querying RAG corpus: {str(e)}"
    
    finally:
        # --- REGION SWITCH BACK ---
        # CRITICAL: Always switch back to US-CENTRAL1 so the Agent (Model) keeps working
        vertexai.init(project=PROJECT_ID, location=MODEL_REGION)

# 3. Define the Root Agent (Model runs in US-CENTRAL1)
root_agent = Agent(
    name='rag_agent',
    model='gemini-2.0-flash-001', 
    instruction=(
        "You are a helpful AI assistant connected to a specialized knowledge base. "
        "Always use the 'search_rag_corpus' tool to find information before answering user questions. "
        "Base your answers strictly on the information returned by the tool."
    ),
    tools=[search_rag_corpus],
)

app = App(root_agent=root_agent, name="my_agent")

if __name__ == '__main__':
    print("Agent initialized. Ready for deployment.")
