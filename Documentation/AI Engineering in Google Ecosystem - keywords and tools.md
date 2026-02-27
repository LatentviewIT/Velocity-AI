## **1\. Core Definitions & Concepts**

### **LLM vs. AI Agents**

* **LLM (Large Language Model):** A passive "reasoning engine" (e.g., Gemini). It predicts text but doesn't *act* unless prompted.  
* **AI Agent:** An active system that uses an LLM to plan, use tools, and autonomously complete a specific goal.

**The Key Difference:** An LLM *describes* the solution; an Agent *executes* the solution.

### **Agentic Development Concepts**

* **RAG (Retrieval-Augmented Generation):** Connecting the agent to external data (PDFs, DBs) to ensure accuracy.  
* **Tool Use / Function Calling:** The mechanism where an agent triggers an external API or code snippet.  
* **Chain of Thought (CoT):** The agent's ability to "reason" by breaking a complex request into smaller logical steps.  
  ---

  ## **2\. The Agentic Developer Toolkit** 

To build agents, you need more than just a model; you need a structured development environment.

### **ADK (Agent Development Kit)**

Google's open-source framework for building multi-agent systems.

* **Orchestration:** Defines how agents collaborate (e.g., a "Researcher Agent" handing off to a "Writer Agent").  
* **State Engine:** Manages "Sessions" so the agent remembers what happened ten steps ago.  
* **Multi-Modal:** Supports bidirectional audio and video streaming for natural interactions.

  ### **Interactions API**

A specialized API designed specifically for agent loops rather than simple chat.

* **Native Thought Separation:** Explicitly separates the agent's "internal reasoning" from the final response.  
* **Background Execution:** Allows an agent to run a 5-minute task in the background without the client timing out.  
* **Server-Side State:** Offloads conversation history to the cloud to reduce your token costs and latency.

  ### **SDKs & CLI**

* **Vertex AI SDK (Python/Go):** The library used to write the code that connects your app to Google's AI services.  
* **Gemini CLI:** A command-line tool for developers to test prompts, trigger agent workflows, and manage MCP servers directly from the terminal.  
  ---


  ## **3\. The Core AI Workspace**

  ### **Google AI Studio**

The **"fastest way to prototype."** It is a web-based tool for developers to quickly test prompts and build lightweight agents.

* **Functionality:** Best for rapid experimentation and getting an API key to start "Vibe Coding."  
* Now supports the **Agentic Vision** and **Thinking** features of the Gemini 3.1 models directly in the UI.

  ### **Vertex AI Studio**

The **"enterprise-grade workshop."** This is where you go when you are ready to move from a prototype to a production-ready application.

* **Features:** Full model lifecycle management, monitoring, and integration with BigQuery and Google Drive.  
* **Vertex AI Agent Builder:** A suite within Vertex that includes a **Low-Code Designer** to visually map out how your agents should behave.

  ### **Google Antigravity**

An agent-first IDE (a specialized VS Code fork). It treats agents as first-class citizens, allowing you to debug an agent's "thoughts" alongside your code.

### **Gemini Models**

| Model | Use Case |
| :---- | :---- |
| **Gemini 3 Pro** | The "Heavy Lifter" for complex planning and multi-agent coordination. |
| **Gemini 3 Flash** | The "Speedster" for simple tasks, low-latency UI, and cost-efficiency. |
| **Gemini Deep Research** | A built-in agent for long-horizon research and document synthesis. |

---

## **4\. The Model & Tool Ecosystem**

### **Model Garden**

A curated library within Vertex AI. It’s essentially an **"App Store for Models."**

* **First-Party:** All Gemini models (3.1 Pro, 3 Flash, etc.).  
* **Third-Party:** Access to Llama, Claude, and Mistral models if your project requires a non-Google model.  
* **Task-Specific:** Specialized models for things like medical data (MedLM) or retail (UCP).

  ### **Agent Garden (New)**

A specialized section of the Model Garden that provides **pre-built agents** for common tasks (e.g., a "Customer Support Agent") that you can deploy and customize immediately.

---

## **5\. Security & Governance**

### **Model Armor**

The **"Firewall for AI."** It is a security layer that proactively screens every prompt and every response.

* **Prompt Injection:** Blocks users from trying to trick the agent into ignoring its rules.  
* **PII Redaction:** Automatically hides sensitive data (like SSNs or credit card numbers) before the model sees it.  
* **Malicious URL Filtering:** Prevents agents from accidentally clicking or generating phishing links.  
  ---

  ## **6\. Data & Connectivity**

  ### **MCP (Model Context Protocol)**

The **"USB-C for AI."** It is an open standard that allows any agent to connect to any data source (BigQuery, Drive, GitHub) without custom code for every integration.

* **Managed MCP Servers:** Google now hosts "zero-infrastructure" servers for BigQuery, Cloud SQL, and Maps.

  ### **BigQuery Data Connect**

Allows agents to treat your massive data warehouse as a "tool." The agent can write and execute its own SQL to answer business questions.

### **Firebase**

The "Agent Backend." It provides **Firestore** for the agent's long-term memory and **Cloud Functions** to run the agent's specific tools.

---

## **7\. Sample Workflow: "Agentic Data Analysis"**

1. **Trigger:** User asks the **CLI** or a **Firebase** UI: *"Why did sales dip in June?"*  
2. **Logic:** An **ADK-based** service on **Cloud Run** starts an "Interaction."  
3. **Reasoning:** **Gemini 3 Pro** decides it needs data.  
4. **Action:** The agent uses an **MCP tool** to query **BigQuery** via **Data Connect**.

