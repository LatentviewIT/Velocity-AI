import os
import json
import operator
import pandas as pd
from typing import TypedDict, List, Dict, Any, Annotated
from datetime import date, datetime

# ADK and Google Cloud Imports
from google.adk.agents import Agent
from google import genai
from google.cloud import bigquery
from langgraph.graph import StateGraph, END

# --- 1. CONFIGURATION ---
_PROJECT_ID = os.environ.get("PROJECT_ID", "project-nirvana-405904")
_DATASET_ID = "velocity_ai_iis"
_LOCATION = os.environ.get("LOCATION", "us-central1")
MODEL_ID = "gemini-2.0-flash"

# --- 2. LIVE SCHEMA MAP ---
LIVE_SCHEMA = {
    "agg_vendor_performance": ["Quarter", "Vendor_Name", "Region", "Category", "CurrentQuarter_whole_frequency", "CurrentQuarter_Sentiment_Shift"],
    "agg_region_performance": ["Quarter", "Region", "CurrentQuarter_whole_frequency", "CurrentQuarter_Sentiment_Shift"],
    "agg_agent_performance": ["Quarter", "Agent_Name", "Vendor_Name", "CurrentQuarter_whole_frequency", "CurrentQuarter_Sentiment_Shift"],
    "agg_category_performance": ["Quarter", "Category", "CurrentQuarter_whole_frequency", "CurrentQuarter_Sentiment_Shift"],
    "agg_sales_tenure_performance": ['Quarter', 'Vendor_Name', 'Operating_Region', 'Total_Sales_Volume', 'Quality_Score_%', 'AHT_Seconds', 'Active_Staffing_%', 'Average_Tenure_Months', 'Attrition_Rate_%', 'QoQ_Total_Sales_Volume', 'QoQ_Quality_Score_%', 'QoQ_AHT_Seconds', 'QoQ_Active_Staffing_%', 'QoQ_Average_Tenure_Months', 'QoQ_Attrition_Rate_%']
}

# Global Client Initialization (ADK handles credentials automatically via Service Account)
genai_client = genai.Client(vertexai=True, project=_PROJECT_ID, location=_LOCATION)
bq_client = bigquery.Client(project=_PROJECT_ID)

# --- 3. STATE DEFINITION ---
class AgentState(TypedDict):
    question: str
    history: List[Dict[str, Any]]
    detail_plan: Dict[str, Any]
    current_h_index: int
    work_items: List[Dict[str, Any]]
    working_notes: Annotated[List[str], operator.add]
    iteration_status: str
    iteration_count: int
    _validate_route: str
    last_act_result: List[Dict[str, Any]]
    consolidated_results: Annotated[List[List[Dict[str, Any]]], operator.add]
    analyst_summaries: Annotated[List[str], operator.add]
    data_scientist_summary: str
    draft_answer: str

# --- 4. NODE LOGIC (PASTED FROM YOUR NOTEBOOK) ---

def llm_query(prompt: str, is_json: bool = False):
    mime_type = "application/json" if is_json else "text/plain"
    config = {"response_mime_type": mime_type, "temperature": 0.0}
    response = genai_client.models.generate_content(model=MODEL_ID, contents=[prompt], config=config)
    return response.text

def get_latest_quarters():
    query = f"SELECT DISTINCT `Quarter` FROM `{_PROJECT_ID}.{_DATASET_ID}.agg_region_performance` ORDER BY `Quarter` DESC LIMIT 2"
    df = bq_client.query(query).to_dataframe()
    if len(df) >= 2:
        return str(df['Quarter'].iloc[0]), str(df['Quarter'].iloc[1])
    return "2026-01-01", "2025-10-01"

# --- CELL 1: THE THINKER INSTRUCTIONS (SCOPE-AWARE) ---

THINKER_PROMPT = """
# ROLE: Strategic Data Architect
# OBJECTIVE: Generate a dynamic SQL investigation blueprint based on the user's inquiry.

# 🚨 DYNAMIC CORRELATION PROTOCOL (FINAL HYPOTHESIS):
The FINAL Hypothesis MUST be "Multivariate Performance Correlation Drill-Down".
- **OBJECTIVE**: Quantify the causal link between Frontline Quality (Sentiment/CSAT), Staffing (Frequency), and Sales Volume.
- **LOGIC**: Perform an INNER JOIN between `agg_sales_tenure_performance` (Sales) and `agg_agent_performance` (Agent).
- **JOIN KEYS**: Use `Quarter`.
- **MANDATORY MULTIVARIATE TASKS**:
    1. **Quality Correlation**: Link `CurrentQuarter_Sentiment_Shift` (Proxy for CSAT/Quality) against `Total_Sales_Volume`.
    2. **Staffing Correlation**: Link `CurrentQuarter_whole_frequency` (Proxy for Staffing/Volume) against `Total_Sales_Volume`.
    3. **The Veteran Paradox**: Correlate `Average_Tenure_Months` and `Tenure_Bracket` against both Sentiment and Sales.
- **DIMENSIONAL SLICING**: Logic must allow for slicing results by `Region` and `Vendor_Name` where available in the join.

# 🚨 THE CLOSED-WORLD MANDATE:
You are an expert SQL Architect. You must operate under a "Strict Schema Assumption":
1. **ONLY** use the table names provided in the LIVE_SCHEMA: {table_names}.
2. **ONLY** use the column names explicitly listed for each table in the provided schema context.
3. **FORBIDDEN**: Do not use `Total_Spend`, `Revenue`, `Clicks`, or `Impressions`. They DO NOT exist.
4. **OPERATIONAL EQUIVALENTS**:
   - Use `CurrentQuarter_whole_frequency` as your primary Volume/Activity metric.
   - Use `CurrentQuarter_Sentiment_Shift` as your primary Quality/Behavior metric.

# 🚨 STRICT ENTRY-POINT & DEPTH RULE:
Match the 'Analysis_Level' of Hypothesis 1 EXACTLY to the user's query scope.
- If Entry = Global: Plan 6 levels (Global -> Region -> Vendor -> Category -> Agent -> Correlation).
- If Entry = Region: Plan 5 levels (Region -> Vendor -> Category -> Agent -> Correlation).
- If Entry = Vendor: Plan 4 levels (Vendor -> Category -> Agent -> Correlation).
- If Entry = Category: Plan 3 levels (Category -> Agent -> Correlation).
- If Entry = Agent:  Plan 2 levels (Agent -> Correlation).

# 🎯 DIMENSION ALIGNMENT RULE (NO EXCEPTIONS):
You MUST use the following Primary Dimensions for grouping in the SQL logic:
- Analysis_Level 'Global' -> Group by `Quarter` ONLY
- Analysis_Level 'Region' -> Group by `Region` ONLY
- Analysis_Level 'Vendor' -> Group by `Vendor_Name` ONLY
- Analysis_Level 'Category' -> Group by `Category` ONLY
- Analysis_Level 'Agent' -> Group by `Agent_Name` ONLY
- Analysis_Level 'Correlation' -> Group by `Quarter`
- 🚨 SELECT PURITY: Do not add extra dimensions to the SELECT clause unless they are part of the join/filter logic.

# 🚨 STRICT GRAIN ENFORCEMENT PROTOCOL:
For every Hypothesis, the SQL 'logic' MUST follow these dimension-alignment rules:

1. **LEVEL: GLOBAL**
   - **Dimension**: `Quarter` only.
   - **SQL Rule**: SELECT only `Quarter` and `AVG/SUM` aggregates.
   - **table**: "agg_region_performance".
   - **Group By**: `Quarter` only.
   - **Goal**: One single row representing the entire organization's pulse.

2. **LEVEL: REGION**
   - **Dimension**: `Operating_Region` (Sales Table) or `Region` (Agent/Vendor Tables).
   - **SQL Rule**: SELECT dimension + `Quarter` + aggregates.
   - **Group By**: 1, 2.

3. **LEVEL: VENDOR**
   - **Dimension**: `Vendor_Name` .
   - **SQL Rule**: SELECT dimension + `Quarter` + aggregates.
   - **Group By**: 1, 2.

4. **LEVEL: CATEGORY**
   - **Dimension**: `Category`.
   - **SQL Rule**: SELECT dimension + `Quarter` + aggregates.
   - **Group By**: 1, 2.

5. **LEVEL: AGENT**
   - **Dimension**: `Agent_Name`.
   - **SQL Rule**: SELECT dimension + `Quarter` + aggregates.
   - **Group By**: 1, 2.

- 🚨 ATTENTION: `Tenure_Bracket` exists ONLY in Sales. `Agent_Name`/`Vendor_Name` exists ONLY in Agent. You CANNOT join on these dimensions. Join ONLY on `Quarter`.
🚨 COMPARISON SNAPSHOT RULE: Step X.1 and Step X.2 MUST use the same aggregation and the same GROUP BY dimension. Never select raw rows for a comparison snapshot.


🚨 JOIN_MULTIPLE INSTRUCTION:
When target_table is JOIN_MULTIPLE, the SQL developer will be given schemas for agg_sales_tenure_performance and agg_agent_performance. Use ONLY these two tables. Do not reference a table actually named "JOIN_MULTIPLE".

# MANDATORY PROTOCOL:
For all levels (Region, Vendor, Category, Agent), do not just request a single average.
Your SQL logic must return the full list of entities and their metrics so the system can identify Top/Bottom Outliers. Ensure the Analysis_Level dimension is always selected and grouped."

1. SCHEMA LOOKUP: Locate the 'target_table' in the LIVE_SCHEMA.
2. COLUMN VALIDATION: Verify every column against the specific table's list.
3. ERROR ADAPTATION: If the previous agent reported an error, cross-reference with the schema to find the correct substitute.
4. ATOMIC LOGIC:
   - Step X.1: Snapshot '{curr_q}' (NO JOINS).
   - Step X.2: Snapshot '{prev_q}' (NO JOINS).
   - Step X.3: Comparison CTE: (X.1 - X.2) / X.2 using SAFE_DIVIDE.



# 🚨 HARD CONSTRAINTS (TO BREAK THE LOOP):
1. **CATEGORY TABLE**: You MUST use `agg_category_performance` for any Category level analysis. If it is missing from the schema context, reference the TABLE MAPPING RULE.
2. **CORRELATION JOIN**: Use ONLY `Quarter` as the join key. Do not reference `Tenure_Bracket` in the JOIN clause.
3. **METRIC CONSISTENCY**: Ensure `Total_Sales_Volume` is included in the metrics list for every hypothesis from Region down to Correlation.
4. **DO NOT SKIP CATEGORY**: The drill-down must pass through the category level.


# JSON STRUCTURE EXAMPLE (REQUIRED):
{{
  "Full_Blueprint": [
    {{
      "Hypothesis_ID": 1,
      "Hypothesis_Title": "Starting Level Analysis",
      "Analysis_Level": "...",
      "Steps": [ {{ "step_id": 1.1, "logic": "...", "target_table": "...", "metrics": [] }} ]
    }},
    {{
      "Hypothesis_ID": 2,
      "Hypothesis_Title": "Next Level Drill-Down",
      "Analysis_Level": "...",
      "Steps": [ {{ "step_id": 2.1, "logic": "...", "target_table": "...", "metrics": [] }} ]
    }},
    ... (Include all levels until vendor,category and Agent level is reached as per the user query) ...
    {{
      "Hypothesis_ID": n,
      "Hypothesis_Title": "Cross-Functional Metric Linkage",
      "Analysis_Level": "Correlation",
      "Steps": [
          {{
            "step_id": 5.1,
            "logic": "Multivariate Join: Correlate Sales vs Sentiment (Quality) and Tenure vs Volume across regions.",
            "target_table": "JOIN_MULTIPLE",
            "metrics": ["Total_Sales_Volume", "CurrentQuarter_Sentiment_Shift", "Average_Tenure_Months"]
          }}
  ]
}}


# 🧬 DYNAMIC CORRELATION PROTOCOL:
Generate a final "Cross-Functional" hypothesis. Do not reference Level numbers.
Instead, reference "Metric Linkage."

1. OBJECTIVE: Link leading indicators (Tenure, Sentiment) to lagging indicators (Sales Volume).
2. LOGIC:
   - Fetch 'Total_Sales_Volume' and 'Tenure_Bracket' from `agg_sales_tenure_performance`.
   - Fetch 'CurrentQuarter_Sentiment_Shift' and 'CurrentQuarter_whole_frequency' from `agg_agent_performance`.
   - JOIN condition: Use `Quarter` and, where available, `Tenure_Bracket`.
3. GOAL: Determine the "Performance Elasticity"—how much does Sales Volume change for every point of Sentiment Shift?

# LIVE SCHEMA: {schema_context}
# USER QUESTION: {user_question}
# PREVIOUS ERRORS: {last_error}
# TEMPORAL CONTEXT: CURRENT_QUARTER: '{curr_q}' | PREVIOUS_QUARTER: '{prev_q}'

"""

# --- CELL 2: THE ANALYST (THE GATEKEEPER) ---

ANALYST_PROMPT = """
# ROLE
Senior Data Quality Analyst & Supervisor.
Check the User Question: "{user_question}"

# 🔍 PHASE 1: PLAN AUDIT (THE GATEKEEPER)
1. **SCOPE ALIGNMENT CHECK**: Does the first Hypothesis match the user's requested scope?
   - User asked about: "{user_question}"
   - Plan starts at: "{plan_start_level}"
   - Current Plan Depth: {num_levels} Levels
Review the "Query Blueprint" generated by the Thinker Agent.
 Does the Hypothesis and its steps correspond to the user query?
 Does it follow the Hierarchical sequence (Global -> Region -> Vendor -> Category -> Agent -> correlation)?
 Validate metrics and tables against the schema.
 Add/Remove/Edit steps if the hypothesis logic is incomplete.

2. **THE HALLUCINATION CHECK**: Cross-reference every metric against the LIVE_SCHEMA: {schema_context}
   - Reject any plan containing "Spend", "Impressions", "Clicks", "Revenue", or "AHT".

3. **GRAIN CHECK**: Reject if 'Vendor' level groups by anything other than `Vendor_Name`.

4 ⚖️ DECISION LOGIC:
- If a hallucinated column is found -> Decision: "Fixing Blueprint" | Feedback: "Column [X] does not exist in table [Y]. Reference the Schema."
- If dimensions are misaligned -> Decision: "Fixing Blueprint" | Feedback: "Grain mismatch: Level is [X] but grouping is [Y]."
- If user asked about "Region" but the plan starts at "Global", set decision to "Fixing Blueprint" and tell the Thinker: "Start at Regional level as requested."
- Only set to "Satisfied" if the first level matches the prompt AND the final level reaches 'Agent'.
- **DECISION**: Set to "Satisfied" ONLY if the schema, grain, and sequence are 100% correct.


# 🔍 METRIC ACCOUNTING:
Reject the plan if:
1. Level is 'Vendor' but `GROUP BY` is `Region`.
2. Level is 'Correlation' but `Average_Tenure_Months` or `Tenure_Bracket` is missing from the logic.
3. Level is 'Correlation' but it fails to compare Sales Volume against Sentiment.



# MANDATORY FEEDBACK RULE
If you reject the plan (any status other than Satisfied), your "feedback" MUST start with:
"Issue in Hypothesis [ID Number]: [Description of issue]"
If multiple hypotheses need changes, list them clearly.
REJECT any join logic that uses Tenure_Bracket as a join key, as it is missing from the agg_agent_performance table.


# SCOPE VALIDATION:
1. If the User asks for 'Global', expect 6 levels.
2. If the User asks for 'Region', expect 5 levels (Region, Vendor, Category, Agent, correlation).
3. If the User asks for 'Vendor', expect 4 levels (Vendor, Category, Agent, correlation).
4. If the User asks for 'Category', expect 3 levels (Category, Agent,correlation).
5. If the User asks for 'Agent', expect 2 level (Agent,correlation).

# 📝 PHASE 2: HYPOTHESIS SUMMARIZATION (THE DIAGNOSTICIAN)
As soon as a hypothesis level is executed, you must generate a narrative summary that is **forensic and elaborate**.
- **The "Who"**: Identify the Top 2 and Bottom 2 Outliers by name.
- **The "How Much"**: Back every claim with the specific numbers (e.g., "+15% Frequency Change") and their '% impact' on the total grain.
- **The "Why"**: Formulate a hypothesis using the **Diagnostic Key**:
    - *Workload Exhaustion*: High Frequency + Negative Sentiment (The Treadmill Effect).
    - *Process Friction*: Low Frequency + Negative Sentiment (The Black Hole).
- **Format**: Use a professional, narrative structure that flows from the outliers to the root cause.

# 🚨 DIMENSION AUDIT (STRICT):
You MUST reject the plan if:
- 'Vendor' level logic groups by anything other than `Vendor_Name`.
- 'Category' level logic groups by anything other than `Category`.
- 'Agent' level logic groups by anything other than `Agent_Name`.
- 'Correlation' level logic attempts to JOIN on anything other than `Quarter`.
- 'Correlation' level attempts to use `Tenure_Bracket` as a JOIN key (it is missing from Agent tables).

# OUTPUT (JSON)
{{
  "decision": "One of the 4 strings above",
  "feedback": "Why the plan was accepted or rejected",
  "suggested_edits": "Specific technical instructions for the Thinker"
}}
"""


# --- CELL 2: THE THINKER NODE ---
def thinker_node(state: AgentState):
    curr_q, prev_q = get_latest_quarters()
    round_num = state.get('iteration_count', 0) + 1
    feedback = state['working_notes'][-1] if state['working_notes'] else "Initial Request"
    table_names_list = ", ".join(LIVE_SCHEMA.keys())


    # Get the last feedback so the Thinker knows what to fix
    feedback = state['working_notes'][-1] if state['working_notes'] else "Initial Request"

    # --- THE FIX: Pre-calculate table names ---
    table_names_list = ", ".join(LIVE_SCHEMA.keys())

    # Ensure schema_context is passed using the LIVE_SCHEMA
    full_prompt = THINKER_PROMPT.format(
        user_question=state['question'],
        curr_q=curr_q,
        prev_q=prev_q,
        table_names=table_names_list,
        schema_context=json.dumps(LIVE_SCHEMA, indent=2),
        last_error=feedback
    )


    try:
        response = llm_query(full_prompt, is_json=True)
        blueprint = json.loads(response)

        # --- THE LOOP-BREAKER CODE (MAINTAINED) ---
        for h in blueprint.get("Full_Blueprint", []):
            # 1. Force correct Category table
            if h.get("Analysis_Level") == "Category":
                for step in h.get("Steps", []):
                    step["target_table"] = "agg_category_performance"

            # 2. Force Join logic to be specific to Correlation Needs
            if h.get("Analysis_Level") == "Correlation":
                for step in h.get("Steps", []):
                    # Ensure it uses the dynamic join logic discussed
                    step["logic"] = "Multivariate JOIN on Quarter. Correlate Sales, Sentiment, and Tenure."
                    step["target_table"] = "JOIN_MULTIPLE"

    except Exception as e:
        print(f"❌ Thinker Logic Error: {e}")
        blueprint = state.get("detail_plan", {}) # Fallback to existing if error

    # Dynamic note to track analysis start level
    start_lvl = blueprint.get('Full_Blueprint',[{}])[0].get('Analysis_Level', 'Unknown')

    return {
        "detail_plan": blueprint,
        "working_notes": [f"Thinker: Blueprint drafted starting at {start_lvl}. Correlation logic locked."],
        "iteration_status": "Planning",
        "current_h_index": 0,
        "iteration_count": round_num
    }

# --- 10. ADAPTIVE SUPER ANALYST NODE (GATEKEEPER & SUMMARIZER) ---

def analyst_node(state: AgentState):
    current_status = state.get("iteration_status", "Planning")
    round_num = state.get("iteration_count", 1)
    idx = state.get("current_h_index", 0)

    detail_plan = state.get('detail_plan', {})
    blueprint_list = detail_plan.get('Full_Blueprint', [])
    h_id = idx + 1

    print(f"\n--- [ANALYST] Dual-Role Audit (Round {round_num}) ---")

    # --- ROLE 1: PLAN AUDITOR (PHASE 1 - PLANNING) ---
    if current_status == "Planning":
        print("🔎 Role: Plan Auditor")
        num_levels = len(blueprint_list)
        plan_start_level = blueprint_list[0].get('Analysis_Level', 'Unknown') if blueprint_list else "Empty"
        user_q = state.get('question', 'No question found')

        formatted_analyst_query = ANALYST_PROMPT.format(
            user_question=user_q,
            num_levels=num_levels,
            plan_start_level=plan_start_level,
            schema_context=json.dumps(LIVE_SCHEMA, indent=2)
        )

        plan_json = json.dumps(detail_plan, indent=2)
        response = llm_query(f"{formatted_analyst_query}\n\nBlueprint: {plan_json}", is_json=True)
        audit = json.loads(response)

        is_satisfied = "Satisfied" in audit.get("decision", "")
        route = "proceed" if is_satisfied else "recontextualize"

        return {
            "current_h_index": 0, # 🚨 FORCE ZERO HERE to ensure Global starts
            "_validate_route": route,
            "working_notes": [f"Status: {'Approved' if is_satisfied else 'Fixing Blueprint: ' + audit.get('feedback', '')}"],
            "iteration_status": "Executing" if is_satisfied else "Planning" # 🚨 SWITCH STATUS
        }

    # --- ROLE 2: EXECUTION VALIDATOR & FORENSIC SUMMARIZER (PHASE 2 - EXECUTING) ---
    else:
        print(f"🔎 Role: Execution Validator & Summarizer (Hypothesis {h_id})")

        if not state.get('consolidated_results'):
            return {"_validate_route": "recontextualize", "iteration_status": "Planning"}

        last_vault_entry = state.get('consolidated_results', [])[-1]
        failed_steps = [r for r in last_vault_entry if r.get('status') == "failed"]

        # Handling Failed SQL
        if failed_steps:
            error_details = failed_steps[0].get('error_message', 'Unknown Error')
            note = f"CRITICAL ERROR AT LEVEL {h_id}: {error_details}. REDESIGNING LEVEL."
            return {
                "current_h_index": idx,
                "_validate_route": "recontextualize",
                "working_notes": [note],
                "iteration_status": "Planning"
            }

        # --- GENERATING INTENSIVELY DETAILED SUMMARY ---
        print(f"📝 Generating Forensic Summary for Level {h_id}...")

        summary_prompt = f"""
        # ROLE: Senior Diagnostic Reporter
        # HYPOTHESIS: {blueprint_list[idx].get('Hypothesis_Title')}
        # GRAIN: {blueprint_list[idx].get('Analysis_Level')}
        # DATA: {json.dumps(last_vault_entry, indent=2)}

        # TASK:
        Provide an ELABORATE, INTENSIVELY DETAILED diagnostic report for this level.
        - Start with a narrative overview of the {blueprint_list[idx].get('Analysis_Level')} landscape.
        - List Positive Outliers (Top 2) with their specific % impact.
        - List Negative Outliers (Bottom 2) with their specific % impact.
        - Apply the 'Why' Hypothesis (Exhaustion vs. Friction).
        - Use bolding and professional structure as per the manager's request.

        Use professional, forensic language. Back every claim with numbers.
        """

        level_report = llm_query(summary_prompt, is_json=False)

        # Update Logic & Pointer
        if h_id < len(blueprint_list):
            print(f"✅ Level {h_id} Summarized. Proceeding to Level {h_id+1}...")
            return {
                "current_h_index": h_id,
                "analyst_summaries": [level_report], # Appends via operator.add
                "_validate_route": "next_hypothesis",
                "working_notes": [f"Analyst: Forensic Summary for Hypothesis {h_id} ({blueprint_list[idx].get('Analysis_Level')}) delivered."],
                "iteration_status": "Executing"
            }
        else:
            print("🏆 Final Level Summarized. Passing to Scientist.")
            return {
                "analyst_summaries": [level_report],
                "_validate_route": "finalize",
                "working_notes": [f"Analyst: Final Forensic Summary for Hypothesis {h_id} delivered."],
                "iteration_status": "Executing"
            }

 # --- 12. ORCHESTRATOR NODE (DYNAMIC BEHAVIORAL MAPPING) ---

def orchestrator_node(state: AgentState):
    idx = state.get("current_h_index", 0)
    blueprint = state['detail_plan'].get('Full_Blueprint', [])

    # 1. Safety check
    if idx >= len(blueprint):
        return {"iteration_status": "Finalizing", "_validate_route": "finalize"}

    # 2. Extract Level Context
    target_h = blueprint[idx]
    analysis_level = target_h.get('Analysis_Level', 'Unknown')

    print(f"\n⚙️ [ORCHESTRATOR] Level {idx+1}: {analysis_level}")
    print(f"📌 Hypothesis: {target_h['Hypothesis_Title']}")

    # 🚨 DYNAMIC LEVEL TABLE MAPPING
    # This ensures Global starts with the behavioral pulse as requested.
    LEVEL_TABLE_MAP = {
        "Global": "agg_region_performance",        # Your specific request
        "Region": "agg_region_performance",
        "Vendor": "agg_vendor_performance",
        "Category": "agg_category_performance",
        "Agent": "agg_agent_performance",
        "Correlation": "JOIN_MULTIPLE"
    }

    # 3. Prepare Work Items
    steps = target_h.get('Steps', [])
    for step in steps:
        # A. Handle Correlation Joins (Linking Behavior to Sales at the end)
        if step.get('target_table') == "JOIN_MULTIPLE" or analysis_level == "Correlation":
            step['target_table'] = "JOIN_MULTIPLE"
            step['source_tables'] = ["agg_sales_tenure_performance", "agg_agent_performance"]

        # B. Apply Dynamic Fallback Mapping
        elif not step.get('target_table'):
            # Use the Hypothesis-defined table first, then the Map, then a generic fallback
            fallback = target_h.get('Analysis_Level_Table') or LEVEL_TABLE_MAP.get(analysis_level, "agg_region_performance")
            step['target_table'] = fallback

    # 4. State Handshake Note
    note = f"Orchestrator: Preparing {len(steps)} steps for {analysis_level} using {step.get('target_table')}."
    print(f"📝 {note}")

    return {
        "work_items": steps,
        "iteration_status": "Executing",
        "working_notes": [note]
    }

# --- CELL 3: ACT AGENT INSTRUCTIONS (ATOMIC) ---

ACT_SQL_PROMPT_TEMPLATE = """

# ROLE: Senior BigQuery Developer
# MISSION: Write SQL for Step {step_id} using {target_table}.

# SCHEMA-FIRST INSTRUCTIONS:
You are provided with the exact schema for {target_table}:
{schema_context}

# 🚨 COLUMN SYNONYM & MAPPING RULES:
If your target table is `agg_sales_tenure_performance`:
1. If you need a regional dimension -> Use `Operating_Region` (NOT `Region`).
2. All other tables in the schema use `Region`.

# 🚨 GLOBAL GRAIN OVERRIDE:
If {Analysis_Level} == "Global":
    1. Your SQL MUST NOT contain the word `Region`, `Vendor_Name`, or `Agent_Name` in the SELECT or GROUP BY.
    2. You are summarizing the entire company.
    3. Example: SELECT `Quarter`, AVG(`Metric`) FROM `table` WHERE `Quarter` = '{current_q}' GROUP BY 1.

# 🚨 CORRELATION JOIN RULES (STRICT SCHEMA ADHERENCE):
When performing a JOIN for Correlation (JOIN_MULTIPLE):
1. **DIMENSION MAPPING**:
   - Agent Table (`agg_agent_performance`) uses `Region`.
   - Sales Table (`agg_sales_tenure_performance`) uses `Operating_Region`.
   - Use `ON agent.Region = sales.Operating_Region` for regional joins.
2. **TENURE DRILL-DOWN**:
   - `Average_Tenure_Months` and `Total_Sales_Volume` are ONLY in the Sales table.
   - `CurrentQuarter_Sentiment_Shift` is ONLY in the Agent table.
3. **CAST IS MANDATORY**: Use `ON CAST(sales.Quarter AS DATE) = CAST(agent.Quarter AS DATE)`.

# 🚨 MANDATORY DIMENSION RULE (PURITY CHECK):
You MUST anchor your SELECT and GROUP BY clauses to the specific grain of the current Analysis Level.
1. If analyzing 'Vendor' -> You MUST use `Vendor_Name`. DO NOT use `Region`.
2. If analyzing 'Category' -> You MUST use `Category`.
3. If analyzing 'Agent' -> You MUST use `Agent_Name`.
4. If analyzing 'Region' -> You MUST use `Region`.
5. Always use a clear ALIAS for aggregates (e.g., AS avg_metric).

# 🚨 CORRELATION JOIN RULES (STRICT SCHEMA ADHERENCE):
When target_table is "JOIN_MULTIPLE":
1. **ONLY JOIN ON QUARTER**: Do not attempt to join on `Tenure_Bracket` directly in the ON clause.
2. **CAST IS MANDATORY**: Use `ON CAST(sales.Quarter AS DATE) = CAST(agent.Quarter AS DATE)`.
3. **SUBQUERY AGGREGATION**: To prevent grain mismatch, ALWAYS aggregate the Agent data to the target dimension (Region, Vendor, Category) in a subquery BEFORE joining with the Sales table.
4. **CORR FUNCTION**: Use `CORR(y, x)` to calculate the relationship.

# EXECUTION RULES:
1. STRICT COLUMN MATCHING: Compare the requested logic ({step_logic}) against the {schema_context}.
2. AUTOMATIC CORRECTION: If the logic asks for a column that does not exist (e.g., "Cost"), find the closest match in the schema (e.g., "Total_Spend" or "total_amount").
3. QUALIFICATION: Use the format: `project-nirvana-405904.velocity_ai_iis.{target_table}`.
4. FALLBACK: If no relevant columns exist for the requested logic, query the most important numeric metric in the table and add a SQL comment: -- Adjusted to available schema.

Example for Category Level:
SELECT `Category`, AVG(`Metric`) AS avg_metric
FROM `table`
WHERE `Quarter` = '...'
GROUP BY `Category`

# MISSION: Write ATOMIC SQL for Step {step_id}.
CRITICAL RULE: FULLY QUALIFIED TABLES
You MUST use the full path for every table: {project_id}.{dataset_id}.{target_table}.
Failure to include the project and dataset ID will crash the mission.

RULES:
"Always provide an explicit alias for every aggregated column using 'AS'. Never allow BigQuery to default to 'f0_'."
Use backticks for the full path: `{project_id}.{dataset_id}.{target_table}`.
Quarters are DATE type: WHERE Quarter = '{current_q}'.
Use SAFE_DIVIDE for all math.

CONTEXT:
Logic: {step_logic}
Target Table: {target_table}
Project: {project_id}
Dataset: {dataset_id}
Current Date: {current_q} | Prev Date: {previous_q}

# RULES:
1. ONLY use the table provided: `{target_table}`.
2. NO CROSS-TABLE JOINS. Do not join Region tables with Sales tables.
3. Use backticks for all columns: `Quarter`, `Region`, etc.
4. Quarters are DATE type: WHERE `Quarter` = '{current_q}'.
5. Use SAFE_DIVIDE for all math.

# 🚨 DYNAMIC EXECUTION RULES:

1. **SINGLE TABLE STEPS**:
   - Include the specific dimension (e.g., `Vendor_Name`) in both SELECT and GROUP BY.
   - Quarters are DATE type: WHERE `Quarter` = '{current_q}'.

# 🧬 FEW-SHOT CORRELATION EXAMPLES:

### Example 1: Global/Regional Correlation (Sentiment vs Sales)
SELECT
    sales.`Operating_Region`, -- Dimension from Sales
    CORR(sales.`Total_Sales_Volume`, agent_agg.avg_sent) AS corr_sales_sentiment
FROM `{project_id}.{dataset_id}.agg_sales_tenure_performance` AS sales
INNER JOIN (
    SELECT `Region`, `Quarter`, AVG(`CurrentQuarter_Sentiment_Shift`) as avg_sent
    FROM `{project_id}.{dataset_id}.agg_agent_performance`
    GROUP BY 1, 2
) AS agent_agg
  ON sales.`Operating_Region` = agent_agg.`Region` -- MAPPING THE SYNONYMS
  AND CAST(sales.`Quarter` AS DATE) = CAST(agent_agg.`Quarter` AS DATE)
WHERE sales.`Quarter` = '{current_q}'
GROUP BY 1

### Example 2: The Veteran Paradox (Tenure vs Quality per Vendor)
SELECT
    sales.`Vendor_Name`,
    sales.`Tenure_Bracket`,
    AVG(sales.`Total_Sales_Volume`) AS avg_sales,
    AVG(agent_agg.avg_sent) AS avg_quality
FROM `{project_id}.{dataset_id}.agg_sales_tenure_performance` AS sales
INNER JOIN (
    SELECT `Vendor_Name`, `Quarter`, AVG(`CurrentQuarter_Sentiment_Shift`) as avg_sent
    FROM `{project_id}.{dataset_id}.agg_agent_performance`
    GROUP BY 1, 2
) AS agent_agg
  ON sales.`Vendor_Name` = agent_agg.`Vendor_Name`
  AND CAST(sales.`Quarter` AS DATE) = CAST(agent_agg.`Quarter` AS DATE)
WHERE sales.`Quarter` = '{current_q}'
GROUP BY 1, 2

# 🧬 DYNAMIC CORRELATION Examples
# Change the dimension (Vendor,category,agent,region depending on what the user query is about.

#For example the vendor level correlation should look like this
SELECT
    sales.`Vendor_Name`,
    CORR(sales.`Total_Sales_Volume`, agent_agg.avg_sent) AS corr_sales_sentiment
FROM `{project_id}.{dataset_id}.agg_sales_tenure_performance` AS sales
INNER JOIN (
    SELECT `Vendor_Name`, `Quarter`, AVG(`CurrentQuarter_Sentiment_Shift`) as avg_sent
    FROM `{project_id}.{dataset_id}.agg_agent_performance`
    GROUP BY 1, 2
) AS agent_agg
  ON sales.`Vendor_Name` = agent_agg.`Vendor_Name`
  AND sales.`Quarter` = agent_agg.`Quarter`
GROUP BY 1

# 🚨 EXECUTION RULES:
- FULLY QUALIFIED TABLES: `{project_id}.{dataset_id}.{target_table}`.
- Use backticks for all columns: `Operating_Region`, `Total_Sales_Volume`, etc.
- Use SAFE_DIVIDE for all math.

3. **GRANULARITY & MATH**:
   - Use backticks for all paths and columns.
   - Always alias aggregates (e.g., AS avg_metric) to avoid 'f0_'.
   - Use SAFE_DIVIDE for all calculations.
   - Quarters are DATE types: `WHERE Quarter = '{current_q}'`.

# ATOMIC LOGIC EXAMPLE:
- Snapshot: SELECT `Dim`, AVG(`Col`) AS avg_val FROM `table` WHERE `Quarter` = '{current_q}' GROUP BY `Dim`
- Comparison:
  WITH curr AS (SELECT `Dim`, AVG(`M`) as v FROM `T` WHERE `Quarter` = '{current_q}' GROUP BY 1),
       prev AS (SELECT `Dim`, AVG(`M`) as v FROM `T` WHERE `Quarter` = '{previous_q}' GROUP BY 1)
  SELECT curr.`Dim`, SAFE_DIVIDE(curr.v - prev.v, prev.v) AS pct_change FROM curr JOIN prev ON curr.Dim = prev.Dim

SQL:"""

def act_node(state: AgentState):
    idx = state.get("current_h_index", 0)
    items = state.get("work_items", [])
    blueprint_item = state['detail_plan']['Full_Blueprint'][idx]

    # Extract the level name for the prompt
    current_level = blueprint_item.get('Analysis_Level', 'Unknown')

    curr_q, prev_q = get_latest_quarters()
    all_results = []

    print(f"\n" + "="*60)
    print(f"🚀 EXECUTING LEVEL {idx+1}: {current_level}")
    print("="*60)

    for item in items:
        raw_target = item.get('target_table')
        logic = item.get('logic', '')
        lookup_key = raw_target.split('.')[-1].replace('`', '').strip()

        # Initialize variable to prevent UnboundLocalError
        generated_sql = "SQL NOT GENERATED"

        # 1. Context Building
        if raw_target == "JOIN_MULTIPLE":
            schema_info = {
                "agg_sales_tenure_performance": LIVE_SCHEMA.get("agg_sales_tenure_performance", {}),
                "agg_agent_performance": LIVE_SCHEMA.get("agg_agent_performance", {})
            }
            schema_context_str = "CORRELATION JOIN CONTEXT: " + json.dumps(schema_info)
            target_label = "JOIN_MULTIPLE"
        else:
            schema_info = LIVE_SCHEMA.get(lookup_key, "SCHEMA_NOT_FOUND")
            schema_context_str = f"STRICT SCHEMA for {lookup_key}: " + json.dumps(schema_info)
            target_label = raw_target

        # 2. Map Prompt Variables (Ensure Analysis_Level is included!)
        prompt_vars = {
            "step_id": item.get('step_id', 'N/A'),
            "step_logic": logic,
            "target_table": target_label,
            "current_q": str(curr_q),
            "previous_q": str(prev_q),
            "project_id": _PROJECT_ID,
            "dataset_id": _DATASET_ID,
            "schema_context": schema_context_str,
            "Analysis_Level": current_level  # 🚨 FIX: Added this to match template
        }

        try:
            # 3. SQL Generation
            formatted_prompt = ACT_SQL_PROMPT_TEMPLATE.format(**prompt_vars)
            raw_response = llm_query(formatted_prompt).strip()
            generated_sql = raw_response.replace("```sql", "").replace("```", "").strip()
            if generated_sql.endswith(";"): generated_sql = generated_sql[:-1]

            print(f"\n🔹 STEP {item.get('step_id')} | Grain: {current_level}")
            print(f"💻 SQL:\n{generated_sql}")

            # 4. Execution
            query_job = bq_client.query(generated_sql)
            results_df = query_job.to_dataframe()
            all_results.append({
                "step_id": item.get('step_id'),
                "status": "success",
                "sql": generated_sql,
                "data": results_df.to_dict(orient='records')
            })
            print(f"✅ Success: {len(results_df)} rows retrieved.")

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            all_results.append({
                "step_id": item.get('step_id'),
                "status": "failed",
                "error_message": str(e),
                "sql_attempted": generated_sql
            })

    return {"last_act_result": all_results}

import pandas as pd
import numpy as np
import json
from datetime import date, datetime

# --- 14. DYNAMIC CONSOLIDATOR (JSON-SAFE & DIAGNOSTIC) ---
def consolidator_node(state: AgentState):
    idx = state.get("current_h_index", 0)
    raw_results = state.get("last_act_result", [])
    existing_vault = state.get("consolidated_results", [])
    summarized_layer = []

    print(f"\n📂 [CONSOLIDATOR] Synthesizing Diagnostic Stats for Level {idx+1}")

    for res in raw_results:
        step_id = res.get("step_id")

        if res.get("status") == "success":
            data = res.get("data", [])
            if not data:
                summary = {"step_id": step_id, "status": "success", "msg": "No rows returned."}
            else:
                df = pd.DataFrame(data)

                # --- 🚨 CRITICAL FIX: JSON SERIALIZATION SAFETY 🚨 ---
                # Convert date/datetime objects to strings and decimals to floats
                for col in df.columns:
                    # Convert dates
                    if pd.api.types.is_datetime64_any_dtype(df[col]) or any(isinstance(x, (date, datetime)) for x in df[col]):
                        df[col] = df[col].astype(str)
                    # Convert Decimals (common in BigQuery) to floats for JSON
                    if any(hasattr(x, 'to_eng_string') for x in df[col]):
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                dimension_cols = [c for c in df.columns if c not in numeric_cols and c != 'Quarter']
                dimension_col = dimension_cols[0] if dimension_cols else "Total_Organization"

                is_correlation_step = any("corr" in col.lower() for col in df.columns)

                if is_correlation_step:
                    stats = {
                        "analysis_type": "Multivariate_Correlation",
                        "dimension_name": dimension_col,
                        "metrics": df.to_dict(orient='records')[0],
                        "msg": "Causal transmission link identified."
                    }
                else:
                    primary_metric = "CurrentQuarter_whole_frequency" if "CurrentQuarter_whole_frequency" in df.columns else (numeric_cols[0] if numeric_cols else None)

                    top_2, bottom_2 = [], []
                    if primary_metric:
                        top_2 = df.sort_values(by=primary_metric, ascending=False).head(2).to_dict(orient='records')
                        bottom_2 = df.sort_values(by=primary_metric, ascending=True).head(2).to_dict(orient='records')

                        total_sum = df[primary_metric].sum()
                        for item in top_2 + bottom_2:
                            item['pct_of_total_impact'] = round((item[primary_metric] / total_sum) * 100, 2) if total_sum > 0 else 0

                    stats = {
                        "dimension_name": dimension_col,
                        "global_averages": df[numeric_cols].mean().round(4).to_dict() if numeric_cols else {},
                        "top_outliers": top_2,
                        "bottom_outliers": bottom_2,
                        "total_records": len(df)
                    }

                summary = {"step_id": step_id, "status": "success", "insights": stats}
                print(f"   ✅ {dimension_col} Level processed and sanitized.")

        else:
            summary = {"step_id": step_id, "status": "failed", "error_message": res.get("error_message")}

        summarized_layer.append(summary)

    return {
        "consolidated_results": [summarized_layer],
        "working_notes": [f"Consolidator: Level {idx+1} diagnostic sanitized and locked."]
    }

# --- CELL 1: THE SCIENTIST INSTRUCTIONS (MASTER DIAGNOSTIC EDITOR) ---

SCIENTIST_PROMPT = """
# ROLE: Lead Strategy Consultant & Master Forensic Weaver
# MISSION: Synthesize layered forensic reports into a cohesive 360-degree story of Operational vs. Behavioral friction.

# 🚨 THE RESEARCH MANDATE:
You are the Master Editor. Do not merely restate data. You must "connect the dots" across the following layers:
1. **The Macro Symptom**: What did the Region/Global levels show?
2. **The Transmission**: How did the Vendor/Category choice amplify the friction?
3. **The Micro Root Cause**: Isolate the "Smoking Gun" behaviors (Agents/Tenure).

## 1. The 360-Degree Mission Story: The Pulse of the Business
- Contrast "Operational Activity" (Frequency) against "Human Health" (Sentiment).
- **Verdict**: Categorize the situation as "Process Maturity," "Operational Friction," or "Behavioral Crisis."

## 2. Deep Research: The Operational-Financial Linkage (CORRELATION)
- **Causal Proof**: Define the **Revenue Elasticity** (Sales Volume lost per 0.1 drop in Sentiment).
- **The Logic of Impact**: Use Level 5 r-values and coefficients to prove the transmission of behavior to finance.
- **The Veteran Paradox**: Explicitly analyze if top sellers/veterans are hitting a breaking point.

## 3. Drill-Down: The Forensic Trail (The Infection Path)
You MUST provide an ELABORATE, INTENSIVE breakdown for every level found in the summaries:
- **🌎 Level 1: Regional Drivers**: Top/Bottom 2 Regions with % weight and why (Resilience vs. Drag).
- **🏭 Level 2: Vendor Attribution**: Top/Bottom 2 Vendors with % impact and why (Resilience vs. Drag).
- **📦 Level 3: Category Dynamics**: Top/Bottom 2 Categories acting as anchors and why (Resilience vs. Drag).
- **👤 Level 4: Agent Behavioral Outliers**: The "Smoking Gun" agents with individual metrics vs. team averages.

# 🧪 THE DIAGNOSTIC KEY:
For every negative outlier, assign:
1. **Workload Exhaustion**: High Frequency + Negative Sentiment (The Treadmill Effect).
2. **Process Friction**: Low Frequency + Negative Sentiment (The Black Hole).

## 4. OKR Impact & Strategic Pivot: The Roadmap to Recovery
- **The Gap**: Quantify total Sales Volume/Revenue distance from the Goal.
- **The Strategic Pivot**: Provide 3 Quantified Recommendations: 1. Operational Shielding, 2. Friction Removal, 3. Resource Re-Alignment.

# 🚨 NUMERICAL MANDATE:
- Every finding MUST use the percentages (e.g., 34.26% impact) and names provided in the 'analyst_summaries'.
- Every finding MUST be backed by: "[Metric] shifted by [X.X]% (from [Prev] to [Curr])".
- NO Banned Vocabulary: "Step 1.1", "JSON", "Dataset", "Hypothesis".

### 🚀 Strategic Pivot: Roadmap to Recovery
- Provide 3 Quantified Recommendations: 1. Operational Shielding, 2. Friction Removal, 3. Resource Re-Alignment.

# 🧪 SCIENTIST RESILIENCE RULE:
- If one region is missing from the correlation (e.g., APAC), perform the analysis on the remaining regions.
- Do not state "data is missing" in the final report; instead, focus on the "Infection Path" found in the successful tiers.

# 🚨 BANNED VOCABULARY:
"Step 1.1", "Step 2.3", "Dataset", "Analysis", "Hypothesis", "The data shows".
"""

# --- 14. ADAPTIVE SCIENTIST NODE (THE MASTER WEAVER) ---

def scientist_node(state: AgentState):
    print(f"\n🧪 [SCIENTIST] Initiating Master Synthesis with Behavioral-Financial Drill-Down...")

    # Pulling the 'Intensively Detailed' summaries from the Analyst
    layered_reports = state.get('analyst_summaries', [])
    user_query = state.get('question')

    # Join the individual intensive summaries into a single narrative context
    # This prevents token overflow by using text summaries instead of raw SQL rows
    research_context = "\n\n".join([
        f"### FORENSIC CHAPTER {i+1}:\n{report}"
        for i, report in enumerate(layered_reports)
    ])

    full_query = f"""
    {SCIENTIST_PROMPT}

    # USER MISSION:
    {user_query}

    # FORENSIC EVIDENCE (CHAPTERS FROM ANALYST):
    {research_context}

    # SPECIAL CORRELATION INSTRUCTION:
    Extract the multivariate linkage from the Correlation Chapter.
    Compare the Tenure-Sales correlation with the Sentiment-Sales correlation to identify 'The Veteran Paradox'.
    Quantify 'Revenue Elasticity'—exactly how much Sales Volume is lost for every 0.1 drop in Sentiment.
    """

    # Generate the Consolidated Master Diagnostic
    master_diagnostic = llm_query(full_query, is_json=False)

    # PRINT THE SCIENTIST OUTPUT FOR AUDIT (As requested)
    print("\n" + "="*60)
    print("🔬 SCIENTIST MASTER RESEARCH OUTPUT (WITH CORRELATION DRILL-DOWN)")
    print("="*60)
    print(master_diagnostic)
    print("="*60 + "\n")

    return {
        "data_scientist_summary": master_diagnostic,
        "draft_answer": master_diagnostic, # Shared with Drafter
        "working_notes": ["Scientist: Master Story woven with specific Revenue Elasticity drill-down."],
        "iteration_status": "Finalizing"
    }

# --- THE DRAFTER INSTRUCTIONS: NARRATIVE ARCHITECTURE ---

DRAFTER_PROMPT = """
# ROLE: Chief Strategy Officer & Lead Storyteller
# MISSION: Transform forensic diagnostic research into a high-stakes "360-Degree Mission Story."

# 🎯 THE NARRATIVE ARC:
1. **The Baseline**: Contrast intended goals vs. actual operational health.
2. **The Shift**: Detail the behavioral decay using the forensic outliers identified.
3. **The Transmission (CRITICAL)**: Use CORRELATION to prove how sentiment decay became a financial leak.
4. **The Pivot**: Provide surgical, quantified recovery steps.

# 🚨 THE NUMERICAL MANDATE:
- NEVER generalize. Use the specific percentages (e.g., "34.26% impact") and names provided in the research.
- **Revenue Elasticity**: You MUST define the link between Sentiment and Sales (e.g., "For every 0.1 drop in sentiment, we lost $X").

# 🚨 FORMATTING RULES:
    - NO technical jargon (Steps, Nodes, Vaults).
    - Use clean Markdown tables for any multi-level metric comparisons.
    - EVERY claim needs a number and a delta (from X to Y).
    - Use professional, authoritative, but narrative tone.

    # 🚨 MANDATORY FORMATTING RULES:
    - NEVER mention "Step X.X", "Dataset", or "Table Name".
    - USE business terminology: "Revenue Impact", "Operational Friction", "Tenure Elasticity".
    - Every claim MUST use the numbers provided in the CORE ANALYSIS.
    - If a 'multivariate_correlation' is present, prioritize explaining the link between Sentiment and Sales.

# 🚀 REPORT STRUCTURE & ELABORATION:

## 🚀 INTELLIGENT INSIGHTS SYSTEM: EXECUTIVE PERFORMANCE REPORT

### 1. The 360-Degree Mission Story: The Pulse of the Business
- **Baseline vs. Shift**: Contrast the intended operational activity (Frequency) against human health (Sentiment).
- **Verdict**: Categorize the situation as "Process Maturity," "Operational Friction," or "Behavioral Crisis."

### 2. Deep Research: The Operational-Financial Linkage (CORRELATION)
- **Causal Proof**: Use the Correlation Coefficients (r) from Level 5 to prove the transmission of behavior to finance.
- **The Multivariate Analysis**: Create a Markdown table showing the relationship between Sentiment, Tenure, and Sales.
- **The Veteran Paradox**: Detail how top-tenure sellers are hitting a sentiment breaking point.

### 3. Drill-Down: The Forensic Trail (The Infection Path)
Provide an ELABORATE, INTENSIVE breakdown for every tier:
- **🌎 Level 1: Regional Drivers**: Identify Top/Bottom 2. Specify % impact 3) the 'Why' (Exhaustion vs. Friction).
- **🏭 Level 2: Vendor Attribution**: Identify Top/Bottom 2 partner impacts 3) the 'Why' (Exhaustion vs. Friction).
- **📦 Level 3: Category Dynamics**: Detail the product categories acting as financial anchors.
- **👤 Level 4: Agent Behavioral Outliers**: Isolate the "Smoking Gun" agents. Compare their metrics to "Superstars."

### 4. OKR Impact & Strategic Pivot: The Roadmap to Recovery
- **The Gap**: Quantify the total Sales Volume distance from the goal.
- **Recovery Plan**: Provide 3 Quantified Recommendations: 1. Operational Shielding, 2. Friction Removal, 3. Resource Re-Alignment.

# 🚨 NO-REFUSAL MANDATE:
- Even if the Scientist mentions data gaps, you MUST still write the full 4-section report using the successful outliers from the other 5 chapters (Region, Vendor, Category, Agent).
- DO NOT provide a meta-commentary about the data quality.
- If a specific correlation is missing, use the "Global" or "EMEA" correlation to represent the organizational trend.

# 🚨 STYLISTIC MANDATES:
- NO technical jargon (Steps, Nodes, Vaults).
- USE business terminology: "Revenue Impact", "Operational Friction", "Tenure Elasticity".
- Use Bolded metrics and clean Markdown tables for comparisons.
"""

# --- 15. DYNAMIC DRAFT NODE (FORENSIC ARCHITECT) ---

def draft_node(state: AgentState):
    print(f"\n🏁 [DRAFT AGENT] Sculpting the Forensic Executive Narrative...")

    # 1. Sources of Truth
    science_insight = state.get("data_scientist_summary", "No research interpretation available.")
    vault = state.get('consolidated_results', [])
    blueprint = state.get('detail_plan', {}).get('Full_Blueprint', [])
    user_query = state.get('question', 'Performance Analysis')

    # 2. Re-incorporating the structured metadata context for absolute accuracy
    # This ensures the Drafter has access to the raw outliers if the Scientist was too brief
    report_context = []
    for i, level_results in enumerate(vault):
        level_name = blueprint[i]['Analysis_Level'] if i < len(blueprint) else f"Level {i+1}"
        level_summary = {"analysis_level": level_name, "findings": []}

        for step in level_results:
            if step.get('status') == "success":
                insights = step.get('insights', {})
                level_summary["findings"].append({
                    "period": insights.get('period', 'Unknown'),
                    "dimension": insights.get('dimension_name', 'Unknown'),
                    "averages": insights.get('global_averages', {}),
                    "top_outliers": insights.get('top_outliers', []),
                    "bottom_outliers": insights.get('bottom_outliers', []),
                    "correlation": insights.get('correlation_metrics', {})
                })
        report_context.append(level_summary)

    # 3. Final 360-Degree Executive Synthesis Prompt
    synthesis_prompt = f"""
    {DRAFTER_PROMPT}

    # DATA CONTEXT (THE CORE EVIDENCE):
    ## SCIENTIFIC INTERPRETATION (The Story Narrative):
    {science_insight}

    ## STRUCTURED METRICS VAULT (The Quantitative Proof):
    {json.dumps(report_context, indent=2)}

    # MISSION OBJECTIVE:
    Respond to: "{user_query}"

    # 🚨 FINAL NARRATIVE CHECK:
    - Did you explicitly name the Top/Bottom 2 Outliers for Region, Vendor, and Category?
    - Did you isolate the specific "Smoking Gun" agents by name?
    - Is the "Revenue Elasticity" clearly defined (e.g., loss per 0.1 sentiment drop)?
    - Is the "Veteran Paradox" explained using the Tenure vs. Sales correlation?
    """

    # Generate the Final Narrative
    final_report = llm_query(synthesis_prompt, is_json=False)

    # 4. Final Polish: Clean up system artifacts and level-specific numbering
    import re
    final_report = re.sub(r"Step \d\.\d", "", final_report)
    final_report = re.sub(r"Level \d", "", final_report)

    # Visual formatting for the final terminal output
    final_report = final_report.replace("##", "---\n##")

    note = "Final Agent: Lead Strategist | Status: Intensive Forensic Report Delivered."
    print(f"✅ {note}")

    return {"draft_answer": final_report, "working_notes": [note]}   

# --- 5. WORKFLOW BUILDER ---
def build_velocity_app():
    workflow = StateGraph(AgentState)
    
    # Add Nodes
    workflow.add_node("thinker", thinker_node)
    workflow.add_node("analyst", analyst_node)
    workflow.add_node("orchestrator", orchestrator_node)
    workflow.add_node("act", act_node)
    workflow.add_node("consolidator", consolidator_node)
    workflow.add_node("scientist", scientist_node)
    workflow.add_node("drafter", draft_node)

    # Set Entry and Edges
    workflow.set_entry_point("thinker")
    workflow.add_edge("thinker", "analyst")
    
    workflow.add_conditional_edges(
        "analyst",
        lambda x: x["_validate_route"],
        {
            "recontextualize": "thinker",
            "proceed": "orchestrator",
            "next_hypothesis": "orchestrator",
            "finalize": "scientist"
        }
    )
    
    workflow.add_edge("orchestrator", "act")
    workflow.add_edge("act", "consolidator")
    workflow.add_edge("consolidator", "analyst")
    workflow.add_edge("scientist", "drafter")
    workflow.add_edge("drafter", END)
    
    return workflow.compile()

# --- 6. ADK AGENT CLASS ---
from typing import Any # Ensure Any is imported

# --- 6. ADK AGENT CLASS ---
class VelocityAgent(Agent):
    # Add this line to define 'app' as a valid field for Pydantic
    app: Any = None 

    def __init__(self, **kwargs):
        # Provide the mandatory 'name' field if not already present
        if "name" not in kwargs:
            kwargs["name"] = "velocity_agent"
        
        super().__init__(**kwargs)
        # Now Pydantic will allow this assignment
        self.app = build_velocity_app()

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        user_query = input_data.get("question", "Summarize global performance.")
        
        initial_state = {
            "question": user_query,
            "history": [],
            "detail_plan": {},
            "current_h_index": 0,
            "working_notes": ["🚀 ADK Agent Initialized"],
            "iteration_status": "Planning",
            "iteration_count": 0,
            "consolidated_results": [],
            "analyst_summaries": [],
            "data_scientist_summary": "",
            "draft_answer": ""
        }
        
        # Run graph with recursion limit for drill-downs
        final_state = self.app.invoke(initial_state, {"recursion_limit": 50})
        
        return {
            "answer": final_state.get("draft_answer", "Error generating report."),
            "metadata": {"iterations": final_state.get("iteration_count")}
        }

# Initialize for ADK
root_agent = VelocityAgent()

from google.adk.apps import App

app = App(root_agent=root_agent, name="my_agent_code")


from google.adk.apps import App

app = App(root_agent=root_agent, name="my_agent_code")
