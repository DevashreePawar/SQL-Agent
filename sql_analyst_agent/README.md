# 📊 SQL Data Analyst Agent

An intelligent, autonomous AI agent that translates natural language questions into executable SQL, dynamically inspects databases, self-corrects errors, and generates rich markdown reports. 

This project is **Project 3** in a progressive series on Agentic AI, building upon the core ReAct loops and Multi-Phase planning concepts from previous projects.

---

## 🧠 Six New Agentic Concepts

This project introduces six advanced concepts for building robust, enterprise-grade AI agents:

| # | Concept | Description | Why It Matters |
|---|---------|-------------|----------------|
| 1 | **Dynamic Tool Generation** | The agent reads the database at runtime and creates perfectly tailored tools (e.g., `query_orders_table()`). | Tools adapt to the live environment rather than being hard-coded. |
| 2 | **Schema-Aware Prompting** | The LLM system prompt is dynamically injected with the exact table names and column types. | Eliminates hallucinations of non-existent columns or schemas. |
| 3 | **Iterative Query Refinement** | If a SQL query fails, the agent intercepts the error, formats it cleanly, and prompts the LLM to fix it (up to 3x). | Mirrors human debugging; dramatically improves reliability. |
| 4 | **Semantic Caching** | Uses TF-IDF and Cosine Similarity to cache results. Semantically similar questions skip the LLM and database entirely. | Saves tokens, reduces latency, and lowers API costs. |
| 5 | **Multi-Database Routing** | Classifies the user's question first, then routes it to the correct target database (e.g., Sales vs. HR). | Essential for complex, multi-source enterprise data environments. |
| 6 | **End-to-End NL2SQL** | Full pipeline: Natural Language → SQL Generation → Execution → Self-Correction → Insight Analysis. | A core pattern for bringing data democratisation to businesses. |

---

## 🏗️ Architecture

The agent operates in a 3-Phase ReAct loop:

```text
 ┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐
 │   DISCOVER   │────▶│    QUERY     │────▶│       ANALYZE        │
 │              │     │              │     │                      │
 │ • Route DB   │     │ • NL → SQL   │     │ • Interpret results  │
 │ • Read schema│     │ • Execute    │     │ • Find insights      │
 │ • Check cache│     │ • Self-Fix   │     │ • Generate report    │
 └──────────────┘     └──────┬───────┘     └──────────────────────┘
                             │
                     ┌───────▼───────┐
                     │  Self-Correct │ 
                     │  (up to 3x)   │
                     └───────────────┘
```

---

## 📂 Project Structure

```text
sql_analyst_agent/
├── main.py                  # Entry point CLI
├── agent.py                 # Core ReAct orchestrator (DISCOVER → QUERY → ANALYZE)
├── schema_inspector.py      # Dynamic tool generation + schema discovery
├── sql_executor.py          # Safe SQL runtime + structured error feedback
├── query_refiner.py         # Iterative LLM self-correction loop
├── semantic_cache.py        # TF-IDF cache for semantic question matching
├── db_router.py             # Keyword-based Multi-DB router
├── tools.py                 # SQL-specific tools wrapper
├── memory.py                # State, schema, and error logging memory
├── report.py                # Markdown report formatter
├── databases/               
│   ├── sales.db             # Auto-generated e-commerce data
│   ├── hr.db                # Auto-generated employee data
│   └── seed_data.py         # Script to populate databases
└── output/                  # Generated markdown reports
```

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.10+
- A [Groq API Key](https://console.groq.com/keys) (using `llama-3.3-70b-versatile`)

### 2. Installation
Clone the repository and install the required dependencies:
```bash
pip install -r requirements.txt
```

Set up your environment variables:
```bash
cp .env.example .env
# Open .env and add your GROQ_API_KEY
```

### 3. Initialize Databases
Seed the realistic fake data for the Sales and HR databases:
```bash
python databases/seed_data.py
```
*This handles the creation of `sales.db` (~600 orders) and `hr.db` (~180 employees).*

---

## 💻 Usage

### Demo Mode
Run the built-in demo to test all 6 concepts automatically across both databases:
```bash
python main.py --demo
```

### Custom Queries
Ask any analytical question using natural language:

**Sales Database:**
```bash
python main.py --question "What are the top 5 products by total revenue?"
python main.py --question "Show me the 5 best performing sales reps."
```

**HR Database (Tests Routing):**
```bash
python main.py --question "Which department has the highest average salary?"
```

### Checking the Cache
Run identical or semantically similar queries to test the caching mechanism:
```bash
# First Run: Hits LLM & DB
python main.py --question "Who are the top 5 salespeople?"

# Second Run: Instant Cache Hit
python main.py --question "Show me the 5 best performing sales reps."

# View cache stats
python main.py --cache-stats
```

---

## 📄 Output Reports

Every successful run generates a detailed Markdown report inside the `output/` directory containing:
1. The routing decision.
2. Semantic cache status (Hit vs. Miss).
3. The exact SQL query generated.
4. The execution self-correction log (if the agent made syntactical mistakes and fixed them).
5. A nicely formatted data table of the results.
6. LLM-generated bullet-point insights explaining the data.
