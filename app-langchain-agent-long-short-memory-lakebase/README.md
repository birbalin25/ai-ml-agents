# LangGraph Agent with Short-Term & Long-Term Memory

A Databricks LangGraph conversational agent with **dual memory** (short-term conversation history + long-term user facts), **tool integration** (UC functions, Knowledge Assistant serving endpoints, code interpreter), and a built-in chat UI. Deployed as a Databricks App using Databricks Asset Bundles (DAB).

## What This Agent Does

- **Short-term memory**: Remembers conversation context within a session using `AsyncCheckpointSaver` (identified by `thread_id`)
- **Long-term memory**: Remembers user preferences and facts across sessions using `AsyncDatabricksStore` with vector embeddings (identified by `user_id`)
- **Tool calling**: Calls UC functions (e.g., weather lookup), serving endpoints (e.g., Knowledge Assistant), and built-in code interpreter
- **Streaming**: Supports both streaming and non-streaming responses via the OpenAI Responses API interface

Both memory types are backed by a **Lakebase** instance (Databricks-managed Postgres).

---

## Fields You Need to Update

Before deploying, update these values across the configuration files with your own:

| Field | Where to Update | Current Value |
|-------|----------------|---------------|
| **Lakebase instance name** | `agent_server/agent.py` (line 37), `databricks.yml`, `app.yaml`, `.env` | `bir-long-short-langchain` |
| **LLM endpoint** | `agent_server/agent.py` (line 43) | `databricks-gpt-5-2` |
| **Serving endpoint** | `agent_server/agent.py` (line 44) | `ka-3c7731c6-endpoint` |
| **UC function** | `agent_server/agent.py` (line 89), `databricks.yml` | `serverless_bir_catalog.birschema.get_weather` |
| **Workspace host** | `databricks.yml` (prod target, line 53) | `https://fevm-serverless-bir.cloud.databricks.com` |
| **App name** | `databricks.yml` (prod target, line 58) | `agent-langgraph-all-mem-app` |
| **Embedding endpoint** | `app.yaml`, `.env` | `databricks-gte-large-en` |
| **Embedding dimensions** | `app.yaml`, `.env` | `1024` |
| **System prompt** | `agent_server/agent.py` (line 45) | Describes current tools |
| **Databricks auth profile** | `.env` | `DEFAULT` |

---

## Quick Start

```bash
# 1. Setup (auth, MLflow experiment, dependencies)
uv run quickstart

# 2. Initialize Lakebase tables (first time only)
uv run python -c "
import asyncio
from databricks_langchain import AsyncCheckpointSaver, AsyncDatabricksStore
async def setup():
    async with AsyncCheckpointSaver(instance_name='<your-instance>') as cp:
        await cp.setup()
    async with AsyncDatabricksStore(instance_name='<your-instance>', embedding_endpoint='databricks-gte-large-en', embedding_dims=1024) as store:
        await store.setup()
    print('Tables created!')
asyncio.run(setup())
"

# 3. Run locally
uv run start-app

# 4. Deploy to Databricks
databricks bundle deploy -t prod -p <profile>
databricks bundle run agent_langgraph -t prod -p <profile>
```

---

## Testing with curl

```bash
# Get OAuth token
TOKEN=$(databricks auth token -p <profile> | jq -r '.access_token')

# Save a memory (long-term)
curl -X POST <app-url>/invocations \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "input": [{"role": "user", "content": "Remember that I prefer Python"}],
    "custom_inputs": {"user_id": "alice@example.com"}
  }'

# Recall memory in a new conversation (long-term)
curl -X POST <app-url>/invocations \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "input": [{"role": "user", "content": "What language do I prefer?"}],
    "custom_inputs": {"user_id": "alice@example.com"}
  }'

# Continue a conversation (short-term - use same thread_id)
curl -X POST <app-url>/invocations \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "input": [{"role": "user", "content": "What did I just say?"}],
    "custom_inputs": {"user_id": "alice@example.com", "thread_id": "<thread_id from previous response>"}
  }'
```

---

## Using Only Short-Term Memory (No Long-Term)

If you only want conversation history (no user fact storage), make these changes in `agent_server/agent.py`:

**1. Remove long-term memory imports and tools** (line 25):
```python
# Change this:
from agent_server.memory_tools import get_user_id, memory_tools, resolve_lakebase_instance_name

# To this:
from agent_server.memory_tools import resolve_lakebase_instance_name
```

**2. Remove `get_user_id` call** (comment out line 124):
```python
    # user_id = get_user_id(request)
```

**3. Remove `memory_tools()` from all_tools** (line 136):
```python
    # Change this:
    all_tools = mcp_tools + memory_tools() + [query_uc_upgrade_knowledge]

    # To this:
    all_tools = mcp_tools + [query_uc_upgrade_knowledge]
```

**4. Remove `AsyncDatabricksStore` block** — replace the nested `async with` blocks (lines 143-175) with:
```python
    async with AsyncCheckpointSaver(
        instance_name=LAKEBASE_INSTANCE_NAME,
    ) as checkpointer:
        agent = create_react_agent(
            model=model,
            tools=all_tools,
            prompt=SYSTEM_PROMPT,
            checkpointer=checkpointer,
        )

        config = {"configurable": {"thread_id": thread_id}}

        async for event in process_agent_astream_events(
            agent.astream(
                input=messages,
                config=config,
                stream_mode=["updates", "messages"],
            )
        ):
            yield event
```

**5. Remove embedding env vars** from `app.yaml`, `.env`, and the top of `agent.py` (lines 40-41).

---

## Using Only Long-Term Memory (No Short-Term)

If you only want persistent user facts (no conversation history), make these changes in `agent_server/agent.py`:

**1. Remove `AsyncCheckpointSaver` import** (line 8):
```python
# Remove AsyncCheckpointSaver from the import block
```

**2. Remove `_get_or_create_thread_id` call** (comment out lines 121, 127-129):
```python
    # thread_id = _get_or_create_thread_id(request)
    # custom_inputs["thread_id"] = thread_id
```

**3. Remove `AsyncCheckpointSaver` block and `checkpointer`** — replace the nested `async with` blocks (lines 143-175) with:
```python
    async with AsyncDatabricksStore(
        instance_name=LAKEBASE_INSTANCE_NAME,
        embedding_endpoint=EMBEDDING_ENDPOINT,
        embedding_dims=EMBEDDING_DIMS,
    ) as store:
        await store.setup()

        agent = create_react_agent(
            model=model,
            tools=all_tools,
            prompt=SYSTEM_PROMPT,
            # No checkpointer = no short-term memory
        )

        config = {
            "configurable": {
                "user_id": user_id,
                "store": store,
            }
        }

        async for event in process_agent_astream_events(
            agent.astream(
                input=messages,
                config=config,
                stream_mode=["updates", "messages"],
            )
        ):
            yield event
```

---

## Adding a New Tool

### Option A: UC Function (via MCP)

**1. Add MCP server in `agent_server/agent.py`** inside `init_mcp_client()`:
```python
DatabricksMCPServer(
    name="my-function",
    url=f"{host_name}/api/2.0/mcp/functions/<catalog>/<schema>/<function_name>",
    workspace_client=workspace_client,
),
```

**2. Grant permission in `databricks.yml`** under `resources`:
```yaml
- name: 'my_function'
  uc_securable:
    securable_full_name: '<catalog>.<schema>.<function_name>'
    securable_type: 'FUNCTION'
    permission: 'EXECUTE'
```

No code changes needed in `streaming()` — MCP tools are auto-discovered.

### Option B: Serving Endpoint (Responses API)

**1. Add tool function in `agent_server/agent.py`**:
```python
@tool
def query_my_endpoint(query: str) -> str:
    """Description of what this endpoint does."""
    try:
        client = DatabricksOpenAI()
        response = client.responses.create(
            model="my-endpoint-name",
            input=[{"role": "user", "content": query}],  # Must be messages array
        )
        return response.output_text
    except Exception as e:
        return f"Error: {e}"  # Always return string, never raise
```

**2. Add to `all_tools` list** in `streaming()`:
```python
all_tools = mcp_tools + memory_tools() + [query_uc_upgrade_knowledge, query_my_endpoint]
```

**3. Grant permission in `databricks.yml`**:
```yaml
- name: 'my_endpoint'
  serving_endpoint:
    name: 'my-endpoint-name'
    permission: 'CAN_QUERY'
```

### Option C: Custom Code Tool

**1. Define with `@tool` decorator** in `agent_server/agent.py`:
```python
@tool
def my_custom_tool(param: str) -> str:
    """Description for the LLM to know when to use this tool."""
    # Your logic here
    return result
```

**2. Add to `all_tools` list** in `streaming()`.

---

## Project Structure

```
ls-memory-app/
├── agent_server/
│   ├── agent.py              # Agent logic: tools, memory, LLM, streaming
│   ├── memory_tools.py        # Long-term memory tools (get/save/delete)
│   ├── utils.py               # Stream processing, workspace client helpers
│   ├── start_server.py        # MLflow AgentServer + FastAPI init
│   └── evaluate_agent.py      # Agent evaluation with MLflow scorers
├── scripts/
│   ├── start_app.py           # Runs backend + frontend together
│   ├── quickstart.py          # Interactive setup wizard
│   └── discover_tools.py      # Lists available workspace resources
├── .claude/skills/            # AI assistant skill guides
├── databricks.yml             # Bundle config, resources, permissions
├── app.yaml                   # App container config + env vars
├── pyproject.toml             # Python dependencies
├── .env.example               # Local dev env template
└── .env                       # Local dev env (not committed)
```

---

## Available Skills

Skills are step-by-step guides in `.claude/skills/` that AI assistants (Claude Code, Cursor, GitHub Copilot) automatically use. You can also read them directly.

### `quickstart`
**What**: Set up the development environment from scratch.
**When to use**: First time setup, configuring Databricks authentication, missing `.env` file.
**Example**: Run `uv run quickstart` to interactively configure auth, create an MLflow experiment, and start the server.

### `deploy`
**What**: Deploy the agent to Databricks Apps using DAB.
**When to use**: Pushing code to Databricks, handling "app already exists" errors, binding existing apps.
**Example**: `databricks bundle deploy -t prod -p vm && databricks bundle run agent_langgraph -t prod -p vm`

### `add-tools`
**What**: Add MCP servers, UC functions, Genie spaces, Vector Search, or serving endpoints as tools.
**When to use**: Connecting new data sources or APIs, granting resource permissions in `databricks.yml`.
**Example**: Adding a UC function requires an MCP server entry in `agent.py` + a `uc_securable` resource in `databricks.yml`.

### `modify-agent`
**What**: Change agent code, model, instructions, or tool configuration.
**When to use**: Switching LLM endpoints, updating system prompts, changing agent behavior.
**Example**: Change `LLM_ENDPOINT_NAME` in `agent.py` to use a different model like `databricks-claude-3-7-sonnet`.

### `run-locally`
**What**: Run and test the agent on your local machine.
**When to use**: Local development, testing with curl, debugging issues.
**Example**: `uv run start-app` starts both backend and chat UI; test with curl to `http://localhost:8000/invocations`.

### `discover-tools`
**What**: Find available tools and resources in your Databricks workspace.
**When to use**: Before adding tools, to see what UC functions, Genie spaces, Vector Search indexes, or MCP servers are available.
**Example**: Run `uv run discover-tools` to list all discoverable resources.

### `lakebase-setup`
**What**: Configure a Lakebase instance for agent memory storage.
**When to use**: Setting up memory for the first time, "Failed to connect to Lakebase" errors, permission issues on checkpoint/store tables.
**Example**: Create a Lakebase instance in the Databricks UI, then add the `database` resource to `databricks.yml` and initialize tables with `await store.setup()`.

### `agent-memory`
**What**: Add short-term or long-term memory capabilities to the agent.
**When to use**: Adding conversation history, persistent user preferences, or memory tools.
**Example**: Copy `memory_tools.py` to `agent_server/`, add `AsyncDatabricksStore` to `agent.py`, and include `memory_tools()` in the tools list.

---

## Key Configuration Files

### `databricks.yml` — Resources & Permissions
Every tool and resource the app uses must be declared here. The app's service principal gets permissions automatically.

### `app.yaml` — Container Environment
Env vars available to the deployed app. Use `value:` for Lakebase instance name (not `valueFrom:`).

### `.env` — Local Development
Same env vars as `app.yaml` but for local use. Copy from `.env.example`.

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `relation "checkpoints" does not exist` | Run `await checkpointer.setup()` locally to initialize tables |
| `relation "store" does not exist` | Run `await store.setup()` locally to initialize tables |
| `npm build failed: Database migration failed` | Frontend is picking up Lakebase connection; this is handled automatically in `scripts/start_app.py` |
| `An app with the same name already exists` | Bind it: `databricks bundle deployment bind <resource_key> <resource_id> -t <target> -p <profile>` |
| `Found AIMessages with tool_calls that do not have a corresponding ToolMessage` | A tool raised an exception instead of returning a string, corrupting the checkpoint. Use a new `thread_id`. Always wrap tools in try/except. |
| `Internal Server Error` from serving endpoint | The endpoint itself is failing. Test it directly with `curl` and check its logs. |
| 302 error when querying deployed app | Use an OAuth token, not a PAT. |
