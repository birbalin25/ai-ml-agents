# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

@AGENTS.md

## Commands

```bash
# Setup & auth
uv run quickstart                    # Interactive setup (auth, MLflow experiment, deps)
uv run discover-tools                # List available workspace resources

# Local development
uv run start-app                     # Start backend (port 8000) + frontend chat UI (port 3000)
uv run start-server                  # Backend only
uv run start-server --reload         # Backend with hot-reload

# Deploy (always run BOTH commands)
databricks bundle deploy -t <target> -p <profile>
databricks bundle run agent_langgraph -t <target> -p <profile>

# Logs
databricks apps logs <app-name> -p <profile> --follow

# Evaluate
uv run agent-evaluate

# Dependencies
uv add <package>
uv sync
```

## Architecture

```
Request → @invoke/@stream decorators → streaming()
  ├── _get_or_create_thread_id()     → thread_id for short-term memory
  ├── get_user_id()                  → user_id for long-term memory
  ├── init_mcp_client()              → MCP tools (system.ai.python_exec, UC functions)
  ├── memory_tools()                 → get/save/delete_user_memory (long-term)
  ├── query_uc_upgrade_knowledge()   → Knowledge Assistant serving endpoint tool
  │
  ├── AsyncCheckpointSaver           → short-term: conversation history per thread_id
  └── AsyncDatabricksStore           → long-term: user facts per user_id (vector search)
        └── create_react_agent(model, tools, checkpointer) → LangGraph ReAct loop
```

### Key Files

| File | Role |
|------|------|
| `agent_server/agent.py` | Agent logic: tools, memory, LLM config, MCP servers, streaming |
| `agent_server/memory_tools.py` | Long-term memory tool factory (`memory_tools()`) + helpers |
| `agent_server/utils.py` | `process_agent_astream_events()`, workspace client helpers |
| `agent_server/start_server.py` | MLflow `AgentServer` + FastAPI app init |
| `scripts/start_app.py` | Process manager running backend + Next.js frontend |
| `databricks.yml` | Bundle config: app resources, permissions, targets |
| `app.yaml` | App container config: command, env vars |

### Current Tools

| Tool | Type | Purpose |
|------|------|---------|
| `system.ai.python_exec` | MCP (built-in) | Code interpreter |
| `serverless_bir_catalog.birschema.get_weather` | MCP (UC function) | Weather lookup by city |
| `query_uc_upgrade_knowledge` | Agent code tool | Queries `ka-3c7731c6-endpoint` (Knowledge Assistant) for UC upgrade / HMS federation info |
| `get_user_memory` / `save_user_memory` / `delete_user_memory` | Agent code tools | Long-term memory via Lakebase vector search |

### How Memory Works

- **Short-term** (`AsyncCheckpointSaver`): Stores full conversation state per `thread_id` in Lakebase. Same thread_id = continued conversation. Auto-generated UUID if not provided via `custom_inputs.thread_id`.
- **Long-term** (`AsyncDatabricksStore`): Stores user facts per `user_id` with vector embeddings for semantic search. Tools access store via `RunnableConfig`. Namespace: `("user_memories", user_id.replace(".", "-"))`.

### How to Add Tools

**UC Function via MCP:**
1. Add a `DatabricksMCPServer` in `init_mcp_client()` in `agent.py` with URL `{host}/api/2.0/mcp/functions/{catalog}/{schema}/{function_name}`
2. Add `uc_securable` resource in `databricks.yml` with `permission: 'EXECUTE'`

**Serving Endpoint (Responses API):**
1. Create a `@tool` function using `DatabricksOpenAI().responses.create(model=endpoint_name, input=[{"role": "user", "content": query}])` — input must be messages array, not plain string
2. Wrap in try/except to always return a string (prevents checkpoint corruption)
3. Add to `all_tools` list in `streaming()`
4. Add `serving_endpoint` resource in `databricks.yml` with `permission: 'CAN_QUERY'`

**Custom code tool:**
1. Define with `@tool` decorator, add to `all_tools` in `streaming()`

### Configuration Flow

`databricks.yml` declares resources/permissions → `app.yaml` exposes env vars → `agent.py` reads `os.environ`. For local dev, `.env` provides the same vars. Lakebase instance name must use `value:` (not `valueFrom:`) in `app.yaml`.

## Critical Patterns

- **Tool error handling**: Agent tools calling external services MUST be wrapped in try/except and always return a string. If a tool raises, the AIMessage with tool_call gets checkpointed but no ToolMessage is saved, permanently corrupting that thread's checkpoint.
- **Frontend DB conflict**: `scripts/start_app.py` strips database env vars (`DATABASE*`, `LAKEBASE*`, `POSTGRES*`, `PG*`) from the frontend subprocess so Drizzle ORM doesn't try to migrate against Lakebase.
- **Lakebase table init**: Tables must be initialized once locally before first deploy via `await store.setup()` and `await checkpointer.setup()`.
- **Bundle binding**: When `databricks bundle deploy` fails with "already exists", bind with `databricks bundle deployment bind <resource_key> <resource_id> -t <target> -p <profile>`.

## Skills Reference

Read the relevant skill file in `.claude/skills/` before executing agent tasks:

| Task | Path |
|------|------|
| Setup & auth | `.claude/skills/quickstart/SKILL.md` |
| Add tools & permissions | `.claude/skills/add-tools/SKILL.md` |
| Deploy | `.claude/skills/deploy/SKILL.md` |
| Run locally | `.claude/skills/run-locally/SKILL.md` |
| Modify agent | `.claude/skills/modify-agent/SKILL.md` |
| Lakebase setup | `.claude/skills/lakebase-setup/SKILL.md` |
| Agent memory | `.claude/skills/agent-memory/SKILL.md` |
| Discover resources | `.claude/skills/discover-tools/SKILL.md` |
