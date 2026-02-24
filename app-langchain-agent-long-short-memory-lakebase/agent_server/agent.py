import os
import uuid
from typing import AsyncGenerator, Optional

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    AsyncCheckpointSaver,
    AsyncDatabricksStore,
    ChatDatabricks,
    DatabricksMCPServer,
    DatabricksMultiServerMCPClient,
)
from databricks_openai import DatabricksOpenAI
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    to_chat_completions_input,
)

from agent_server.memory_tools import get_user_id, memory_tools, resolve_lakebase_instance_name
from agent_server.utils import (
    get_databricks_host_from_env,
    get_user_workspace_client,
    process_agent_astream_events,
)

mlflow.langchain.autolog()
sp_workspace_client = WorkspaceClient()

# Lakebase configuration for memory
LAKEBASE_INSTANCE_NAME = resolve_lakebase_instance_name(
    os.environ.get("LAKEBASE_INSTANCE_NAME", "bir-long-short-langchain"),
    sp_workspace_client,
)
EMBEDDING_ENDPOINT = os.environ.get("EMBEDDING_ENDPOINT", "databricks-gte-large-en")
EMBEDDING_DIMS = int(os.environ.get("EMBEDDING_DIMS", "1024"))

LLM_ENDPOINT_NAME = "databricks-gpt-5-2"
UC_UPGRADE_ENDPOINT = "ka-3c7731c6-endpoint"
SYSTEM_PROMPT = (
    "You are a helpful assistant. Use available tools to answer questions. "
    "You have the following tools:\n"
    "- **Memory tools** (get_user_memory, save_user_memory, delete_user_memory): Use these to remember and recall user preferences and important details across conversations.\n"
    "- **get_weather**: Use this to get the current weather for a city when users ask about weather conditions.\n"
    "- **system.ai.python_exec**: Use this to execute Python code for calculations, data analysis, or any computation the user requests.\n"
    "- **query_uc_upgrade_knowledge**: Use this to answer questions about the phased approach to upgrade Unity Catalog using internal HMS federation, migration phases, steps, or timelines."
)


@tool
def query_uc_upgrade_knowledge(query: str) -> str:
    """Query the Knowledge Assistant for information about Unity Catalog upgrade using internal HMS federation.

    Use this tool when users ask about:
    - Phased approach to upgrade Unity Catalog (UC)
    - Internal HMS (Hive Metastore) federation
    - UC migration phases, steps, or timelines
    - HMS to UC migration strategies

    Args:
        query: The question about UC upgrade or HMS federation to search for.
    """
    try:
        client = DatabricksOpenAI()
        response = client.responses.create(
            model=UC_UPGRADE_ENDPOINT,
            input=[{"role": "user", "content": query}],
        )
        return response.output_text
    except Exception as e:
        return f"Error querying Knowledge Assistant: {e}"


def init_mcp_client(workspace_client: WorkspaceClient) -> DatabricksMultiServerMCPClient:
    host_name = get_databricks_host_from_env()
    return DatabricksMultiServerMCPClient(
        [
            DatabricksMCPServer(
                name="system-ai",
                url=f"{host_name}/api/2.0/mcp/functions/system/ai",
                workspace_client=workspace_client,
            ),
            DatabricksMCPServer(
                name="get-weather",
                url=f"{host_name}/api/2.0/mcp/functions/serverless_bir_catalog/birschema/get_weather",
                workspace_client=workspace_client,
            ),
        ]
    )


def _get_or_create_thread_id(request: ResponsesAgentRequest) -> str:
    """Extract thread_id from custom_inputs or generate a new one."""
    custom_inputs = dict(request.custom_inputs or {})
    if "thread_id" in custom_inputs:
        return custom_inputs["thread_id"]
    if request.context and getattr(request.context, "conversation_id", None):
        return request.context.conversation_id
    return str(uuid.uuid4())


@invoke()
async def non_streaming(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    outputs = [
        event.item
        async for event in streaming(request)
        if event.type == "response.output_item.done"
    ]
    return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)


@stream()
async def streaming(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    # Extract thread_id for short-term memory (conversation history)
    thread_id = _get_or_create_thread_id(request)

    # Extract user_id for long-term memory (user preferences/facts)
    user_id = get_user_id(request)

    # Store thread_id back in custom_inputs so it's returned in custom_outputs
    custom_inputs = dict(request.custom_inputs or {})
    custom_inputs["thread_id"] = thread_id
    request.custom_inputs = custom_inputs

    # Initialize MCP tools
    mcp_client = init_mcp_client(sp_workspace_client)
    mcp_tools = await mcp_client.get_tools()

    # Combine MCP tools with long-term memory tools
    all_tools = mcp_tools + memory_tools() + [query_uc_upgrade_knowledge]

    model = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
    messages = {"messages": to_chat_completions_input([i.model_dump() for i in request.input])}

    # Short-term memory: AsyncCheckpointSaver persists conversation history per thread_id
    # Long-term memory: AsyncDatabricksStore persists user facts per user_id with vector embeddings
    async with AsyncCheckpointSaver(
        instance_name=LAKEBASE_INSTANCE_NAME,
    ) as checkpointer:
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
                checkpointer=checkpointer,
            )

            config = {
                "configurable": {
                    "thread_id": thread_id,
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
