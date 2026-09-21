"""Record WHICH MCP server each tool call hit.

Revision ID: 067
Revises: 066
Create Date: 2026-09-22

The ten MCP servers (/mcp, /cbs/mcp, /knesset/mcp, ... /deals/mcp) share one
authorization server and one usage table, and the row carried only the tool
name. Eleven tool names exist on more than one server (run_sql, get_stats,
parcel_deals, ...), so "how much does each user use each service" was not
answerable. The server is now stamped on every row (app/mcp/usage.py).

Backfill is best-effort and literal: a tool name that exists on exactly one
server identifies it, measured against the servers' TOOLS lists on the day this
was written. Rows of a shared name stay NULL ("unknown") rather than guessed.
"""
from alembic import op

revision = "067"
down_revision = "066"
branch_labels = None
depends_on = None

_UNIQUE_TOOLS = {
    "list_tags": "over", "query_dataset_rows": "over",
    "resolve": "cbs", "get_page": "cbs", "facets": "cbs", "list_featured": "cbs",
    "search_committees": "knesset", "search_sessions": "knesset",
    "search_protocols": "knesset", "get_session": "knesset", "search_mmm": "knesset",
    "search_events": "ocal", "get_event": "ocal", "list_entities": "ocal",
    "list_sources": "ocal", "find_meetings_between": "ocal",
    "entity_get": "ocoi", "graph_neighbors": "ocoi", "graph_path": "ocoi",
    "document_get": "ocoi", "document_entities": "ocoi", "top_connected": "ocoi",
    "by_ministry": "ocoi", "registry_lookup": "ocoi",
    "list_schemas": "data", "get_table": "data",
    "search_donations": "elections", "donor_profile": "elections",
    "recipient_profile": "elections", "top_donors": "elections",
    "list_election_types": "elections",
    "lookup_property": "nadlan", "suggest_streets": "nadlan",
    "parcel_geometry": "nadlan", "coverage_stats": "nadlan",
    "search_deals": "deals", "price_series": "deals", "compare_settlements": "deals",
    "list_settlements": "deals", "list_deal_types": "deals", "register_stats": "deals",
}


def upgrade() -> None:
    op.execute("ALTER TABLE auth.mcp_usage_events ADD COLUMN IF NOT EXISTS mcp_server varchar(40)")
    # The in-process deep search logs under a fixed session id, not a server.
    op.execute("UPDATE auth.mcp_usage_events SET mcp_server = 'deep_search' "
               "WHERE mcp_server IS NULL AND mcp_session_id = 'deep-search'")
    values = ", ".join(f"('{t}', '{s}')" for t, s in _UNIQUE_TOOLS.items())
    op.execute(f"""
        UPDATE auth.mcp_usage_events e SET mcp_server = m.server
        FROM (VALUES {values}) AS m(tool, server)
        WHERE e.mcp_server IS NULL AND e.tool_name = m.tool
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_mcp_usage_user_server "
               "ON auth.mcp_usage_events (api_user_id, mcp_server)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS auth.ix_mcp_usage_user_server")
    op.execute("ALTER TABLE auth.mcp_usage_events DROP COLUMN IF EXISTS mcp_server")
