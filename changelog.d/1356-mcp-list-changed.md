### Changed
- `phdb mcp` advertises `tools.listChanged` and sends `notifications/tools/list_changed` when a call changes the tool list, so a session that creates its first vector family can discover `nearest` without reconnecting (#1356).
