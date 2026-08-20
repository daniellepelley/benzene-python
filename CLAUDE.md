# Benzene for Python — Project Guide for Claude Code

See @AGENTS.md for the full project guide and conventions — the same content other AI coding tools
(Codex, Cursor, Copilot, etc.) read via the cross-tool `AGENTS.md` standard. This file exists only
because Claude Code specifically looks for `CLAUDE.md`; keep it a thin pointer rather than
duplicating content here, to avoid the two drifting out of sync.

This is the **Python port** of Benzene. The language-neutral specification and the conformance
fixtures live in the cross-language [Benzene](https://github.com/daniellepelley/Benzene) repo;
`conformance/` here is a vendored snapshot of them, and is never edited to make this port pass.
