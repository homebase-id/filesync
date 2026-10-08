# CLAUDE.md

Homebase desktop file-sync client, written in Rust, targeting Windows, macOS and Linux. It
syncs a local folder tree with a Homebase drive (server: `../odin-core`; mobile client
reference: `../chat-kmp`).

Status: design phase, no code yet. Read these first:
- `docs/foundations.md`: the pinned decisions (client-computed stored + content hashes on
  `PayloadDescriptor`, id tree with `GroupId` = parent, start-up catch-up tiers, placeholder
  strategy, and the odin-core issues #1892, #1893 and #1895).
- `docs/roadmap.md`: the layered, step-by-step build plan.
- `docs/modules.md`: the module sketch (its step list is superseded by the roadmap).

Ground rules:
- Never lose data. The reconciler is a pure function; all OS and network work sits behind traits.
- Build against a fake remote that models the real node schema before touching Homebase.
- Homebase encryption is out of scope here for now; the focus is the three-OS side.
- When stating facts about the Homebase API, say whether they were verified in the sibling repos'
  code or inferred.
