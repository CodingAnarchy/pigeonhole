# Using the memory tools

This server keeps long-term memory in one Pigeonhole file. It lasts across conversations,
so a later conversation can recall what an earlier one learned.

## Recall before acting
- **Call `memory_recall` before you act on anything the user may have told you before:**
  their preferences, a project's conventions, earlier decisions, names and facts.
- **Set `max_bytes` to the context you can spare.** The hits stop there, best first, and
  `truncated` says whether more would have fit a bigger budget. Use a small budget for a
  quick check and a larger one before a big task.
- **Use `k` for the most hits,** 10 by default.
- **After you use hits, call `memory_mark_used`** with the ids of the ones that helped. Used
  memories rank higher later and are kept longer. Don't mark hits you ignored.

## What to remember
- **Remember durable things:** facts about the user or the project, decisions and their
  reasons, preferences, and lessons from mistakes.
- **Don't remember chatter,** greetings, what the conversation already holds, or anything
  you can look up again cheaply.
- **One self-contained fact per memory.** Write it so it still makes sense with no
  context: "Dana prefers tabs in Go code", not "they prefer tabs".
- **`kind`:** `semantic` for facts and preferences, `episodic` (the default) for something
  that happened, and `procedural` for how to do something.
- **`importance`, from 0 to 1, is 0.5 by default.** Go near 1 only for what must not be
  lost, such as safety constraints or an explicit "always" or "never" from the user.
- **An exact duplicate of a live memory merges into it** (outcome `merged`), so
  re-remembering a fact is harmless.

## Namespaces
- **A `namespace` is whose or what memory it is:** a user, a project, or an agent role.
  Pick a stable name, such as `user:dana` or `project:pigeonhole`.
- **Recall looks only in the namespaces you name** (`namespace`, or `namespaces` for
  several), so one user's memories never surface for another.
- **Links never cross namespaces.**

## When a fact changes
- **Don't store a contradiction next to the old fact.** Call `memory_remember` with the new
  fact and set `supersedes` to the old memory's id. Recall then leaves the old one out,
  and `include_superseded` shows it with `superseded_by`.
- **`memory_supersede` does the same for two memories that already exist.**
- **Superseding is a second step after the write.** If it fails, the new memory is still
  stored: retry `memory_supersede`.
- **Use `memory_forget` only when a memory is wrong or the user asks you to forget.** It
  deletes for good.

## Links
- **`memory_link` connects two memories:** `derived_from` (a summary drawn from its
  sources), `similar_to`, or an application's own numbered type. The weight runs from 0
  to 1.

## Embeddings
The store is pinned to one embedding model when it's created: the model's id and its
dimension. Vectors from different models aren't comparable, so mixing them silently ruins
recall.
- **Started with `--embed-url`:** the server computes every embedding itself. Send text
  only, never an `embedding`. The memory text and your recall queries go to the configured
  endpoint.
- **A store pinned to a model, without `--embed-url`:** `memory_remember` needs an
  `embedding` of the text from exactly that model, with `embedding_model` set to its id.
  `memory_recall` can take one too, or recall by text alone.
- **A store without a model:** send text only. Recall ranks by keywords, recency,
  importance and use.
- **Never send an embedding from another model,** even one of the same dimension. The
  server refuses one whose `embedding_model` isn't the store's, but it can't tell if you
  name the store's model for another model's vectors.

## Forgetting
- **The store may have a forget policy** (`phdb mcp --memory-episodic-ttl`, or one set by
  the application). It deletes old episodic memories in the background.
- **Pinned memories (`pinned: true`) and the sources of a summary are kept.**
