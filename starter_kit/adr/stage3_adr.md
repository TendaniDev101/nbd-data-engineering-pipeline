# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** [your name]
**Date:** [date of submission]
**Status:** Final

---

## Instructions (remove this section before submitting)

This ADR is required at Stage 3. It must be present in your repository at `adr/stage3_adr.md` when you push the `stage3-submission` tag.

The ADR is human-reviewed at the finalist stage and is worth **2 points** out of 100. Scoring:
- All three questions addressed substantively: **2 points**
- One or two questions answered superficially: **1 point**
- ADR absent or none of the questions addressed: **0 points**

**What "substantive" means:** Each question requires a minimum of approximately 100 words of specific, concrete reasoning about your own pipeline. General statements ("I would have made it more modular") do not qualify as substantive. Specific statements do ("I would have separated the schema definition from the transformation logic and placed it in `config/schemas.py`, because when Stage 3 required a new output table I had to modify `transform.py` in three places that all referenced the Gold schema directly").

**What reviewers are looking for:** Evidence that you understand the architectural trade-offs you made, not evidence that you know what good architecture looks like in the abstract. Reference your own code. Name specific files, classes, or design decisions.

Delete these instructions before submitting.

---

## Context

[1–2 paragraphs describing the Stage 3 requirement and the constraints you were working within.]

[Describe: what did the mobile product team need? What did the streaming interface look like — directory of micro-batch JSONL files, delivered to `/data/stream/`, requiring polling? What output tables were required (`current_balances`, `recent_transactions`) and what SLA applied (5-minute lag from file arrival to Gold update)?]

[Also describe: what was the state of your pipeline coming into Stage 3? Approximately how many lines of code, what structure, what had you changed between Stage 1 and Stage 2?]

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

**Minimum: approximately 100 words of specific reasoning about your own pipeline.**

[Address at least the following:]

[**What made Stage 3 easier:**]
[— Which specific design choices in Stage 1 or Stage 2 reduced the work required to add the streaming path. Examples: did you already have a modular ingestion layer that made it easy to add a new input source? Did your Delta MERGE pattern from Stage 2 transfer directly to the streaming upsert logic? Was your config-driven path setup easy to extend with a `/data/stream/` source?]

[**What made Stage 3 harder:**]
[— Which specific choices created friction. Examples: did you use a monolithic `run_all.py` that combined batch and stream concerns in a way that was difficult to separate? Did you have hardcoded schema assumptions that broke when the new `current_balances` table was introduced? Did your Spark session configuration conflict with the polling loop's concurrency requirements?]

[**Code survival rate:**]
[— Roughly what fraction of your Stage 1/2 code survived intact into Stage 3? What had to be modified versus extended versus rewritten?]

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

**Minimum: approximately 100 words of specific, concrete changes.**

[Be specific. "I would have..." followed by a concrete architectural choice and an explanation of why it would have improved Stage 3. General statements are not sufficient.]

[Examples of the level of specificity required:]
[— "I would have defined my Gold table schemas in a single `config/schemas.py` file rather than inline in `provision.py`. When I needed to add the `current_balances` table in Stage 3, I had to trace schema definitions across three modules."]
[— "I would have designed `run_all.py` to accept a `--mode` argument (`batch` or `stream`) from the start, rather than adding a branching conditional in Stage 3 that made the entry point harder to reason about."]
[— "I would not have used `.toPandas()` in my Silver-to-Gold join. It worked at Stage 1 scale but I had to refactor it at Stage 2, and the refactor left technical debt that complicated the Stage 3 streaming path."]

[Describe at least one concrete structural change.]

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

**Minimum: approximately 100 words of forward-looking architectural reasoning.**

[This is the forward-looking design question. Describe the architecture you would have chosen from Day 1 if the full three-stage specification had been visible to you at the start.]

[Consider addressing:]
[— **Ingestion patterns:** Would you have designed the ingest layer to accept both batch file paths and streaming directory sources from the beginning? What interface would that look like?]
[— **State management:** The `current_balances` table requires maintaining running state across stream batches. Would you have chosen a different state management approach if you had known this from Day 1? Delta MERGE, checkpoint files, an embedded key-value store?]
[— **Output format choices:** Would you have structured your Gold layer differently — for example, using a single `gold/` output module that handles both batch and streaming tables — rather than retrofitting `stream_gold/` as a separate output path?]
[— **Pipeline entry points:** Would you have used a single entry point with mode selection, or kept batch and streaming as separate executables that share common library code?]
[— **Anything else** specific to your implementation that would have changed with full visibility.]

---

## Appendix (optional)

[Architecture diagram, code snippets, or other supporting material. Not required. Does not substitute for the written responses above. If you include a diagram, describe what it shows in plain text as well — reviewers may not have access to rendering tools.]
