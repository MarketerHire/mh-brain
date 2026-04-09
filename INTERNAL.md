# mh-brain — Internal Architecture

How the intelligence layer connects to mh1-hq, the mh2 builds, and mh-os.

---

## What mh-brain is

A standalone service (deployed on Modal) that accumulates learning from every skill execution across every client. It exposes an HTTP API. All other repos consume it — they never run the intelligence engine locally.

## How it connects

```
┌──────────────────────────────────────────────────────────────────┐
│  mh2 builds (rocket-service, fast-mold, etc.)                    │
│                                                                  │
│  00_data/semantic-layer/   → glossary, metrics, entities         │
│  10_context/               → brand voice, personas, products     │
│  20_intelligence/          → driver tree, revenue model, signals │
│  30_strategy/              → lever priorities, campaign backlog  │
│                                                                  │
│  Trigger.dev tasks → shared/signals.ts    (local + Supabase)     │
│                    → shared/airtable.ts   (recommendations)      │
│                    → shared/brightmatter.ts (episodes + guidance) │
└──────────────────────┬─────────────────────┬─────────────────────┘
                       │ HTTP                │ Supabase events
┌──────────────────────┼─────────────────────┼─────────────────────┐
│  mh1-hq (execution engine)                                       │
│  lib/intelligence_bridge.py                                      │
│  ─ get_skill_guidance() → predictions + parameter recs           │
│  ─ start_tracking() → registers prediction                       │
│  ─ complete_tracking() → records outcome, triggers learning      │
│  ─ consolidate_from_module() → extracts patterns from run        │
│                                                                  │
│  lib/execution/event_emitter.py                                  │
│  ─ emit_skill_completed() → writes to shared Supabase table     │
│  ─ emit_plan_completed() → writes to shared Supabase table      │
└──────────────────────┼───────────────────────────────────────────┘
                       │ HTTP + Supabase events
┌──────────────────────▼───────────────────────────────────────────┐
│  mh-brain (this repo — brightmatter/)                            │
│                                                                  │
│  api.py               → FastAPI: /guidance, /episodes,           │
│                          /tracking, /outcomes, /consolidation     │
│  worker.py            → Cron: processes events from Supabase,    │
│                          runs consolidation every 15 min          │
│  modal_app.py         → Deployment: API + crons on Modal         │
│                                                                  │
│  lib/intelligence_bridge.py   → local engine wrapper             │
│  lib/intelligence/            → the actual brain:                │
│    memory/working.py          → volatile scratch (per-session)   │
│    memory/episodic.py         → decaying experience log          │
│    memory/semantic.py         → Bayesian-updated patterns        │
│    memory/procedural.py       → cross-skill meta-strategies      │
│    memory/consolidation.py    → promotion pipeline               │
│    learning/predictor.py      → outcome predictions              │
│    learning/learner.py        → pattern updates from outcomes    │
│    learning/shadow.py         → shadow model evaluation          │
│    learning/gold_standard.py  → regression benchmarks            │
│    outcomes/                  → deferred outcome processing      │
│    adapters/                  → domain-specific scoring          │
└──────────────────────────────────────────────────────────────────┘
```

## The learning loop

Every skill execution is a prediction-outcome pair. The loop:

1. **Before execution** — mh-brain returns guidance: predicted outcomes, confidence, parameter recommendations based on patterns from prior runs across all clients.

2. **During execution** — The skill runs in its sandbox (mh1-hq or mh2 build). mh-brain is not involved.

3. **After execution** — The result is recorded. mh-brain compares prediction vs actual, stores the episode, and updates patterns if the evidence is strong enough.

4. **Consolidation (every 15 min)** — Episodes decay. Recurring patterns graduate from episodic to semantic memory (Bayesian updates). Patterns that appear across 3+ skills graduate to procedural knowledge. Stale patterns are archived.

## Memory layers

| Layer | What it holds | Lifetime | Storage |
|-------|--------------|----------|---------|
| Working | Current session scratch | Session only | In-memory |
| Episodic | Individual prediction-outcome pairs | 90-day TTL, decays | Firebase |
| Semantic | Statistical patterns with confidence | Persistent, confidence-gated | Firebase |
| Procedural | Cross-skill meta-strategies | Persistent, validated across 3+ skills | Firebase |

Upward flow is automatic (consolidation promotes). Downward flow closes the loop (patterns feed guidance for new executions).

## The intelligence already in mh2 builds (and where mh-brain fits)

Each mh2 repo (rocket-service, fast-mold, etc.) has its own local intelligence built into four numbered directories. mh-brain doesn't replace this — it sits on top and adds cross-client learning that no single client repo can do alone.

```
mh2-rocket-service/                         mh-brain
├── 00_data/semantic-layer/                 (not involved — this is local)
│   glossary.yaml    — canonical definitions
│   metrics.yaml     — metric formulas
│   entities.yaml    — data model
│   cohort-methodology.md
│
├── 10_context/                             (not involved — this is local)
│   BRAND-BRAIN-TEMPLATE.md  — 9-file client identity
│   brand/    — voice, positioning, guardrails
│   icp/      — personas, products, objections
│
├── 20_intelligence/                        ← mh-brain READS from here
│   driver-tree.md       — revenue decomposition       and WRITES back
│   revenue-model.md     — revenue equation + actuals
│   compounding-loops.md — 6 virtuous cycles
│   signals/             — signal log from Trigger.dev tasks
│
├── 30_strategy/                            ← mh-brain recommendations
│   lever-priorities.md  — ranked levers       land here (via Airtable,
│   campaign-backlog.md  — queued campaigns     human-approved)
│
├── shared/
│   signals.ts       — persist signals to 20_intelligence/signals/
│   airtable.ts      — write recommendations to Airtable for approval
│   brightmatter.ts  — ← THE BRIDGE to mh-brain
│
└── [execution modules: creative/, paid-ads/, email-sms/, seo/, etc.]
```

**What each layer does locally vs what mh-brain adds:**

| Layer | What the mh2 build knows (local) | What mh-brain adds (cross-client) |
|-------|----------------------------------|-----------------------------------|
| `00_data/` | This client's metric definitions, data model, cohort methodology | Nothing — this is client-specific infrastructure |
| `10_context/` | This client's brand voice, personas, products, competitors | Nothing — brand identity is never shared across clients |
| `20_intelligence/` | This client's driver tree, revenue model, signal history | Patterns from all clients: "brands with churn >10% respond better to retention-first framing" |
| `30_strategy/` | This client's lever priorities and campaign backlog | Confidence-scored recommendations grounded in what worked for similar clients |

## The Trigger.dev signal loop

The mh2 builds use Trigger.dev for scheduled data tasks. This is where mh-brain integrates:

```
Trigger.dev task fires (e.g., shopify-weekly, ads-weekly)
  │
  ├─ 1. getBrightMatterContext("shopify-weekly")
  │     → mh-brain returns relevant patterns + confidence
  │     → injected into Claude prompt alongside client data
  │
  ├─ 2. Task runs (Claude analyzes, generates signal + recommendations)
  │
  ├─ 3. persistSignal(signal)
  │     → writes to 20_intelligence/signals/ (local markdown + JSONL)
  │     → writes to Supabase (shared)
  │
  ├─ 4. writeRecommendation(rec)
  │     → writes to Airtable with Status: "Open"
  │     → human reviews and approves/denies
  │
  └─ 5. writeBrightMatterEpisode(episode)
        → fires to mh-brain API (fire-and-forget)
        → feeds the learning loop
```

The signal persistence layer (`shared/signals.ts`) dual-writes: local markdown for the mh2 repo's intelligence layer + Supabase for the shared event bus. The recommendation layer (`shared/airtable.ts`) puts actions in front of humans. The brightmatter layer (`shared/brightmatter.ts`) closes the loop by feeding outcomes back to mh-brain.

**What completes the loop:** Without mh-brain, each mh2 build is an island — it knows its own client's data but has no way to learn from patterns across clients. mh-brain is the layer that says "we've seen this pattern 23 times across 8 clients, and here's what worked." The local `20_intelligence/` layer holds client-specific signals. mh-brain holds statistical patterns that no single client could generate alone.

## How mh1-hq uses it

mh1-hq's execution engine (engine.py, cloud_engine.py) uses `IntelligenceBridge` from `lib/intelligence_bridge.py`. When `BRIGHTMATTER_URL` is set, a `RemoteIntelligenceBridge` routes calls to this service instead of running the engine locally.

The bridge maps all 64 skills to 5 domains (revenue, health, content, campaign, generic) for pattern matching. Phase 0 computed metrics (from data retrieval) are injected into context so the predictor can match patterns against actual client data.

After plan completion, `consolidate_from_module()` extracts learnings and stores Phase 0 snapshots for temporal comparison (WoW, MoM deltas).

## What lives where (decision guide)

| Kind of knowledge | Where it lives | Why |
|-------------------|---------------|-----|
| "What is AOV?" | `00_data/semantic-layer/glossary.yaml` in the mh2 build | Universal definition, doesn't change per client |
| "Our brand voice is warm and direct" | `10_context/brand/voice_and_tone.md` in the mh2 build | Client-specific, never shared |
| "Our CVR dropped 15% this week" | `20_intelligence/signals/` in the mh2 build | Client-specific signal from Trigger.dev |
| "Brands with CVR drops >10% recover faster with urgency-based email flows" | mh-brain semantic memory | Cross-client pattern, learned from many executions |
| "Focus on retention this quarter" | `30_strategy/lever-priorities.md` in the mh2 build | Client-specific decision, human-approved |
| "Retention-first framing improves engagement across lifecycle skills for high-churn segments" | mh-brain procedural memory | Cross-skill meta-strategy, validated across 3+ skills and multiple clients |

## Deployment

```
modal deploy modal_app.py
```

Runs on Modal with these scheduled functions:

| Function | Schedule | What it does |
|----------|----------|-------------|
| `api_endpoint` | Always on | FastAPI serving guidance, episodes, tracking |
| `worker_cron` | Every 15 min | Process Supabase events, run consolidation |
| `platform_data_cron` | Daily 1pm EST | Pull platform data (Google Ads, Meta, Shopify, Klaviyo) |
| `weekly_eval` | Sundays 10:00 UTC | Shadow model evaluation, gold standard benchmarks |
| `improvement_review` | Mondays 12:00 UTC | Pattern analysis, improvement proposals |

## Key env vars

| Var | Used by | Purpose |
|-----|---------|---------|
| `BRIGHTMATTER_URL` | mh1-hq, mh2 builds | API endpoint for this service |
| `BRIGHTMATTER_API_KEY` | mh1-hq, mh2 builds | Shared auth key |
| `SUPABASE_URL` + `SUPABASE_KEY` | mh-brain, mh1-hq | Shared event bus + data store |
| `FIREBASE_*` | mh-brain | Memory persistence (episodic, semantic, procedural) |
