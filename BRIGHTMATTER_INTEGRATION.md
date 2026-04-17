# BrightMatter Integration Architecture

How BrightMatter becomes the central learning and intelligence engine for the entire MarketerHire platform — MH-OS, mh1-hq, and mh1-skills.

---

## The Shift: API + Cron, Backed by a Shared Database

BrightMatter currently runs as a FastAPI service (`api.py` on port 8100). Consumers call HTTP endpoints to write episodes, get guidance, record outcomes, and query patterns. Firebase is the persistence layer for all four memory layers.

**The new model:** BrightMatter keeps the live API **and** adds a cron worker. Two modes serve different needs:

1. **API (live)** — Synchronous `get_guidance` calls. Deterministic lookups against memory, so latency is minimal. Mandatory for any platform that needs guidance before execution.
2. **Cron (batch)** — Pulls events from a shared database, processes them through the learning pipeline (episodes → consolidation → pattern updates → guidance refresh), writes results back. Runs on schedule (every 5 min or on change triggers).
3. All platforms **push events to a shared database** (Supabase PostgreSQL)
4. BrightMatter **pulls from that database** via cron, learns, and updates its memory
5. Platforms **call the API** for guidance before execution, or read from `guidance_cache` in Supabase

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│    MH-OS     │   │   mh1-hq     │   │  mh1-skills  │
│  (marketing  │   │  (execution  │   │  (agent-team │
│   operator)  │   │   engine)    │   │   skills)    │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       │  PUSH events     │  PUSH events     │  PUSH events
       │  to Supabase     │  to Supabase     │  to Supabase
       │                  │                  │
       │  GET guidance    │  GET guidance    │  GET guidance
       │  via API         │  via API         │  via API
       ▼                  ▼                  ▼
┌──────────────────────────────────────────────────────┐
│              BrightMatter                            │
│                                                      │
│  ┌─────────────────────────────────────────────┐     │
│  │  API (FastAPI, always running)               │     │
│  │  GET  /guidance/{skill_name}?client_id=X     │     │
│  │  POST /tracking/start                        │     │
│  │  POST /tracking/complete                     │     │
│  │  POST /episodes/write                        │     │
│  │  Deterministic — reads memory, returns fast  │     │
│  └─────────────────────────────────────────────┘     │
│                                                      │
│  ┌─────────────────────────────────────────────┐     │
│  │  Cron Worker (every 5 min)                   │     │
│  │  1. Pull new events from Supabase            │     │
│  │  2. Convert to episodes + outcomes           │     │
│  │  3. Run consolidation (episodic → semantic)  │     │
│  │  4. Refresh guidance_cache in Supabase       │     │
│  │  5. Check for improvement proposals          │     │
│  └─────────────────────────────────────────────┘     │
│                                                      │
│  IntelligenceEngine (4-layer memory, learning,       │
│  scoring, shadow testing, improvement)               │
│                                                      │
│  Firebase = internal working memory                  │
│  Supabase = shared event bus + guidance cache        │
└──────────────────────────────────────────────────────┘
```

**Why API + Cron:**
- `get_guidance` is deterministic (memory lookup) — sub-100ms latency, no reason to make it async
- Platforms need guidance **before** execution, not after a 5-minute batch cycle
- Cron handles the heavy work: consolidation, pattern learning, drift detection, shadow testing
- API stays thin — reads from memory, doesn't do learning inline
- Any new platform just writes events to Supabase and calls the API for guidance
- Firebase stays as BrightMatter's internal working memory; Supabase is the shared event bus

---

## What Each Platform Does Today

### MH-OS (Growth Operating System)

**Produces intelligence data. Has zero learning.**

| What exists | Where | Format |
|-------------|-------|--------|
| Signal persistence | `src/trigger/shared/signals.ts` | Writes to local MD + Supabase `signals` table |
| Recommendation feedback | `src/trigger/shared/feedback.ts` | Writes to Supabase `recommendation_feedback` table |
| Recommendations | Airtable `Recommendations` table | Human approve/deny via Slack |
| Build logs | `20_intelligence/build-log/` | Markdown session summaries |
| Signal log | `20_intelligence/signals/signal-log.jsonl` | Append-only JSONL index |
| BigQuery reads | `src/trigger/shared/bq-client.ts` | Read-only (spend, funnel, deals, revenue) |

**Signal persistence is widespread:** `persistSignal()` is called by 20 trigger task files. `writeRecommendation()` is called by 17 trigger task files. The data is flowing — it just doesn't feed a learning system.

**What's missing:** No outcome tracking. No learning loop. `writeFeedback()` and `updateFeedbackOutcome()` exist in `feedback.ts` but are never called from MH-OS itself — they're designed for a Slack interaction webhook that hasn't been built. `getFeedbackContext()` is the one function that IS used (by `channel-advisor`) to inject past approve/deny patterns into Claude prompts. The `closed-loop-signals.md` plan describes future work but nothing beyond that single function is implemented.

**Signal contract (`signals.ts`):**
```typescript
interface Signal {
  date: string;           // "2026-03-08"
  cadence: "daily" | "weekly" | "monthly" | "quarterly";
  source: string;         // "daily-pulse", "google-ads-weekly"
  lever: string;          // "Lead Volume", "Retention"
  summary: string;        // 1-2 sentence headline
  body: string;           // full markdown
  metrics?: Record<string, number>;  // {spend: 1234, ff: 45}
}
```

**Feedback contract (`feedback.ts`):**
```typescript
interface RecommendationFeedback {
  recommendation_id: string;    // Airtable record ID
  source: string;               // which trigger task
  type: string;                 // recommendation type
  summary: string;
  action_decision?: "approved" | "denied";
  eval_sentiment?: "agree" | "disagree";
  user_context?: string;        // free text from human
  decided_by?: string;          // Slack user ID
  outcome?: string;             // post-action result (never populated today)
}
```

---

### mh1-hq (Execution Engine)

**Has a full embedded copy of BrightMatter's code, but it is not actively running.** The 36-file intelligence system exists in `lib/intelligence/` and the `IntelligenceBridge` facade is wired into `engine.py`, but the bridge is optional — wrapped in `try/except`, and if it fails or `_intelligence_bridge` is None, execution continues without guidance. In practice, the intelligence system has never been turned on for production runs. This is the legacy code to migrate and strip.

| What exists | Where | Status |
|-------------|-------|--------|
| IntelligenceEngine | `lib/intelligence/__init__.py` | Code present, not running in prod |
| IntelligenceBridge | `lib/intelligence_bridge.py` | Wired but optional — fails silently |
| Domain adapters | `lib/intelligence/adapters/` | Content, Revenue, Health, Campaign scoring |
| Memory layers | `lib/intelligence/memory/` | Working, Episodic, Semantic, Procedural |
| Learning | `lib/intelligence/learning/` | Predictor, Learner, Shadow, Accuracy, Gold Standard |
| Outcomes | `lib/intelligence/outcomes/` | Pending store, Checkpoint processor, Signal computer, Behavior collector, Three-gate |
| Improvement | `lib/intelligence/improvement/` | Analyzer, Proposer, Executor, Reviewer |
| Knowledge cards | `knowledge_store/` | **Active** — expert knowledge injected into sandboxes |
| Telemetry | `lib/execution/telemetry/writer.py` | **Active** — JSONL events per run |
| Firebase | `lib/firebase_client.py` | All memory paths at `system/intelligence/*` |

**How the engine's intelligence integration is wired (currently inactive):**

The engine calls guidance on **some** nodes, not all. It skips: join nodes, Phase-0-gate-skipped nodes, unsupported executor nodes, and any case where `_intelligence_bridge` is None or throws.

```python
# Before skill execution (engine.py ~1051) — wrapped in try/except
if self._intelligence_bridge:
    try:
        guidance = self._intelligence_bridge.get_skill_guidance(
            skill_name=skill_id, client_id=...,
            inputs=enriched_inputs, phase0_metrics=phase0_metrics
        )
        tracking_id = self._intelligence_bridge.start_tracking(...)
        # Guidance injected into sandbox context
        context.guidance = guidance.to_dict()
    except Exception as e:
        print(f"Intelligence guidance failed for {node_id}: {e}")
        # Execution continues without guidance

# After skill execution (engine.py ~1782) — runs on BOTH success and failure
if self._intelligence_bridge and node_id in self._tracking_ids:
    try:
        deferred = False  # default is False, not True
        if result.success:
            deferred = self._register_pending_outcome(...)  # may set True
        self._intelligence_bridge.complete_tracking(
            tracking_id=tracking_id, result=result.outputs or {},
            metrics=result.metrics or {},
            goal_completed=result.success,  # True or False
            business_impact=impact,         # validation_score or 1.0/0.0
            deferred=deferred,
        )
    except Exception as e:
        print(f"Intelligence tracking completion failed for {node_id}: {e}")
```

**66 skills mapped to 5 domains** in `SKILL_DOMAINS` dict (code comment says "64" but actual count is 66). Domains: content (24), revenue (14), generic (14), campaign (11), health (3).

---

### mh1-skills (Agent-Team Skills)

**Has 86 skills across 11 categories. Zero intelligence or learning code.**

| What exists | Where | Format |
|-------------|-------|--------|
| Skill library | `skills/` (11 categories, 86 SKILL.md files) | SKILL.md instruction files |
| Client data | `clients/{slug}/` + Firestore (`mh1-skills` collection) | JSON + artifacts |
| PostHog instrumentation | `.claude/settings.json` hooks | 10 event types (see below) |
| Plan execution | `clients/{slug}/plans/` | Markdown plans, parallel agent teams |

**No intelligence persistence.** Client data syncs to Firestore via `bin/clients-pull`/`bin/clients-push`. PostHog captures behavior events to the cloud. No outcome tracking, no feedback loop, no learning. Skills execute, produce artifacts, and forget.

**PostHog's purpose — monitoring expert marketers:**

The PostHog instrumentation is not generic telemetry. mh1-skills is used by the world's best marketers. PostHog captures their decision-making logic:

| Hook | What it captures | Learning signal |
|------|-----------------|-----------------|
| `session_start` / `session_end` | Session timing and context | How long experts spend on different task types |
| `user_prompt` | What the marketer asks for | What strategies they request vs what they generate themselves |
| `tool_use` | Which tools and skills they invoke | Which skills experts reach for — their instinct for what works |
| `agent_stop` | Why execution ended (completed, error, token limit) | Failure modes and where skills break down |
| `subagent_start` / `subagent_stop` | Parallel skill orchestration | How experts compose multi-skill plans |
| `task_completed` | Final outcome of a skill execution | Success rates by skill, client, and context |
| `skill_invoked` (derived) | Triggered when `tool_use` fires on a SKILL.md path | Exact skill usage patterns |

**The key insight:** When an expert marketer overrides a generated strategy with their own, that's a high-value learning signal. When they accept a generated strategy, that's validation. When they modify it, the delta is the lesson. PostHog captures all of this — it just doesn't flow to BrightMatter yet.

---

## How MH-OS Uses BrightMatter

MH-OS is a signal factory. It already writes to Supabase. The integration is about closing the loop — turning signals into learning.

### What MH-OS pushes to BrightMatter

| MH-OS source | → BrightMatter table | What BrightMatter learns |
|--------------|---------------------|--------------------------|
| `persistSignal()` → Supabase `signals` | `events` | What happened (metrics, anomalies, trends). Each signal becomes an episode keyed by `{date}_{source}` |
| `writeFeedback()` → Supabase `recommendation_feedback` | `outcomes` | Did the human approve or deny? What was the sentiment? This is BrightMatter's client feedback signal |
| `updateFeedbackOutcome()` → Supabase `recommendation_feedback.outcome` | `outcomes` | Post-action result. Did the approved recommendation actually work? This closes the loop |
| Build logs → `20_intelligence/build-log/` | `events` | What Claude built in each session. Session-level episodes |
| Expert panel scores → `/evaluate` output | `outcomes` | Quality scores from 8 expert dimensions |

### What MH-OS reads from BrightMatter

| BrightMatter output | → MH-OS consumer | How it's used |
|---------------------|-------------------|---------------|
| `guidance_cache` (per trigger task) | `src/trigger/` tasks | Before generating a recommendation, the trigger task reads BrightMatter's guidance: "Last 5 times you recommended shifting budget from Meta to Google, 3 were approved, 2 denied with context 'timing wrong'. Adjust threshold." |
| `patterns` (per channel/lever) | `/start` and `/brief` commands | Triage scoring informed by learned patterns: "Google Ads CPA spikes on Mondays are usually false alarms (resolved 4/5 times without action)" |
| `improvement_proposals` | CLAUDE.md routing | BrightMatter detects that `daily-pulse` consistently generates low-quality signals on weekends → proposes skipping Saturday triggers |

### Example flow

```
Monday 8am:
  weekly-growth-report trigger fires
    → queries BigQuery (spend, funnel, deals)
    → reads guidance_cache for "weekly-growth-report"
      → BrightMatter says: "Last 3 weeks, 'scale LinkedIn' was denied.
         Human context: 'not until Q2 budget approved.'
         Suppress LinkedIn scaling recommendations until April."
    → generates report WITHOUT the LinkedIn recommendation
    → posts to Slack
    → persistSignal() → Supabase signals table
    → writeRecommendation() → Airtable

Human approves "shift $5K from Meta to Google search":
    → writeFeedback({action_decision: "approved"}) → Supabase
    → BrightMatter pulls this, records as positive outcome
    → Semantic memory updates: "Meta→Google shifts: 4/5 approved"
    → Next week's guidance_cache for this trigger reflects the pattern
```

### Implementation: what changes in MH-OS

1. **Wire `writeFeedback()`** — the Slack interaction webhook (currently external) needs to call this. Or build it as a Trigger.dev task that watches Airtable status changes.

2. **Wire `updateFeedbackOutcome()`** — after an approved recommendation is acted on, measure the result. Example: "We shifted $5K to Google. Did CPA improve?" Check the next daily-pulse for the answer.

3. **Add guidance reads** — each trigger task calls BrightMatter's API before generating recommendations. New shared util:
```typescript
// src/trigger/shared/guidance.ts
export async function getGuidance(
  source: string,
  clientId: string = "marketerhire"
): Promise<GuidanceBlock> {
  const bmUrl = process.env.BRIGHTMATTER_URL || "http://localhost:8100";
  try {
    const res = await fetch(
      `${bmUrl}/api/v1/guidance/${source}?tenant_id=${clientId}&domain=campaign`,
      { headers: { "X-API-Key": process.env.BRIGHTMATTER_API_KEY || "" } }
    );
    if (!res.ok) return DEFAULT_GUIDANCE;
    return await res.json();
  } catch {
    return DEFAULT_GUIDANCE; // graceful degradation — no guidance is fine
  }
}
```

4. **Signal-to-episode adapter** — BrightMatter needs a lightweight adapter to convert MH-OS signals into its episode format:
```python
def signal_to_episode(signal_row: dict) -> EpisodicMemory:
    return EpisodicMemory(
        skill_name=signal_row["source"],       # "daily-pulse"
        tenant_id="marketerhire",
        domain=_lever_to_domain(signal_row["lever"]),
        context={"metrics": signal_row["metrics"], "cadence": signal_row["cadence"]},
        outcome={"summary": signal_row["summary"]},
        signal_value=_extract_primary_metric(signal_row["metrics"]),
        source=EpisodeSource.MARKET_OBSERVATION,
    )
```

---

## How mh1-hq Uses BrightMatter

mh1-hq has the full intelligence system's code embedded in `lib/intelligence/`, but it is **not actively running**. The `IntelligenceBridge` is wired into `engine.py` but is optional — if it fails or isn't configured, execution proceeds without guidance. The migration is about replacing the dead embedded code with a thin client that calls BrightMatter's live API.

### What's embedded today (to remove)

```
lib/intelligence/          ← DELETE (move to brightmatter repo)
├── __init__.py            ← IntelligenceEngine
├── types.py               ← Domain, Prediction, Outcome, etc.
├── adapters/              ← Scoring
├── memory/                ← 4 layers
├── learning/              ← Predictor, Learner, Shadow
├── improvement/           ← Analyzer, Proposer, Executor, Review
├── outcomes/              ← Pending store, Checkpoint, Signal computer
└── jarvis_episodes.py     ← Jarvis converter

lib/intelligence_bridge.py ← REWRITE (thin client → Supabase reads/writes)
```

### What stays in mh1-hq

```
lib/execution/knowledge/   ← KEEP (I3 expert knowledge cards)
knowledge_store/           ← KEEP (curated domain knowledge)
lib/execution/telemetry/   ← KEEP (JSONL run telemetry)
```

I3 (expert knowledge) is fundamentally different from I1/I2 (learning). Knowledge cards are curated, version-controlled documents — they belong in the execution repo. Learning and memory belong in BrightMatter.

### What mh1-hq pushes to BrightMatter

| mh1-hq source | → BrightMatter table | What BrightMatter learns |
|----------------|---------------------|--------------------------|
| Skill execution result (`ExecutionResult`) | `events` | Signal value, goal completion, business impact, token usage, duration |
| Node validation scores | `outcomes` | Quality scores per deliverable |
| Phase leader QA results | `outcomes` | Phase-level quality assessment |
| Telemetry events (JSONL) | `events` | Timing, retries, errors, token counts |
| Deferred outcome checkpoints | `outcomes` | 24h/7d post-execution measurements |

### What mh1-hq reads from BrightMatter

| BrightMatter output | → mh1-hq consumer | How it's used |
|---------------------|-------------------|---------------|
| `get_guidance` API call (per skill + client) | `engine.py` before execution | Injected into sandbox `context_slice.json` as `context.guidance` — same interface as today, just backed by BrightMatter API instead of local engine |
| `patterns` (per domain) | Plan compiler | "lifecycle-audit has 92% success rate with agent lifecycle-auditor but 71% with generic. Prefer lifecycle-auditor." |
| `improvement_proposals` | Operator review | "email-copy-generator produces empty outputs 40% of the time. Propose adding explicit write_output mandate to agent_instructions." |

### Rewritten IntelligenceBridge (thin client)

The current `IntelligenceBridge` orchestrates the full engine locally. The new version calls BrightMatter's API for guidance (synchronous, deterministic) and writes events to Supabase (async, fire-and-forget):

```python
class IntelligenceBridge:
    """Thin client — calls BrightMatter API for guidance, writes events to Supabase."""

    def __init__(self):
        self._bm_url = os.environ.get("BRIGHTMATTER_URL", "http://localhost:8100")
        self._bm_key = os.environ.get("BRIGHTMATTER_API_KEY", "")
        self._supabase = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_SERVICE_KEY"]
        )

    def get_skill_guidance(self, skill_name, client_id, inputs=None, phase0_metrics=None):
        """Call BrightMatter API — deterministic memory lookup, sub-100ms."""
        try:
            resp = requests.get(
                f"{self._bm_url}/api/v1/guidance/{skill_name}",
                params={"tenant_id": client_id, "domain": self._get_domain(skill_name)},
                headers={"X-API-Key": self._bm_key},
                timeout=2,
            )
            if resp.ok:
                return SkillGuidance.from_dict(resp.json().get("guidance", {}))
        except Exception:
            pass
        return SkillGuidance()  # graceful degradation — exploration mode

    def start_tracking(self, skill_name, client_id, guidance, **kwargs):
        """Call BrightMatter API to register prediction."""
        try:
            resp = requests.post(
                f"{self._bm_url}/api/v1/tracking/start",
                json={"skill_name": skill_name, "client_id": client_id,
                       "guidance": guidance.to_dict(), **kwargs},
                headers={"X-API-Key": self._bm_key},
                timeout=2,
            )
            if resp.ok:
                return resp.json()["tracking_id"]
        except Exception:
            pass
        return None

    def complete_tracking(self, tracking_id, result, metrics, **kwargs):
        """Write execution outcome to Supabase (fire-and-forget)."""
        if not tracking_id:
            return LearningResult()
        try:
            self._supabase.table("events").insert({
                "source": "mh1-hq",
                "tracking_id": tracking_id,
                "event_type": "skill_completed",
                "client_id": kwargs.get("client_id", ""),
                "result": result,
                "metrics": metrics,
                "goal_completed": kwargs.get("goal_completed"),
                "business_impact": kwargs.get("business_impact", 0.0),
            }).execute()
        except Exception as e:
            print(f"Event write failed: {e}")
        return LearningResult()
```

### Migration steps

1. **Verify parity** — BrightMatter standalone passes all the same tests as the embedded copy
2. **Add Supabase tables** — `events`, `predictions`, `outcomes`, `guidance_cache`, `patterns`, `episodes`
3. **Rewrite `lib/intelligence_bridge.py`** — thin client (above)
4. **Delete `lib/intelligence/`** — the 36-file embedded engine
5. **Keep `lib/execution/knowledge/`** — I3 stays in mh1-hq
6. **BrightMatter worker** — cron job that pulls from Supabase, processes through IntelligenceEngine, writes guidance/patterns back
7. **Test** — run a full plan execution, verify guidance is injected correctly from Supabase

---

## How mh1-skills Uses BrightMatter

mh1-skills has zero intelligence code today. Skills execute, produce artifacts, and forget. The integration adds learning where it matters — specifically for skills that generate strategy, email, content, or anything requiring domain knowledge.

### Where guidance matters in mh1-skills

Not every skill needs BrightMatter. Data extraction and discovery skills are deterministic — they don't benefit from learned patterns. But strategy, content, and email generation skills absolutely do.

| Skill category | Needs `get_guidance`? | Why |
|---------------|----------------------|-----|
| `strategy-skills/` (17) | **Yes** — positioning, GTM, email sequences, experiments | Strategy recommendations should reflect what's worked for this client and similar clients |
| `generation-skills/` (12) | **Yes** — email copy, content, SEO, creative briefs | Content quality improves when BrightMatter knows which tones, angles, and formats have succeeded |
| `extraction-skills/` (13) | **Selective** — brand voice, audience persona when building (not extracting) | When building personas or voice profiles from scratch, prior patterns help |
| `analysis-skills/` (18) | **No** — lifecycle audit, pipeline analysis, data quality | These are measurement skills — they should report objectively, not be influenced by predictions |
| `discovery-skills/` (8) | **No** — CRM, warehouse, platform discovery | Deterministic data retrieval |
| `search-skills/` (7) | **No** — company, competitor, founder research | Factual retrieval |
| `conversion-skills/` (7) | **No** — funnel analysis, deal velocity | Measurement |
| `operations-skills/` (16) | **Selective** — onboarding, report writing benefit from patterns | Onboarding sequencing should learn from prior onboarding outcomes |

### What mh1-skills pushes to BrightMatter

| mh1-skills source | → BrightMatter table | What BrightMatter learns |
|--------------------|---------------------|--------------------------|
| PostHog `task_completed` events | `events` | Which skills ran, for which client, duration, success/failure |
| PostHog `agent_stop` events | `events` | Why agents stopped (completed, error, token limit) |
| PostHog `user_prompt` events | `events` | **What expert marketers ask for** — their strategic instincts, what they request vs what they accept from generation |
| PostHog `skill_invoked` events | `events` | Which skills experts reach for first — their mental model of what works |
| Plan outcomes (`clients/{slug}/plans/`) | `outcomes` | Phase gate results — did the plan produce usable artifacts? |
| Expert overrides | `outcomes` (type: `expert_override`) | **When a marketer rejects a generated strategy and writes their own** — the delta between generated and human is the highest-value learning signal |
| Client feedback (manual) | `outcomes` | If a client says "this audit was great" or "this was off" |

### What mh1-skills reads from BrightMatter

| BrightMatter output | → mh1-skills consumer | How it's used |
|---------------------|----------------------|---------------|
| `guidance_cache` (per skill + client) | Strategy/generation skills only | "For this client, email sequences with 3 touches outperform 5 touches. Keep sequences short." |
| `patterns` (per skill category) | Plan generation (CLAUDE.md step 3) | "lifecycle-audit works better when you run research-company first. Add research-company to Phase 1." |
| `patterns` (cross-client) | Skill selection | "For clients with HubSpot + Snowflake, crm-discovery has 95% success rate. data-warehouse-discovery needs credentials that 60% of clients don't have — prompt for credentials before including." |
| `improvement_proposals` | Skill updates | "extract-founder-voice produces better results with 3+ LinkedIn posts as input. Minimum input threshold should be documented in SKILL.md." |

### Implementation: what changes in mh1-skills

1. **PostHog → Supabase bridge** — PostHog webhook or batch export that writes to the shared `events` table. Filter to meaningful events only: `task_completed`, `agent_stop`, `user_prompt`, `skill_invoked`. Drop `tool_use` (too noisy — every grep, file read, etc.) and `notification` / `pre_compact` (system noise).

2. **Expert override capture** — when a marketer rejects a generated strategy and provides their own, write an `expert_override` outcome. This is the highest-value signal — it tells BrightMatter what human experts would have done differently.

3. **Guidance injection into strategy/generation skills** — skills in `strategy-skills/` and `generation-skills/` call `get_guidance` from the BrightMatter API before execution:
```python
# In skill execution context (injected by plan runner)
guidance = requests.get(
    f"{BRIGHTMATTER_URL}/api/v1/guidance/{skill_name}",
    params={"client_id": client_id, "domain": "content"}
).json()

# Guidance injected as context for the skill agent
# e.g. "For this client, short-form emails (50-80 words) have 2x open rate vs long-form"
```

4. **Outcome reporting** — after plan completion, write a summary event:
```bash
# New bin script: bin/report-outcome
curl -X POST $SUPABASE_URL/rest/v1/events \
  -H "apikey: $SUPABASE_SERVICE_KEY" \
  -d '{
    "source": "mh1-skills",
    "skill_name": "lifecycle-audit",
    "client_id": "acme-corp",
    "event_type": "plan_completed",
    "result": {"phases_completed": 3, "artifacts": 7},
    "metrics": {"duration_s": 340, "tokens": 45000}
  }'
```

---

## The Shared Database Schema

All three platforms write to the same Supabase tables. BrightMatter is the only system that processes them.

### Tables

### Client-Keyed Memory

All memory is tagged by `client_id`. This is non-negotiable — different clients have different baselines, channel maturity, and activity levels. A pattern learned for MarketerHire (mature ad accounts, high email volume) doesn't apply to a startup with zero ad history.

**Client attributes that affect confidence and predictions:**

| Attribute | Why it matters | Effect on BrightMatter |
|-----------|---------------|----------------------|
| **Channel age** (ads, social, email) | A 3-year Google Ads account has stable baselines; a 1-month account has volatile ones | Confidence scales with channel age — younger channels get wider confidence intervals and more exploration |
| **Dormancy** (active vs inactive) | A client who hasn't run lifecycle emails in 6 months has decayed patterns | Dormant channels reset prediction baselines toward priors; active channels use learned baselines |
| **Account maturity** | Early-stage clients need discovery; mature clients need optimization | `is_exploration` is more likely for new clients; exploit mode for mature ones |
| **Historical signal volume** | 100 prior executions vs 3 | Confidence is a function of sample size — Bayesian priors dominate with few observations |

BrightMatter's domain adapters (`channels.py`) already model channel timing and baselines. The `ChannelConfig` / `CHANNEL_REGISTRY` system defines per-channel `optimal_timing`, `min_interval`, and `historical_window`. Client-level overrides extend this — a client whose email channel has been dormant for 90 days gets a different baseline than one sending 3x/week.

### Tables

```sql
-- Raw events from all platforms
CREATE TABLE events (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  source TEXT NOT NULL,           -- "mh-os", "mh1-hq", "mh1-skills"
  event_type TEXT NOT NULL,       -- "signal", "skill_completed", "plan_completed"
  skill_name TEXT,                -- "daily-pulse", "lifecycle-audit"
  client_id TEXT NOT NULL,        -- "marketerhire", "acme-corp" — always required
  domain TEXT DEFAULT 'generic',  -- "revenue", "content", "health", "campaign"
  result JSONB,                   -- execution result data
  metrics JSONB,                  -- {spend: 1234, tokens: 5000, duration_s: 120}
  context JSONB,                  -- input context at execution time
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_events_client ON events(client_id, created_at DESC);
CREATE INDEX idx_events_skill ON events(skill_name, client_id);

-- Predictions registered before execution
CREATE TABLE predictions (
  tracking_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  client_id TEXT NOT NULL,
  domain TEXT DEFAULT 'generic',
  expected_signal FLOAT,
  expected_baseline FLOAT,
  confidence FLOAT DEFAULT 0.5,
  guidance JSONB,                 -- guidance that was active at prediction time
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Measured outcomes (signals, feedback, quality scores)
CREATE TABLE outcomes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tracking_id TEXT REFERENCES predictions(tracking_id),
  source TEXT NOT NULL,
  client_id TEXT NOT NULL,
  outcome_type TEXT NOT NULL,     -- "signal_measurement", "human_feedback", "quality_score", "expert_override"
  observed_signal FLOAT,
  goal_completed BOOLEAN,
  business_impact FLOAT,
  feedback JSONB,                 -- {action_decision, eval_sentiment, user_context}
  measured_at TIMESTAMPTZ DEFAULT now()
);

-- BrightMatter writes these — all platforms read
-- Keyed by (skill_name, client_id) — different clients get different guidance
CREATE TABLE guidance_cache (
  skill_name TEXT NOT NULL,
  client_id TEXT NOT NULL,        -- "*" for global defaults, specific ID for client-specific
  domain TEXT DEFAULT 'generic',
  parameters JSONB,               -- recommended parameters
  confidence FLOAT DEFAULT 0.5,
  expected_value FLOAT,
  is_exploration BOOLEAN DEFAULT true,
  patterns_used TEXT[],           -- IDs of semantic patterns
  predicted_outcome FLOAT,
  channel_context JSONB,          -- {channel_age_days, dormancy_days, last_active, signal_count}
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (skill_name, client_id)
);

-- BrightMatter writes these — queryable learned patterns
CREATE TABLE patterns (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  domain TEXT NOT NULL,
  skill_name TEXT,
  client_id TEXT,                 -- NULL for cross-client patterns
  pattern_type TEXT,              -- "semantic", "procedural"
  description TEXT,
  confidence FLOAT,
  successes INT DEFAULT 0,
  failures INT DEFAULT 0,
  parameters JSONB,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Existing MH-OS tables (already in Supabase)
-- signals                        — kept as-is, BrightMatter reads
-- recommendation_feedback        — kept as-is, BrightMatter reads
-- transcripts                    — kept as-is, not BrightMatter-relevant
-- lead_enrichments               — kept as-is, not BrightMatter-relevant
```

---

## What Already Exists That Routes to a Central DB

### Already routing (ready today)

| System | What | Where | BrightMatter can pull? |
|--------|------|-------|----------------------|
| MH-OS | Signals (20 of 26 trigger tasks call `persistSignal()`) | Supabase `signals` | Yes — each signal is an episode |
| MH-OS | Recommendation feedback | Supabase `recommendation_feedback` | Yes — approve/deny is outcome data |
| MH-OS | Signal log | Local `signal-log.jsonl` | Needs Supabase sync (or BM reads JSONL) |
| mh1-hq | Firebase intelligence data | Firebase `system/intelligence/*` | Yes — but Firebase, not Supabase |
| mh1-hq | Telemetry | Local JSONL `{run_dir}/telemetry/` | Needs Supabase push |
| mh1-skills | PostHog events | PostHog cloud | Needs PostHog → Supabase bridge |

### Not routing (needs new code)

| System | What's missing | Effort |
|--------|---------------|--------|
| MH-OS | `writeFeedback()` is never called (Slack webhook not wired) | Small — build Trigger.dev task watching Airtable status |
| MH-OS | `updateFeedbackOutcome()` is never called | Medium — need "did the action work?" measurement logic |
| MH-OS | Build logs not persisted to Supabase | Small — add Supabase write to `session-build-log.sh` hook |
| mh1-hq | Execution results stay in local JSONL | Medium — add Supabase write to `TelemetryWriter` |
| mh1-hq | Firebase memory → Supabase migration | Large — migrate `system/intelligence/*` to Supabase tables |
| mh1-skills | No persistence at all beyond Firestore client data | Medium — PostHog webhook + outcome reporting script |
| mh1-skills | Plan outcomes not captured | Small — `bin/report-outcome` script |

### The gap: Firebase vs Supabase

mh1-hq uses Firebase for intelligence. MH-OS uses Supabase for signals. This is the central tension.

**Option A: Supabase as the universal bus.** BrightMatter migrates its internal persistence from Firebase to Supabase (or reads from both). Supabase is SQL — easier to query, join, and aggregate across platforms.

**Option B: Firebase as the universal bus.** MH-OS adds Firebase writes alongside Supabase. More infrastructure to maintain, but no migration needed for mh1-hq.

**Option C (recommended): Supabase is the shared bus, Firebase is BrightMatter's working memory.** BrightMatter keeps Firebase for its internal 4-layer memory system (fast, document-oriented, already works). But the input/output contract with other platforms is Supabase. BrightMatter pulls events/outcomes from Supabase → processes through its engine → writes guidance/patterns to Supabase. Internally it still uses Firebase for episodic decay, semantic pattern storage, and shadow testing.

```
MH-OS ──────► Supabase (signals, feedback) ──┐
mh1-hq ─────► Supabase (events, outcomes)  ──┤
mh1-skills ──► Supabase (events, outcomes)  ──┤
                                              │
                                    ┌─────────▼─────────┐
                                    │   BrightMatter     │
                                    │                    │
                                    │  Supabase ←→ pull  │
                                    │  Firebase ←→ think │
                                    │  Supabase ←→ push  │
                                    └────────────────────┘
```

---

## BrightMatter's Runtime: API + Cron Worker

BrightMatter runs two processes:

### 1. API (always running)

The existing FastAPI server stays. It handles synchronous requests — `get_guidance` is a memory lookup, not a computation, so latency is sub-100ms.

```bash
# Same as today
uvicorn api:app --host 0.0.0.0 --port 8100
```

All existing endpoints remain. The API reads from the IntelligenceEngine's memory (backed by Firebase). Platforms call it before execution to get guidance. This is mandatory and deterministic.

### 2. Cron Worker (new)

The worker handles the heavy learning pipeline — pulling events from Supabase, converting them to episodes/outcomes, running consolidation, and refreshing the guidance cache.

```python
# brightmatter/worker.py

class BrightMatterWorker:
    """Pull events from Supabase, process through learning pipeline, update guidance."""

    def __init__(self):
        self.engine = IntelligenceEngine()  # Firebase for internal memory
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    def run_cycle(self):
        """One processing cycle. Called by cron (every 5 min) or change trigger."""

        # 1. Pull new events since last checkpoint
        new_events = self._pull_new_events()

        # 2. Convert to episodes and outcomes, keyed by client_id
        for event in new_events:
            client_id = event["client_id"]

            if event["event_type"] == "signal":
                episode = signal_to_episode(event)
                self.engine.episodic.store(episode)

            elif event["event_type"] == "skill_completed":
                self._process_skill_completion(event)

            elif event["event_type"] == "human_feedback":
                self._process_feedback(event)

            elif event["event_type"] == "expert_override":
                # Highest-value signal: expert marketer overrode generated output
                self._process_expert_override(event)

        # 3. Run consolidation (episodic → semantic → procedural)
        self.engine.run_consolidation()

        # 4. Update guidance cache per (skill, client) in Supabase
        self._refresh_guidance_cache()

        # 5. Check for improvement opportunities
        proposals = self._check_improvements()
        if proposals:
            self._write_proposals(proposals)

    def _refresh_guidance_cache(self):
        """Recompute guidance for every (skill, client) pair and write to Supabase."""
        for skill_name, client_id in self._get_active_skill_client_pairs():
            guidance = self.engine.get_guidance(
                skill_name, tenant_id=client_id
            )
            self.supabase.table("guidance_cache").upsert({
                "skill_name": skill_name,
                "client_id": client_id,
                "parameters": guidance.parameters,
                "confidence": guidance.confidence,
                "expected_value": guidance.expected_value,
                "is_exploration": guidance.is_exploration,
                "patterns_used": guidance.patterns_used,
                "predicted_outcome": guidance.predicted_outcome,
                "channel_context": self._get_channel_context(client_id, skill_name),
                "updated_at": datetime.utcnow().isoformat(),
            }).execute()

    def _get_channel_context(self, client_id, skill_name):
        """Compute client channel maturity for confidence adjustment."""
        domain = self.engine._bridge.SKILL_DOMAINS.get(skill_name, "generic")
        events = self.supabase.table("events") \
            .select("created_at") \
            .eq("client_id", client_id) \
            .eq("domain", domain) \
            .order("created_at", desc=True) \
            .limit(1).execute()
        last_active = events.data[0]["created_at"] if events.data else None
        count = self.supabase.table("events") \
            .select("id", count="exact") \
            .eq("client_id", client_id) \
            .eq("skill_name", skill_name).execute()
        return {
            "last_active": last_active,
            "signal_count": count.count or 0,
            "dormancy_days": _days_since(last_active) if last_active else None,
        }
```

**Scheduling options:**
- **Cron (every 5 min)** — simple, good enough for most cases
- **Supabase Realtime** — subscribe to `events` table inserts, process immediately
- **Modal scheduled function** — runs in the cloud, same infra as mh1-hq execution

**How API and Cron interact:** The cron worker updates the IntelligenceEngine's memory (Firebase). The API reads from the same memory. Since the engine is initialized once and both processes share the same Firebase project, guidance returned by the API automatically reflects the latest cron processing. No cache invalidation needed — Firebase is the single source of truth for memory, Supabase is the shared event bus.

---

## Migration Sequence

### Phase 1: Foundation (no behavior change)

1. Create Supabase tables (`events`, `predictions`, `outcomes`, `guidance_cache`, `patterns`)
2. BrightMatter worker reads existing `signals` and `recommendation_feedback` tables
3. BrightMatter writes to `guidance_cache` and `patterns`
4. No platform changes — everything still works as before

### Phase 2: MH-OS integration

5. Wire `writeFeedback()` — Airtable status change → Supabase feedback
6. Add `getGuidance()` to trigger tasks — read `guidance_cache` before generating recommendations
7. MH-OS signals now inform BrightMatter's learning, and BrightMatter's guidance informs MH-OS recommendations

### Phase 3: mh1-hq migration

8. Rewrite `IntelligenceBridge` as thin client — API calls for guidance, Supabase writes for events
9. `engine.py` calls BrightMatter API for guidance instead of local engine (same `get_skill_guidance` interface, different backend)
10. `complete_tracking()` writes to Supabase `events` table instead of Firebase
11. Delete `lib/intelligence/` from mh1-hq (36 files) — this code is not running anyway
12. Keep `knowledge_store/` and `lib/execution/knowledge/` (I3 is separate)
13. Graceful degradation — if BrightMatter API is unreachable, return default guidance (exploration mode), same as current behavior when bridge fails

### Phase 4: mh1-skills integration

13. PostHog → Supabase event bridge (webhook or Trigger.dev task)
14. `bin/report-outcome` script for manual outcome reporting
15. Plan generation reads `guidance_cache` before selecting skills

### Phase 5: Close the loop

16. `updateFeedbackOutcome()` wired — measure whether approved recommendations worked
17. BrightMatter improvement proposals surface in all three platforms
18. Shadow testing runs across platforms (A/B test skill configurations)
19. Expert override capture in mh1-skills — highest-value learning signal flowing to BrightMatter

---

## What BrightMatter Learns From Each Platform

| Platform | Episode type | Domain signal | Learning value |
|----------|-------------|---------------|----------------|
| **MH-OS** | Market signals (spend, funnel, deals) | Campaign, Revenue | "When daily spend exceeds $X, CPA degrades" |
| **MH-OS** | Human recommendation feedback | All | "Humans approve budget shifts 80% of the time but deny hiring recommendations 90%" |
| **MH-OS** | Expert panel scores | Content | "Landing pages score higher when Cialdini's social proof principle is applied" |
| **mh1-hq** | Skill execution results | All | "lifecycle-audit produces A-grade output with agent lifecycle-auditor, C-grade with generic" |
| **mh1-hq** | Deferred outcomes (24h/7d) | Revenue, Health | "Email sequences generated by email-copy-generator have 23% open rate vs 18% baseline" |
| **mh1-hq** | Phase leader QA | All | "Phase 2 analysis nodes fail validation 30% of the time when Phase 1 data is sparse" |
| **mh1-skills** | Expert marketer overrides | All | **Highest value.** "Senior marketer rejected the generated positioning and wrote their own — the delta reveals what the model doesn't understand about this market" |
| **mh1-skills** | Expert prompt patterns | Strategy, Content | "Top marketers always ask for competitor research before running positioning-angles — add research-competitors as a Phase 1 dependency" |
| **mh1-skills** | Strategy acceptance rates | Strategy | "Generated GTM playbooks are accepted 70% of the time but email sequences only 40% — email generation needs improvement" |
| **mh1-skills** | Plan completion rates | All | "Plans with >5 parallel skills in Phase 1 have 40% failure rate (token limits)" |

### The Expert Marketer Learning Loop

The most valuable signal in the entire system comes from mh1-skills — not from code, but from people. The world's best marketers use mh1-skills daily. PostHog captures their behavior:

1. **What they ask for** (`user_prompt`) — reveals their mental model of what's valuable
2. **What skills they invoke** (`skill_invoked`) — shows their instinct for what tools work
3. **Whether they accept generated output** (`task_completed` + subsequent behavior) — validation signal
4. **When they override generated strategies** (`expert_override`) — the delta between AI output and expert correction is the highest-value training data in the system
5. **What they skip** (sessions without certain skills) — reveals which skills experts don't trust

Over time, BrightMatter builds a cross-platform intelligence graph. A pattern learned from expert marketers in mh1-skills (e.g., "top marketers always research competitors before positioning work") informs mh1-hq plan generation ("auto-add research-competitors to Phase 1 for positioning modules"). A signal from MH-OS ("churn spiked 3%") surfaces in mh1-skills guidance ("prioritize retention-focused strategies for this client"). An execution result from mh1-hq ("lifecycle-audit needs HubSpot credentials") prevents mh1-skills from including it in plans for clients without HubSpot. The learning compounds across all three systems, anchored by client-keyed memory that respects each client's channel maturity, activity level, and history.
