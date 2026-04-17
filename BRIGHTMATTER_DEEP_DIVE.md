# BrightMatter Deep Dive

BrightMatter is a standalone intelligence engine for marketing operations. It watches what happens when skills run, remembers the results, learns what works, and uses that knowledge to give better recommendations next time. It is a self-improving brain that sits alongside the MH1 execution engine.

---

## Repository Structure

```
brightmatter/
├── api.py                          # FastAPI HTTP server
├── pyproject.toml                  # Package config & deps
├── requirements.txt                # Pin file
├── .env.example                    # Config template
├── .gitignore
├── scripts/
│   └── diagnose_consolidation.py   # Debug script
└── lib/
    ├── __init__.py
    ├── client.py                   # HTTP client for callers
    ├── firebase_client.py          # Thread-safe Firebase wrapper
    ├── intelligence_bridge.py      # Simplified runner interface
    ├── remote_bridge.py            # API-backed bridge replacement
    ├── memory_health.py            # Production health checker
    └── intelligence/
        ├── __init__.py             # IntelligenceEngine (main class)
        ├── types.py                # Core dataclasses & enums
        ├── jarvis_episodes.py      # Jarvis episode converter
        ├── adapters/
        │   ├── __init__.py         # Package exports
        │   ├── base.py             # BaseDomainAdapter & ScoringResult
        │   ├── campaign.py         # Campaign efficiency scoring
        │   ├── content.py          # Content & engagement scoring
        │   ├── health.py           # Customer health & churn risk
        │   ├── revenue.py          # Pipeline velocity & deal scoring
        │   └── channels.py         # Channel-to-adapter registry
        ├── memory/
        │   ├── __init__.py         # Package exports
        │   ├── working.py          # In-memory scratch pad (session)
        │   ├── episodic.py         # Firebase diary (prediction+outcome pairs)
        │   ├── semantic.py         # Firebase pattern library (Bayesian)
        │   ├── procedural.py       # Firebase wisdom (cross-skill)
        │   └── consolidation.py    # Working→Episodic→Semantic→Procedural
        ├── learning/
        │   ├── __init__.py         # Package exports
        │   ├── predictor.py        # Explore/exploit parameter advisor
        │   ├── learner.py          # Bayesian updates & drift detection
        │   ├── shadow.py           # A/B testing alternative configs
        │   ├── accuracy.py         # Daily prediction report card
        │   └── gold_standard.py    # Regression test benchmarks
        ├── improvement/
        │   ├── __init__.py         # Package exports
        │   ├── analyzer.py         # Detect systematic failure patterns
        │   ├── proposer.py         # Generate concrete fix proposals
        │   ├── executor.py         # Dispatch approved fixes to queue
        │   └── review.py           # Human review queue management
        └── outcomes/
            ├── __init__.py         # Package exports
            ├── pending_store.py    # Track predictions awaiting results
            ├── checkpoint_processor.py  # Cron: measure 24h/7d outcomes
            ├── delivery_extractor.py    # Find campaign/email IDs in outputs
            ├── signal_computer.py       # Combine platform+feedback+behavior
            ├── behavior_collector.py    # PostHog & Firebase user signals
            └── three_gate.py            # Operator→Client→Market resonance
```

---

## Adapters — Domain-Specific Scoring

**What they do:** Score marketing performance using a universal formula: `Score = (Signal / Baseline) × Context Multiplier`. Each adapter knows what "signal" and "baseline" mean in its specific business domain.

### `adapters/base.py` — BaseDomainAdapter & ScoringResult

The abstract contract all adapters follow. Defines the `ScoringResult` dataclass (score, signal, baseline, context_multiplier, confidence, components, explanation, domain, metadata) and the `score()` orchestration method (aliased as `calculate_score()`). Subclasses implement `get_domain_name()`, `get_signal()`, `get_baseline()`, `get_context_multiplier()`, and `validate_event()`.

**Example:** Every score goes through the same pipeline — extract the raw signal, calculate what "normal" looks like (baseline), apply situational adjustments (multiplier), and wrap it in a ScoringResult with a confidence rating.

### `adapters/campaign.py` — CampaignAdapter

Scores marketing campaign efficiency. Signal = inverse CPA (cost per acquisition). Baseline = channel-specific expected CPA adjusted for seasonality. Multiplier factors in funnel stage, audience quality, attribution model, and campaign maturity.

**Example:** A paid search campaign spends $1,000 and gets 25 conversions ($40 CPA). The expected CPA for paid search is $50. Score = ($50/$40) × 1.2 (decision-stage bonus) = 1.5. That means the campaign is performing 50% above expectations.

### `adapters/content.py` — ContentAdapter

Scores content and engagement across platforms. Signal = impressions weighted by engagement quality. Baseline = expected reach based on audience size, platform norms, and content age (exponential decay model). Multiplier adjusts for content type, platform, and timing.

**Example:** An Instagram carousel gets 5,000 impressions and 245 engagements (4.9% rate). With 20,000 followers and platform avg of 1.5%, the content is outperforming by ~3.5x. Older content automatically scores lower because of the decay model.

### `adapters/health.py` — HealthAdapter

Scores customer health and churn risk using an RFM-inspired approach (Recency, Frequency, Monetary/Satisfaction). Signal = composite of days since last activity, interaction frequency, and NPS score. Baseline = what's expected for their contract tier, customer age, and segment.

**Example:** A professional-tier customer active 3 days ago, with 12 interactions/month and NPS 9/10. For their tier, that's excellent. Score = 1.73 (healthy). An enterprise customer inactive for 60 days with NPS 2/10 scores 0.08 (high churn risk).

### `adapters/revenue.py` — RevenueAdapter

Scores pipeline velocity and deal progression. Signal = how fast deals move between stages vs benchmarks. Baseline = segment-specific benchmarks (SMB/mid-market/enterprise) and historical win rates.

**Example:** A mid-market deal moves from SQL to Opportunity in 10 days (benchmark is 21). Plus strong engagement signals. Score = 3.34 (deal is moving 3x faster than expected). A slow enterprise deal stuck at Opportunity for 45 days scores 0.35.

### `adapters/channels.py` — Channel Registry

Not a scorer — it's a configuration registry that maps marketing channels (like `paid_social.meta`, `email.lifecycle`, `organic.seo`) to their domain adapters and defines channel-specific measurement parameters: primary signals, decay half-life, measurement windows, and minimum sample sizes. Also maps MH1 skill names to channels via `SKILL_CHANNEL_MAP`.

### `adapters/__init__.py` — Package Exports

Makes all adapter classes and channel utility functions importable from the `adapters` package.

---

## Memory — The 4-Layer Memory System

BrightMatter's memory mimics how humans remember. Short-term experiences get stored, decay over time, and the important patterns get promoted to long-term knowledge.

### `memory/working.py` — WorkingMemory

**What it is:** The scratch pad. Fast, in-memory, session-scoped storage for active predictions and recent outcomes. Thread-safe with RLock. Predictions are also persisted to Firebase so deferred checkpoints (24h/7d later) can retrieve them after the process exits.

**What it stores:**
- Active predictions waiting for outcomes (keyed by prediction_id)
- FIFO queue of recent completed outcomes (last 50)
- Arbitrary session context (key-value store for cross-component data sharing)

**Example:** Before running `lifecycle-audit`, you register a prediction saying "I expect a 15% improvement signal." When the skill finishes, you complete the prediction with the actual result. Working memory calculates the prediction error and creates an EpisodicMemory record.

### `memory/episodic.py` — EpisodicMemoryStore

**What it is:** The diary. Firebase-persisted storage for specific experiences (prediction + outcome pairs). Each episode has a weight that decays over time. Old episodes eventually get archived.

**Key behaviors:**
- Episodes decay exponentially: `weight *= 0.95^(age_in_days)`. A 30-day-old episode has ~21% of its original weight.
- When weight drops below 0.3 (relevance threshold), the episode is ready for consolidation into a semantic pattern.
- Episodes older than 90 days get archived (moved to a separate collection).
- Retrieval bumps the retrieval count (frequently-accessed episodes stay relevant longer).

**Firebase path:** `system/intelligence/episodic/{tenant_id}/{skill_name}/{episode_id}`

**Example:** After running `dormant-detection` for Acme Corp, an episode stores: "predicted 15% reactivation rate, observed 22%, prediction error = +0.07, goal completed = true." Over the next month, this episode's weight decays from 1.0 to 0.21, signaling it's ready to be distilled into a pattern.

### `memory/semantic.py` — SemanticMemoryStore

**What it is:** The pattern library. Firebase-persisted storage for learned patterns generalized from episodic memories. Patterns have confidence scores updated via Bayesian inference (Beta distribution).

**Key behaviors:**
- Patterns are skill+domain specific with conditions (context) and recommendations (parameters).
- Confidence is updated via Bayesian inference: `alpha = prior×10 + successes`, `beta = (1-prior)×10 + failures`, `posterior = alpha / (alpha + beta)`.
- Expected value uses EMA: `new = 0.9 × old + 0.1 × observed`.
- Patterns decay at 0.99/day without reinforcement. Below 0.1 confidence with 5+ evidence = archived.
- Includes similarity search (token-based Jaccard + optional LLM-based via Claude).

**Firebase path:** `system/intelligence/semantic/{domain}/patterns/{pattern_id}`

**Example:** After consolidating 12 episodes of `lifecycle-audit` runs for enterprise SaaS clients, a pattern emerges: "when segment_count=5 and include_revenue_impact=true, the expected improvement ratio is 1.35 with confidence 0.72." Next time a similar client runs lifecycle-audit, this pattern informs the recommendation.

Also provides `SemanticMemory` — a lightweight local-file alternative (stores in `~/.mh1/memory/semantic/index.json`) for development without Firebase.

### `memory/procedural.py` — ProceduralMemoryStore

**What it is:** The wisdom. Firebase-persisted storage for cross-skill generalizations. These are the highest-level insights — patterns validated across 3+ different skills.

**Key behaviors:**
- Requires validation from at least 3 different skills before creation.
- Has aggregate confidence based on all validating skills (must exceed 0.6 average).
- Decays very slowly (0.995/day) because stable generalizations shouldn't fade quickly.
- Applicable to new skills that haven't validated it yet.

**Firebase path:** `system/intelligence/procedural/{knowledge_id}`

**Example:** If timing patterns show "morning sends outperform" in email-drip, social-post, AND push-notification skills (3 different skills), that becomes procedural knowledge. It then gets applied to a new skill like `newsletter-builder` even though newsletter-builder hasn't validated it yet.

### `memory/consolidation.py` — MemoryConsolidationManager

**What it is:** The librarian. Orchestrates the entire memory lifecycle: Working → Episodic → Semantic → Procedural. Runs as a periodic job (daily).

**The 6-step cycle:**
1. **Decay** — Apply temporal decay to all episodic memories
2. **Consolidate** — Promote decayed episodes to semantic patterns (with context-aware splitting when a context variable explains >30% of outcome variance)
3. **Archive** — Remove stale semantic patterns below confidence threshold
4. **Promote** — Find cross-skill patterns and create procedural knowledge
5. **TTL cleanup** — Archive episodes past 90 days
6. **Procedural decay** — Apply slow decay to procedural knowledge

**Context-aware splitting:** Before consolidating, the manager checks if splitting episodes by a context variable (like "enterprise" vs "SMB") would reduce outcome variance. If one variable explains >30% of variance, episodes are split into separate groups before creating patterns — so you get separate patterns for enterprise vs SMB instead of one blurry average.

**Trajectory building:** For multi-checkpoint episodes (7-day, 14-day, 30-day measurements), builds `TrajectoryPoint` timelines that track expected performance curves. This prevents scoring a "spike then recover" strategy as a failure at the spike checkpoint.

Also handles module-level consolidation via `consolidate_from_module()` — extracts skill episodes, updates semantic patterns, checks for promotions, and evaluates skill sequences for procedural knowledge.

---

## Learning — Prediction, Bayesian Updates, and Drift Detection

### `learning/predictor.py` — Predictor

**What it is:** The advisor. Decides what parameters to recommend for a skill execution and whether to try something new (explore) or go with what's known to work (exploit).

**Exploration triggers (any of these → explore):**
- Random chance (15% base rate) — always leave room for discovery
- No patterns exist for this skill/domain
- Best pattern confidence < 0.7
- No patterns match the current context (novel situation)

**When exploiting:** Selects the best pattern by `confidence × recent_accuracy`, builds parameters from its recommendation, and blends in procedural knowledge (70% pattern + 30% procedural for numeric values).

**When exploring:** Starts with default parameters for the skill, applies procedural knowledge, then perturbs numeric values by ±20% to try nearby alternatives.

**Phase 0 integration:** When Phase 0 retrieval has run (real CRM data is available), the predictor uses it for smarter pattern matching. If Phase 0 says churn_rate=0.15, the predictor matches patterns from clients with similar churn rates instead of guessing.

**Upstream strategy context:** When strategy skills (positioning-angles) complete before execution skills (email-sequences), their outputs inform downstream parameters. This closes the feedback loop: strategy → execution → learning.

**Example:** You're about to run `lifecycle-audit` for Acme Corp (enterprise SaaS, 2,000 contacts). The Predictor checks semantic memory and finds a pattern: "for enterprise SaaS with 1,500–3,000 contacts, segment_count=5 and include_revenue_impact=true works best, confidence 0.78." Since confidence > 0.7, it exploits — recommends those exact parameters. But 15% of the time, it might explore instead: start with those defaults but bump segment_count to 6 (±20% perturbation) to see if that works better.

### `learning/learner.py` — Learner

**What it is:** The student. Takes observed outcomes and updates the system's beliefs about what works.

**Learning process:**
1. Calculate prediction error: `(observed_signal/baseline) - (expected_signal/baseline)`
2. Apply learning weight (from Three-Gate scoring — see outcomes section)
3. Update each semantic pattern used in the prediction via Bayesian update
4. Check for concept drift
5. Dual-score via shadow testing (if active)
6. Periodically persist error history to Firebase

**Concept drift detection:** Splits error history into "older half" and "recent half." If the mean difference exceeds `2 × standard_deviation`, drift is detected. When detected:
- Reduces confidence of all patterns for that skill/domain by treating each as a failure
- Clears error history to start fresh
- Logs a warning

**Trajectory-aware scoring:** When `checkpoint_day` is provided, the error is computed against the trajectory point for that checkpoint (not the final target). This handles multi-stage strategies correctly.

**Example:** BrightMatter predicted a 1.35 improvement ratio for `lifecycle-audit`. The actual result was 1.22. Prediction error = 1.22 - 1.35 = -0.13 (under-projected). The Three-Gate score says learning_signal_quality = 0.85 (clean signal — the marketer used BrightMatter's parameters, the client approved, and market data came in). So the Learner updates the semantic pattern: Bayesian update treats this as a partial failure, nudging confidence down from 0.78 to 0.74, and adjusting the expected value from 1.35 toward 1.22. If this keeps happening 5 more times, drift detection kicks in — it means the market changed and all patterns for this skill need confidence resets.

### `learning/shadow.py` — ShadowManager

**What it is:** The A/B tester. Tests alternative configurations ("shadow candidates") against the current production settings without disrupting live execution.

**How it works:**
1. **Spawn:** When average prediction error exceeds 0.15, creates a candidate with perturbed weights (±10% of current pattern confidences) or a hypothesis-driven candidate from the improvement analyzer.
2. **Dual-score:** Every outcome gets scored against both production weights AND candidate weights. Results accumulate.
3. **Evaluate:** After 20+ observations over 14+ days, if the candidate's average error is 0.03+ better than production, it gets promoted.
4. **Promote:** Archive production weights (for rollback), apply candidate weights to all semantic patterns, update channel timing if applicable.
5. **Reject:** If improvement < 0.03, archive the candidate and try again later.

**Channel timing exploration:** Candidates also perturb measurement windows (±25%) — maybe a 36-hour window works better than 24 hours for email campaigns.

**Example:** The system's average prediction error for email skills is 0.18 (above the 0.15 threshold). ShadowManager spawns a candidate that tweaks pattern confidences by ±10% — maybe `lifecycle-audit` confidence goes from 0.74 to 0.81, and `email-copy-generator` confidence drops from 0.65 to 0.59. For the next 20+ email skill runs, every outcome gets dual-scored: once with production weights, once with candidate weights. After 3 weeks and 25 observations, if the candidate's average error is 0.12 vs production's 0.18 (improvement of 0.06 > 0.03 threshold), the candidate gets promoted — all semantic pattern confidences update to the candidate's values. If not, it gets archived and a new candidate is tried.

### `learning/accuracy.py` — AccuracyScorer

**What it is:** The report card. Runs daily (or on-demand) to evaluate how well the system's predictions are performing.

**Metrics computed:**
- **MAE (Mean Absolute Error):** Average prediction error across all recent episodes.
- **Per-skill error:** MAE broken down by skill (shows which skills the system is good/bad at predicting).
- **Confidence calibration:** Bins predictions by confidence decile and checks if actual success rate matches. If the system says 80% confidence, do 80% of those predictions succeed?
- **Trend accuracy:** When the system predicted improvement, did improvement actually happen?
- **Exploration hit rate:** What percentage of exploratory runs discovered better parameters?
- **Accuracy trend:** Compares current MAE against last 3 reports (improving/stable/declining).

**Firebase path:** `system/intelligence/accuracy_reports/{YYYY-MM-DD}`

**Example:** Today's accuracy report shows: overall MAE = 0.11, `lifecycle-audit` MAE = 0.07 (good), `email-copy-generator` MAE = 0.22 (bad). Confidence calibration reveals that predictions marked "80% confidence" only succeed 55% of the time — the system is overconfident. Exploration hit rate is 30% — nearly 1 in 3 exploratory runs found better parameters. Trend: MAE improved from 0.14 → 0.12 → 0.11 over the last 3 days (improving). The per-skill breakdown tells you exactly where the system is struggling so you can focus improvement efforts.

### `learning/gold_standard.py` — GoldStandardValidator

**What it is:** The regression test suite. Curated datasets of known-good outcomes that validate shadow promotions and drift relearning don't break things.

**How it works:**
- Loads benchmark datasets from Firebase (`system/intelligence/gold_standards/{dataset_name}`)
- Computes metrics (prediction_error, success_rate, segment_accuracy) against the datasets
- Checks each metric against configured thresholds
- Error metrics: value must be ≤ threshold. Rate/accuracy metrics: value must be ≥ threshold.
- Results persisted to `system/intelligence/benchmark_results`

**Config:** `config/brain_benchmarks.yaml` defines datasets, min sample sizes, and metric thresholds. **Note:** This file does not exist in the brightmatter repo — the code falls back to `{}` if missing. It's expected to be provided by the parent project (e.g., mh1-hq) or created during deployment.

**Example:** After a shadow candidate gets promoted (pattern weights changed), GoldStandardValidator runs the "enterprise-lifecycle-v1" benchmark — 50 curated prediction/outcome pairs from known-good runs. It checks: prediction_error must be ≤ 0.15, success_rate must be ≥ 0.70. If the new weights cause prediction_error to spike to 0.21, the benchmark fails and the promotion gets rolled back. This prevents "improving on average but breaking edge cases."

---

## Improvement — Self-Improvement Engine

### `improvement/analyzer.py` — ImprovementAnalyzer

**What it is:** The detective. Scans closed outcomes for systematic problems — not one-off failures, but repeating patterns that indicate something needs to change.

**Three pattern types detected:**
1. **Under-projection (3+ consecutive):** A skill consistently delivers worse results than predicted. Severity = count × average delta.
2. **Negative feedback (3+ samples, avg < 2.5/5):** Clients consistently rate outputs poorly.
3. **Heavy editing (3+ samples, avg edit depth > 50%):** Clients consistently rewrite the output, meaning tone/voice is wrong.

Over-projections (consistently exceeding predictions) are logged as positive insights for procedural memory.

**Example:** If `email-copy-generator` has 5 consecutive under-projections for enterprise SaaS clients (avg delta = 0.12), the analyzer flags it with severity = 0.60 and produces an `ImprovementCandidate`.

### `improvement/proposer.py` — ImprovementProposer

**What it is:** The prescriber. Takes ImprovementCandidates and generates concrete, actionable proposals for what to change.

**Mapping:**
- Under-projection → `skill_update` (modify SKILL.md strategies) or `new_skill` (if skill doesn't exist)
- Negative feedback → `agent_training` (add training examples to agent persona)
- Heavy editing → `agent_training` (fix voice/tone mismatch, extract more voice samples)

Enriches proposals with external context: failure date clustering, affected clients, temporal patterns.

**Example:** The analyzer flagged `email-copy-generator` with an under-projection candidate (severity 0.60, 5 consecutive misses for enterprise SaaS). The proposer generates a `skill_update` proposal: "Update SKILL.md to add enterprise SaaS-specific tone guidance — current templates use startup-casual language that underperforms with enterprise audiences. Evidence: 5 runs for clients Acme Corp, BigCo, MegaSoft all scored 0.10–0.15 below prediction. Failures clustered in March 2026." The proposal includes the specific SKILL.md section to modify and the suggested changes.

### `improvement/executor.py` — ImprovementExecutor

**What it is:** The dispatcher. Takes approved proposals and queues them for human review. Nothing is auto-deployed — all changes go through the Firebase review queue.

**Handlers:**
- `skill_update` → Queue skill modification for review
- `new_skill` → Queue skill-builder invocation
- `agent_training` → Queue agent persona update
- `template_revision` → Queue template update

**Example:** A `skill_update` proposal for `email-copy-generator` gets approved by a human reviewer. The executor queues it: writes the proposed SKILL.md diff to `system/intelligence/improvement_queue/imp_abc123` in Firebase with status `dispatched`. A human developer picks it up, reviews the suggested tone changes, applies them to `skills/email-copy-generator/SKILL.md`, and marks it `applied`. The system never auto-modifies skill files — humans always approve the final change.

### `improvement/review.py` — ImprovementReviewer

**What it is:** The gatekeeper. Manages the review queue in Firebase. Proposals flow through: `pending → approved/rejected → dispatched → applied`.

Provides listing (with status filtering), approval/rejection with notes, statistics, and audit trail.

**Example:** A team lead opens the review queue and sees 3 pending proposals: one `skill_update` for `email-copy-generator` (severity 0.60), one `agent_training` for `linkedin-ghostwriter` (negative feedback, avg rating 2.1/5), and one `template_revision` for the weekly-report template (heavy editing, avg 65% rewrite). They approve the first two with notes ("agreed, enterprise tone is off" and "add 3 more voice samples from client"), reject the third ("clients prefer customizing reports, editing is expected"). Each decision is timestamped and stored for audit.

---

## Outcomes — Deferred Outcome Tracking

### `outcomes/pending_store.py` — PendingOutcomeStore

**What it is:** The patience engine. Stores predictions awaiting real-world outcome measurement. Marketing results don't appear instantly — email campaigns need 24 hours for open rates, 7 days for conversion data.

**Lifecycle:** `pending → checkpoint_24h → checkpoint_7d → closed` (or `→ expired` after 14 days with no data).

**Channel-aware scheduling:** Different channels have different measurement windows. Email might use 24h/7d. Paid social might use 6h/3d. The schedule is derived from `ChannelConfig.measurement_window_hours` and `full_measurement_days`.

**Firebase path:** `system/intelligence/pending_outcomes/{client_id}/{prediction_id}`

**Example:** You run `cohort-email-builder` for Acme Corp. The skill generates a 5-email drip sequence. BrightMatter predicted a 28% open rate. But you can't know the actual open rate for at least 24 hours. So a PendingOutcome gets created: prediction_id = "pred_xyz", status = "pending", checkpoint schedule = [24h, 7d], channel = "email.lifecycle". It sits in Firebase waiting. 24 hours later, the CheckpointProcessor picks it up and measures the first checkpoint.

### `outcomes/checkpoint_processor.py` — CheckpointProcessor

**What it is:** The cron job. Runs on schedule, finds pending outcomes that are due for measurement, collects metrics from data sources, computes composite scores, and closes the loop.

**Data sources (priority order):**
1. MH1HQ MCP — query execution results and report data
2. Platform APIs — campaign_id, ad_set_id from delivery metadata
3. Report URL — check if report was viewed/actioned

Classifies outcomes as `under_projection` (<80% of expected), `over_projection` (>120%), or `accurate_projection`.

**Example:** It's 24 hours after the Acme Corp email drip launched. The CheckpointProcessor finds "pred_xyz" is due for its 24h checkpoint. It queries the email platform via delivery metadata (sequence_id = "seq_456") and gets: open rate = 32%, click rate = 4.2%. It also checks PostHog — the client viewed the performance dashboard 3 times and shared the report link. CompositeSignalComputer combines everything into a score of 0.78. BrightMatter predicted 0.72. Delta = +0.06 — classified as `accurate_projection`. The pending outcome advances to "checkpoint_24h" and waits for the 7-day measurement.

### `outcomes/delivery_extractor.py` — DeliveryExtractor

**What it is:** The identifier finder. Parses skill outputs to extract measurable identifiers (campaign IDs, email sequence IDs, report URLs) that the checkpoint system can use to measure real-world outcomes later.

Knows which skills produce measurable outputs (25 skills mapped) and recursively scans output dicts for known ID keys.

**Example:** The `cohort-email-builder` skill finishes and outputs a JSON blob with email content. The DeliveryExtractor scans the output and finds `{"sequence_id": "seq_456", "campaign_id": "camp_789", "report_url": "https://app.mh1.dev/reports/abc"}`. These get attached to the PendingOutcome so that 24 hours later, the CheckpointProcessor knows exactly where to look for performance data — it queries the email platform using sequence_id "seq_456" and checks whether anyone visited the report URL.

### `outcomes/signal_computer.py` — CompositeSignalComputer

**What it is:** The scorer. Combines three signal sources into one composite score:
- **Platform metrics (40%)** — email open/click rates, pipeline velocity, campaign ROAS
- **Client feedback (35%)** — portal ratings, sentiment analysis, comment tone
- **User behavior (25%)** — time-to-approval, edit depth, report views, sharing, adoption

Each source produces a 0–1 score. The composite is a weighted average. The delta from expected score determines the projection classification.

**Example:** For the Acme Corp email campaign at the 24h checkpoint:
- **Platform metrics (40%):** Open rate 32% vs 25% industry avg → normalized to 0.82. Click rate 4.2% vs 3% avg → 0.78. Platform score = 0.80.
- **Client feedback (35%):** Client left a 4/5 rating on the portal with comment "great subject lines." Sentiment = positive. Feedback score = 0.80.
- **User behavior (25%):** Approved in 2 hours (fast), edit depth 10% (barely touched it), 3 report views. Behavior score = 0.72.
- **Composite:** (0.80 × 0.40) + (0.80 × 0.35) + (0.72 × 0.25) = 0.32 + 0.28 + 0.18 = **0.78**. Expected was 0.72. Delta = +0.06 → `accurate_projection`.

### `outcomes/behavior_collector.py` — BehaviorCollector

**What it is:** The spy (the good kind). Queries PostHog (analytics) and Firebase (user activity) for behavioral signals that feed the composite scorer.

**Signals collected:**
- `time_to_approval_hours` — how fast the client approved (fast = good fit)
- `edit_depth` — how much content was rewritten (heavy = poor fit)
- `report_views` — view count (more = higher engagement)
- `scroll_depth_pct` — how far they scrolled (deeper = more engaged)
- `shared` — did they share it externally (strong positive)
- `adopted` — did they implement recommendations (strongest positive)

**Example:** PostHog shows that after the Acme Corp lifecycle audit was delivered, the client: approved in 2.1 hours (fast — suggests good fit), edited 10% of the content (minimal — voice was right), viewed the report 3 times over 2 days, scrolled to 92% depth, shared it with their VP of Marketing, and started implementing the first recommendation. The BehaviorCollector normalizes these into a behavior score of 0.72 — strong engagement signals that feed into the composite scorer.

### `outcomes/three_gate.py` — ThreeGateScorer

**What it is:** The resonance meter. Measures three sequential gates of marketing output validation:

1. **Gate 1: Operator Resonance (25%)** — Did the marketer adopt the strategy? Measured by edit distance, parameter overrides, review speed.
2. **Gate 2: Client Resonance (25%)** — Did the client approve and implement? Measured by approval rounds, sentiment, implementation fidelity.
3. **Gate 3: Market Resonance (50%)** — Did the audience respond? Measured by primary metric vs baseline, sample size for confidence.

**Key innovation:** `learning_signal_quality = compound × openness_ratio × gate_completeness`. The learning weight tells the Learner how much to trust this outcome. If the marketer overrode all of BrightMatter's suggestions (low openness), the outcome doesn't teach BrightMatter much — it was the marketer's choices, not the system's. If all parameters were BrightMatter-generated and all 3 gates are measured, it's a clean signal.

**Example:** BrightMatter recommended 5 segments for a lifecycle audit. Here's what happened at each gate:
- **Gate 1 (Operator):** The marketer used 4 of the 5 recommended segments and changed one parameter. Edit distance = 15%, review speed = 45 min. Openness ratio = 0.85 (mostly BrightMatter's plan). Gate 1 score = 0.80.
- **Gate 2 (Client):** The client approved on the first round with positive sentiment ("this is exactly what we needed"). Implementation fidelity = 90%. Gate 2 score = 0.90.
- **Gate 3 (Market):** The churn rate dropped from 8.2% to 6.1% over 30 days (25% improvement). Sample size = 2,000 customers (high confidence). Gate 3 score = 0.85.
- **Compound:** 0.80 × 0.90 × 0.85 = 0.612. **learning_signal_quality** = 0.612 × 0.85 (openness) × 1.0 (all 3 gates measured) = **0.52**. This tells the Learner: "trust this outcome at 52% weight when updating patterns." If the marketer had overridden everything (openness = 0.10), the quality would drop to 0.06 — barely any learning, because it was the human's choices, not the system's.

---

## Intelligence Root Files

### `intelligence/__init__.py` — IntelligenceEngine

**What it is:** The brain. The unified entry point that wires everything together. Initializes all memory layers, the predictor, learner, shadow manager, accuracy scorer, gold standard validator, and domain adapters.

**Main methods:**
- `get_guidance(skill, tenant, domain, context)` → Guidance object with parameters and confidence
- `register_prediction(skill, tenant, domain, expected_signal, expected_baseline)` → prediction_id
- `record_outcome(prediction_id, observed_signal, goal_completed)` → learning result
- `score(domain, event, context)` → ScoringResult from domain adapter
- `run_consolidation(tenant_id)` → memory consolidation stats
- `record_user_feedback(prediction_id, rating, correction)` → pattern confidence updates

**Example:** When MH1 is about to run `lifecycle-audit` for Acme Corp, the runner calls `engine.get_guidance("lifecycle-audit", "acme-corp", "health", {"industry": "saas", "contacts": 2000})`. The engine returns a Guidance object: `{parameters: {segment_count: 5, include_revenue_impact: true}, confidence: 0.78, source: "exploit", pattern_id: "pat_abc"}`. The runner uses those parameters. After execution, it calls `engine.record_outcome("pred_xyz", observed_signal=1.22, goal_completed=True)`. The engine stores the episode, updates the pattern's confidence via Bayesian inference, and checks for drift.

### `intelligence/types.py` — Core Types

Defines all shared dataclasses and enums:
- `Domain` enum: CONTENT, REVENUE, HEALTH, CAMPAIGN, GENERIC
- `Prediction` / `Outcome` — the prediction-outcome pair
- `EpisodicMemory` — a specific experience with weight and decay
- `SemanticPattern` — a learned pattern with confidence and recommendations
- `ProceduralKnowledge` — cross-skill generalization
- `TrajectoryPoint` — checkpoint in a multi-day performance curve
- `EpisodeSource` / `MemoryLayer` — classification enums

**Example:** A `Prediction` object looks like: `{prediction_id: "pred_xyz", skill_name: "lifecycle-audit", tenant_id: "acme-corp", domain: "HEALTH", expected_signal: 1.35, expected_baseline: 1.0, context: {industry: "saas", contacts: 2000}, confidence: 0.78}`. Its matching `Outcome` would be: `{prediction_id: "pred_xyz", observed_signal: 1.22, goal_completed: True, feedback_rating: 4.0}`. Together they become an `EpisodicMemory` with `prediction_error: -0.13` and `weight: 1.0` (which decays daily).

### `intelligence/jarvis_episodes.py` — Jarvis Episode Converter

Converts Jarvis-format episodes (interactive AI assistant sessions) into BrightMatter-compatible EpisodicMemory objects. Maps user satisfaction (0–5 scale) to observed signal (0.0–1.0), preserves context (trigger, tools used, topic tags), and stores corrections/preferences as outcome metadata.

**Example:** A Jarvis session where the user asked "help me write a LinkedIn post about AI in healthcare" and rated it 4/5 stars. The converter maps satisfaction 4 → observed_signal 0.8, records context `{trigger: "user_request", tools_used: ["ghostwrite-content"], topic_tags: ["linkedin", "ai", "healthcare"]}`, and stores the user's correction ("make it less formal") as outcome metadata. This becomes an EpisodicMemory that teaches BrightMatter: "for LinkedIn + healthcare topics, a less formal tone scores 0.8 satisfaction."

---

## Library Root Files

### `lib/firebase_client.py` — FirebaseClient

Thread-safe Firebase Firestore client with connection pooling. Features:
- Per-thread connection pool with configurable max connections (10 default)
- Retry with exponential backoff (3 retries, 0.5s base delay)
- Credential resolution chain: `FIREBASE_SERVICE_ACCOUNT_JSON` → `FIREBASE_CREDENTIALS_JSON` → `SERVICE_ACCOUNT_KEY` → `GOOGLE_APPLICATION_CREDENTIALS` → Application Default
- Full CRUD: `get_document`, `set_document`, `update_document`, `delete_document`, `query`, `get_collection`
- Batch writes (atomic or non-atomic)
- Transaction support
- `resolve_collection_path()` for deep nested paths
- `list_subcollections()` for subcollection enumeration
- Singleton accessor via `get_firebase_client()`

**Example:** You need to store a semantic pattern. Instead of dealing with raw Firestore SDK, you call `fb.set_document("system/intelligence/semantic/health/patterns", "pat_abc", {confidence: 0.78, ...})`. The client handles retries (if Firebase is temporarily unavailable, it waits 0.5s, then 1s, then 2s before giving up), thread safety (multiple skills running in parallel won't corrupt each other's writes), and credential resolution (it figures out which env var has the Firebase key).

### `lib/intelligence_bridge.py` — IntelligenceBridge

The simplified interface between MH1 runners and the intelligence system. Maps all 66 skills to their business domains. Handles:
- **Guidance retrieval** with Phase 0 metric enrichment and upstream strategy context
- **Prediction tracking** — `start_tracking()` → run skill → `complete_tracking()`
- **Deferred outcomes** — `complete_tracking(deferred=True)` skips learning; `close_deferred_outcome()` triggers it later with real platform data
- **Three-Gate scoring** integration for learning signal quality
- **Phase 0 snapshot persistence** — stores temporal snapshots for MoM/WoW delta computation
- **Module consolidation** — extracts patterns from completed workflow executions
- Graceful degradation when intelligence system is unavailable

**Example:** The MH1 execution engine is about to run `lifecycle-audit`. The runner calls `bridge.start_tracking("lifecycle-audit", context={industry: "saas"})` — the bridge infers domain = HEALTH (from its 64-skill mapping), asks the IntelligenceEngine for guidance, and returns a tracking_id. The skill runs. Then the runner calls `bridge.complete_tracking(tracking_id, result=skill_output, deferred=True)`. Because `deferred=True`, the bridge skips immediate learning and instead creates a PendingOutcome — the real email open rates won't be available for 24 hours. If BrightMatter is down entirely, the bridge logs a warning and returns default parameters so the skill still runs.

### `lib/client.py` — BrightMatterClient

Thin HTTP client for MH1HQ and Jarvis to call the BrightMatter API. Uses only stdlib `urllib` (no `requests` dependency). Methods mirror the API: `write_episode()`, `write_jarvis_episode()`, `get_guidance()`, `record_outcome()`, `record_feedback()`, `run_consolidation()`, `get_patterns()`, `health()`.

**Example:** From MH1HQ (a completely separate codebase), you can talk to BrightMatter with 3 lines:
```python
bm = BrightMatterClient("http://localhost:8100", api_key="sk-xxx")
guidance = bm.get_guidance("lifecycle-audit", tenant_id="acme-corp", domain="health")
print(guidance["parameters"])  # {segment_count: 5, include_revenue_impact: true}
```
No need to import any BrightMatter internals — it's just HTTP calls.

### `lib/remote_bridge.py` — RemoteIntelligenceBridge

Drop-in replacement for `IntelligenceBridge` that routes all calls through the BrightMatter HTTP API instead of importing the engine directly. Used during transition from direct imports to API-based integration. Copy-pasteable into other repos.

**Example:** In MH1HQ, you used to have `from lib.intelligence_bridge import IntelligenceBridge` (direct import — requires BrightMatter code on the same machine). To decouple, you swap one line: `from lib.remote_bridge import RemoteIntelligenceBridge as IntelligenceBridge`. Everything else stays identical — `bridge.start_tracking()`, `bridge.complete_tracking()` — but now it's making HTTP calls to the BrightMatter API instead of running the engine in-process.

### `lib/memory_health.py` — Memory Health Checker

Production verification tool. Connects to Firebase and checks each memory layer:
- Episodic: counts episodes per tenant/skill
- Semantic: counts patterns per domain
- Procedural: counts cross-skill knowledge entries
- Shadow: checks shadow testing state
- Accuracy: checks accuracy report history

Outputs human-readable table or JSON. Diagnoses empty memory ("learning loop hasn't persisted data"), degraded layers, and Firebase connectivity issues.

**Example:** You run `mh1 memory-health` and get:
```
Layer          │ Status  │ Count │ Last Updated
───────────────┼─────────┼───────┼────────────────
Episodic       │ healthy │   847 │ 2026-03-21 14:30
Semantic       │ healthy │   123 │ 2026-03-21 14:30
Procedural     │ healthy │    18 │ 2026-03-20 02:00
Shadow         │ active  │     1 │ 2026-03-19 09:15
Accuracy       │ healthy │    45 │ 2026-03-21 00:00
```
If episodic shows 0 counts, it means the learning loop isn't persisting — probably Firebase credentials aren't reaching Modal sandboxes. If semantic shows 0 but episodic is healthy, consolidation isn't running (the daily cron is broken).

---

## Repo Root Files

### `api.py` — FastAPI Application

The HTTP server that exposes the IntelligenceEngine. Endpoints:
- `GET /api/v1/health` — engine status
- `POST /api/v1/episodes/write` — write a skill episode
- `POST /api/v1/episodes/jarvis` — write a Jarvis episode
- `GET /api/v1/guidance/{skill_name}` — pre-execution guidance
- `POST /api/v1/outcomes/record` — record observed outcome
- `POST /api/v1/outcomes/feedback` — record user feedback
- `POST /api/v1/consolidation/run` — trigger consolidation
- `POST /api/v1/tracking/start` — start prediction tracking
- `POST /api/v1/tracking/complete` — complete tracking with outcome
- `POST /api/v1/modules/consolidate` — extract module learnings
- `POST /api/v1/checkpoints/process` — process due checkpoints (cron)
- `GET /api/v1/patterns/{skill_name}` — get learned patterns

Auth via `X-API-Key` header (optional in dev mode). Runs on port 8100 via uvicorn.

**Example:** Start the server with `uvicorn api:app --port 8100`. Now any service can call it:
```
curl -X GET http://localhost:8100/api/v1/guidance/lifecycle-audit \
  -H "X-API-Key: sk-xxx" \
  -d '{"tenant_id": "acme-corp", "domain": "health", "context": {"industry": "saas"}}'

# Returns: {"parameters": {"segment_count": 5}, "confidence": 0.78, "source": "exploit"}
```
The API is the public door to the entire intelligence system — everything that happens inside (memory lookups, pattern matching, Bayesian updates) is hidden behind these simple REST endpoints.

### `pyproject.toml`

Python 3.11+. Dependencies: firebase-admin, fastapi, uvicorn, pydantic, python-dotenv, numpy. Dev extras: pytest, pytest-asyncio, httpx, ruff.

### `scripts/diagnose_consolidation.py`

Debug tool that tests each consolidation pipeline stage independently: Firebase connectivity → episodic store (tenant/skill enumeration, ready episodes) → semantic store (method availability) → procedural store → full consolidation cycle. Pinpoints exactly where failures occur.

**Example:** Memory consolidation is failing silently. You run `python scripts/diagnose_consolidation.py` and it tests each stage:
```
[1/5] Firebase connectivity... OK (project: moe-platform-479917)
[2/5] Episodic store... OK (3 tenants, 847 episodes, 23 ready for consolidation)
[3/5] Semantic store... FAIL: method 'consolidate_episodes' not found
[4/5] Procedural store... SKIPPED (depends on semantic)
[5/5] Full consolidation... SKIPPED
```
Now you know exactly where the problem is — the semantic store class is missing a method, probably from a bad deploy. Without this script, you'd be digging through logs guessing.

---

## How Everything Connects

Here is the end-to-end flow of a single skill execution through BrightMatter:

### Before Execution

```
Runner                    IntelligenceBridge           IntelligenceEngine
  │                              │                            │
  │─── get_skill_guidance() ────►│                            │
  │                              │── infer_domain() ─────────►│
  │                              │── get_guidance() ─────────►│
  │                              │                            │── Predictor.get_guidance()
  │                              │                            │     ├── SemanticMemoryStore.retrieve_patterns()
  │                              │                            │     ├── ProceduralMemoryStore.get_applicable()
  │                              │                            │     ├── Phase 0 metric adjustments
  │                              │                            │     ├── Upstream strategy adjustments
  │                              │                            │     └── Explore vs Exploit decision
  │◄── SkillGuidance ───────────│◄── Guidance ───────────────│
  │                              │                            │
  │─── start_tracking() ────────►│                            │
  │                              │── register_prediction() ──►│
  │                              │                            │── WorkingMemory.register_prediction()
  │                              │                            │── Firebase persist (for deferred lookups)
  │◄── tracking_id ─────────────│                            │
```

### After Execution

```
Runner                    IntelligenceBridge           IntelligenceEngine
  │                              │                            │
  │─── complete_tracking() ─────►│                            │
  │    (result, metrics)         │── record_outcome() ──────►│
  │                              │                            │── WorkingMemory.complete_prediction()
  │                              │                            │     └── Calculates prediction error
  │                              │                            │── EpisodicMemoryStore.store()
  │                              │                            │── Learner.learn_from_outcome()
  │                              │                            │     ├── Update SemanticPatterns (Bayesian)
  │                              │                            │     ├── Check for concept drift
  │                              │                            │     └── Shadow dual-scoring
  │◄── LearningResult ─────────│◄── outcome_result ─────────│
```

### Deferred Outcomes (24h / 7d later)

```
Cron                      CheckpointProcessor         IntelligenceBridge
  │                              │                            │
  │─── process_all_due() ──────►│                            │
  │                              │── PendingOutcomeStore.query_due("24h")
  │                              │── BehaviorCollector.collect()
  │                              │── CompositeSignalComputer.compute()
  │                              │── ThreeGateScorer.score_gate_3()
  │                              │── close_deferred_outcome() ──────────►│
  │                              │                            │── Learner.learn_from_outcome()
  │                              │                            │     (with learning_signal_quality weight)
```

### Daily Consolidation

```
Cron/CLI                  IntelligenceEngine          Memory Stores
  │                              │                            │
  │─── run_consolidation() ────►│                            │
  │                              │── 1. EpisodicStore.decay_all()
  │                              │       └── Apply weight *= 0.95^days
  │                              │── 2. Consolidate ready episodes
  │                              │       ├── Context-split detection
  │                              │       ├── SemanticStore.consolidate_episodes()
  │                              │       └── Trajectory building
  │                              │── 3. SemanticStore.forget_stale_patterns()
  │                              │── 4. Promote to procedural
  │                              │       ├── Find cross-skill patterns (3+ skills)
  │                              │       └── ProceduralStore.create_from_patterns()
  │                              │── 5. EpisodicStore.cleanup_old_episodes()
  │                              │── 6. ProceduralStore.decay_all()
```

### Self-Improvement Loop

```
AccuracyScorer                ImprovementAnalyzer        ShadowManager
  │                              │                            │
  │── Daily MAE report ─────────►│                            │
  │                              │── Scan closed outcomes ───►│
  │                              │── Detect patterns:         │
  │                              │     3+ under-projections   │
  │                              │     Negative feedback      │
  │                              │     Heavy editing          │
  │                              │                            │
  │                              │── ImprovementProposer      │
  │                              │     → skill_update         │
  │                              │     → agent_training       │
  │                              │     → new_skill            │
  │                              │                            │
  │                              │── ImprovementReviewer      │
  │                              │     → Firebase queue       │
  │                              │     → Human approval       │
  │                              │                            │
  │                              │                            │── maybe_spawn_candidate()
  │                              │                            │     ├── Hypothesis-driven
  │                              │                            │     │   (from analyzer results)
  │                              │                            │     └── Random perturbation
  │                              │                            │── dual_score() on every outcome
  │                              │                            │── evaluate_candidate()
  │                              │                            │     ├── Promote (→ update patterns)
  │                              │                            │     └── Reject (→ archive)
```

### External Integration

```
MH1HQ / Jarvis                                    BrightMatter
  │                                                      │
  ├── Option A: Direct import ──────────────────────────►│
  │   from lib.intelligence_bridge import IntelligenceBridge
  │                                                      │
  ├── Option B: HTTP client ────────────────────────────►│
  │   from lib.client import BrightMatterClient          │── api.py (FastAPI)
  │   bm = BrightMatterClient("http://localhost:8100")   │
  │                                                      │
  └── Option C: Remote bridge ──────────────────────────►│
      from lib.remote_bridge import RemoteIntelligenceBridge
      (drop-in replacement, same interface, API-backed)
```

### The Closed Learning Loop

The entire system forms a closed loop:

1. **Predict** — Before a skill runs, the Predictor recommends parameters based on what has worked before
2. **Execute** — The skill runs with those parameters
3. **Observe** — Outcomes are recorded immediately (validation score) and deferred (24h adoption, 7d performance)
4. **Score** — Three-Gate scoring determines how clean the learning signal is
5. **Learn** — The Learner updates pattern confidence via Bayesian inference
6. **Consolidate** — Episodes decay and get promoted to semantic patterns
7. **Generalize** — Cross-skill patterns become procedural knowledge
8. **Improve** — Systematic failures trigger improvement proposals
9. **Test** — Shadow testing validates changes before promotion
10. **Verify** — Gold standard benchmarks prevent regression

Every execution makes the system slightly smarter. After enough cycles, it knows which parameters work for which client types, which strategies to recommend, and when the market has shifted (drift detection triggers relearning).
