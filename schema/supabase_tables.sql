-- BrightMatter Shared Database Schema (Supabase/Postgres)
--
-- All four platforms (MH1HQ, MH-OS, mh1-skills, Jarvis) write events
-- to the shared 'events' table. BrightMatter pulls on a cron schedule,
-- processes through the learning pipeline, and writes guidance back to
-- 'guidance_cache'. Each platform reads guidance before execution.

-- ── Events (write path) ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS events (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  source TEXT NOT NULL,                         -- "mh-os", "mh1-hq", "mh1-skills", "jarvis"
  event_type TEXT NOT NULL,                     -- "signal", "skill_completed", "plan_completed",
                                                -- "human_feedback", "expert_override"
  skill_name TEXT,
  client_id TEXT NOT NULL,
  domain TEXT DEFAULT 'generic',
  result JSONB,
  metrics JSONB,
  context JSONB,                                -- includes channel_context
  channel_context JSONB,                        -- structured ChannelContext
  created_at TIMESTAMPTZ DEFAULT now(),
  processed_by_bm BOOLEAN DEFAULT false
);

-- ── Predictions (tracking) ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS predictions (
  tracking_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  client_id TEXT NOT NULL,
  domain TEXT DEFAULT 'generic',
  expected_signal FLOAT,
  expected_baseline FLOAT,
  confidence FLOAT DEFAULT 0.5,
  guidance JSONB,
  channel_context JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- ── Outcomes (resolution) ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS outcomes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tracking_id TEXT REFERENCES predictions(tracking_id),
  source TEXT NOT NULL,
  client_id TEXT NOT NULL,
  outcome_type TEXT NOT NULL,                   -- "24h_checkpoint", "7d_checkpoint", "immediate"
  observed_signal FLOAT,
  goal_completed BOOLEAN,
  business_impact FLOAT,
  feedback JSONB,
  external_context JSONB,                       -- seasonality, anomaly flags, platform issues
  measured_at TIMESTAMPTZ DEFAULT now()
);

-- ── Guidance Cache (read path) ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS guidance_cache (
  skill_name TEXT NOT NULL,
  client_id TEXT NOT NULL,
  domain TEXT DEFAULT 'generic',
  parameters JSONB,
  confidence FLOAT DEFAULT 0.5,
  expected_value FLOAT,
  is_exploration BOOLEAN DEFAULT true,
  patterns_used TEXT[],
  predicted_outcome FLOAT,
  predicted_baseline FLOAT,
  pattern_expected_value FLOAT,
  channel_context JSONB,
  blend_weights JSONB,                          -- {"client": 0.15, "segment": 0.55, "universal": 0.30}
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (skill_name, client_id)
);

-- ── Patterns (mirror of Firebase semantic patterns) ────────────────

CREATE TABLE IF NOT EXISTS patterns (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  domain TEXT NOT NULL,
  skill_name TEXT,
  client_id TEXT,
  pattern_level TEXT,                           -- "universal", "segment", "client"
  description TEXT,
  confidence FLOAT,
  evidence_count INT DEFAULT 0,
  parameters JSONB,
  condition JSONB,
  expected_trajectory JSONB,
  expected_value FLOAT,
  recent_accuracy FLOAT DEFAULT 0.5,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- ── Indexes ────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_events_unprocessed
  ON events(processed_by_bm, created_at) WHERE NOT processed_by_bm;

CREATE INDEX IF NOT EXISTS idx_events_client
  ON events(client_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_source_type
  ON events(source, event_type);

CREATE INDEX IF NOT EXISTS idx_guidance_lookup
  ON guidance_cache(skill_name, client_id);

CREATE INDEX IF NOT EXISTS idx_patterns_level
  ON patterns(pattern_level, domain);

CREATE INDEX IF NOT EXISTS idx_patterns_skill_client
  ON patterns(skill_name, client_id);

CREATE INDEX IF NOT EXISTS idx_outcomes_tracking
  ON outcomes(tracking_id);

CREATE INDEX IF NOT EXISTS idx_events_source_skill
  ON events(source, event_type, client_id);

CREATE INDEX IF NOT EXISTS idx_events_report
  ON events(event_type, created_at)
  WHERE event_type IN ('report_viewed', 'report_shared', 'report_feedback');

-- ── Internal Memory (BrightMatter only) ───────────────────────────
--
-- These tables replace Firebase Firestore as the authoritative store
-- for BrightMatter's 4 memory layers. The shared tables above (events,
-- predictions, outcomes, guidance_cache, patterns) remain unchanged.

-- Episodic: individual experiences (prediction + outcome pairs)
-- Weight decays daily via cron. Ready for consolidation at weight < 0.3.
CREATE TABLE IF NOT EXISTS episodic_memory (
  episode_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  domain TEXT DEFAULT 'generic',
  prediction JSONB NOT NULL DEFAULT '{}',
  outcome JSONB DEFAULT '{}',
  weight FLOAT DEFAULT 1.0,
  prediction_error FLOAT,
  retrieval_count INT DEFAULT 0,
  last_retrieved_at TIMESTAMPTZ,
  consolidated_at TIMESTAMPTZ,
  archived_at TIMESTAMPTZ,
  source TEXT DEFAULT 'mh1hq',
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Semantic: learned patterns generalized from episodes
-- Confidence updated via Bayesian Beta distribution.
CREATE TABLE IF NOT EXISTS semantic_patterns (
  pattern_id TEXT PRIMARY KEY,
  skill_name TEXT NOT NULL,
  domain TEXT NOT NULL,
  client_id TEXT,
  pattern_level TEXT DEFAULT 'segment',
  condition JSONB DEFAULT '{}',
  recommendation JSONB DEFAULT '{}',
  confidence FLOAT DEFAULT 0.5,
  expected_value FLOAT DEFAULT 1.0,
  variance FLOAT DEFAULT 1.0,
  expected_trajectory JSONB,
  expected_time_to_target_days FLOAT,
  variance_days FLOAT,
  evidence_count INT DEFAULT 0,
  successes INT DEFAULT 0,
  failures INT DEFAULT 0,
  recent_accuracy FLOAT DEFAULT 0.5,
  source_episodes TEXT[],
  last_reinforced_at TIMESTAMPTZ,
  tenant_ids TEXT[],
  pattern_type TEXT,
  archived_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Procedural: cross-skill generalizations (validated by 3+ skills)
-- Decays very slowly: confidence *= 0.995^days.
CREATE TABLE IF NOT EXISTS procedural_knowledge (
  knowledge_id TEXT PRIMARY KEY,
  skill_name TEXT,
  domain TEXT DEFAULT 'generic',
  description TEXT,
  condition JSONB DEFAULT '{}',
  recommendation JSONB DEFAULT '{}',
  confidence FLOAT DEFAULT 0.5,
  cross_skill_confidence FLOAT DEFAULT 0.5,
  validating_skills TEXT[],
  validation_count INT DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Working memory predictions (persisted for deferred checkpoint lookup)
CREATE TABLE IF NOT EXISTS working_predictions (
  prediction_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  domain TEXT DEFAULT 'generic',
  expected_signal FLOAT,
  expected_baseline FLOAT,
  confidence FLOAT DEFAULT 0.5,
  context JSONB,
  patterns_used TEXT[],
  is_exploration BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Pending outcomes: predictions awaiting real-world measurement.
-- Skill executions register here with deferred 24h / 7d checkpoints.
-- Status lifecycle: pending -> checkpoint_24h -> checkpoint_7d -> closed
--                                                              -> expired (14d)
CREATE TABLE IF NOT EXISTS pending_outcomes (
  prediction_id             TEXT PRIMARY KEY,
  tracking_id               TEXT,
  skill_name                TEXT,
  client_id                 TEXT NOT NULL,
  channel_id                TEXT,
  module_id                 TEXT,
  run_id                    TEXT,
  node_id                   TEXT,
  status                    TEXT DEFAULT 'pending',
  prediction                JSONB DEFAULT '{}',
  generation_score          FLOAT,
  delivery_metadata         JSONB DEFAULT '{}',
  platform_config           JSONB DEFAULT '{}',
  checkpoints               JSONB DEFAULT '[]',
  checkpoint_schedule       JSONB DEFAULT '[]',
  projection_classification TEXT,
  composite_score           FLOAT,
  created_at                TIMESTAMPTZ DEFAULT now(),
  due_24h                   TIMESTAMPTZ,
  due_7d                    TIMESTAMPTZ,
  closed_at                 TIMESTAMPTZ,
  updated_at                TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_outcomes_client_status
  ON pending_outcomes(client_id, status);

CREATE INDEX IF NOT EXISTS idx_pending_outcomes_due_24h
  ON pending_outcomes(due_24h) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_pending_outcomes_due_7d
  ON pending_outcomes(due_7d) WHERE status = 'checkpoint_24h';

CREATE INDEX IF NOT EXISTS idx_pending_outcomes_closed_at
  ON pending_outcomes(closed_at DESC) WHERE status = 'closed';

-- Auxiliary tables for shadow testing, accuracy, and error history.
-- These are optional — the system degrades gracefully without them.
CREATE TABLE IF NOT EXISTS shadow_state (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shadow_history (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accuracy_reports (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS error_history (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS channel_timing (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_standards (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS benchmark_results (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- ── Indexes for internal memory ───────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_episodic_tenant_skill
  ON episodic_memory(tenant_id, skill_name);

CREATE INDEX IF NOT EXISTS idx_episodic_unconsolidated
  ON episodic_memory(weight, consolidated_at)
  WHERE consolidated_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_episodic_weight
  ON episodic_memory(weight DESC);

CREATE INDEX IF NOT EXISTS idx_semantic_domain_skill
  ON semantic_patterns(domain, skill_name);

CREATE INDEX IF NOT EXISTS idx_semantic_client
  ON semantic_patterns(client_id, skill_name)
  WHERE client_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_semantic_level
  ON semantic_patterns(pattern_level, domain);

CREATE INDEX IF NOT EXISTS idx_procedural_domain
  ON procedural_knowledge(domain);

CREATE INDEX IF NOT EXISTS idx_procedural_skills
  ON procedural_knowledge USING GIN(validating_skills);

CREATE INDEX IF NOT EXISTS idx_working_tenant
  ON working_predictions(tenant_id, skill_name);

-- ── RPC Functions ──────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION get_active_pairs(lookback_days INT DEFAULT 30)
RETURNS TABLE(skill_name TEXT, client_id TEXT) AS $$
  SELECT DISTINCT e.skill_name, e.client_id
  FROM events e
  WHERE e.skill_name IS NOT NULL
    AND e.created_at > NOW() - (lookback_days || ' days')::INTERVAL
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION get_distinct_tenants()
RETURNS TABLE(tenant_id TEXT) AS $$
  SELECT DISTINCT e.tenant_id FROM episodic_memory e
$$ LANGUAGE sql STABLE;

-- ── BrightMatter Watermarks ──────────────────────────────────────
-- Tracks ingestion cursors for external data sources (signals, BQ, etc.)
-- so BrightMatter only processes new data on each cycle.

CREATE TABLE IF NOT EXISTS bm_watermarks (
  source TEXT PRIMARY KEY,
  last_processed_id TEXT,
  last_processed_at TIMESTAMPTZ,
  metadata JSONB DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bm_watermarks_source
  ON bm_watermarks(source);

-- ── Reference Knowledge ────────────────────────────────────────────
-- Curated expert frameworks, tactics, ad examples, benchmarks, and
-- course material from DTC-OS and other sources.  BrightMatter queries
-- this at guidance-time and consolidation-time — never double-stored
-- as episodic memory.

CREATE TABLE IF NOT EXISTS reference_knowledge (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL,          -- expert-panel | tactics-vault | ad-vault | mkt1-templates | b2c-courses | dtc-benchmarks
  category TEXT NOT NULL,        -- persuasion | advertising | positioning | growth | cro | analytics | retention | strategy
  title TEXT NOT NULL,
  summary TEXT,                  -- 200-char human summary
  content JSONB NOT NULL DEFAULT '{}',  -- full structured data
  tags TEXT[] DEFAULT '{}',      -- e.g. ["meta-ads", "creative-testing", "roas"]
  levers TEXT[] DEFAULT '{}',    -- driver-tree nodes: ["CVR", "AOV", "Sessions"]
  expert_handle TEXT,            -- schwartz, ogilvy, dunford, kaushik, etc.
  confidence_weight FLOAT DEFAULT 0.7,  -- 1.0 = peer-reviewed, 0.7 = practitioner, 0.5 = anecdotal
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_refknow_source
  ON reference_knowledge(source);

CREATE INDEX IF NOT EXISTS idx_refknow_category
  ON reference_knowledge(category);

CREATE INDEX IF NOT EXISTS idx_refknow_tags
  ON reference_knowledge USING GIN(tags);

CREATE INDEX IF NOT EXISTS idx_refknow_levers
  ON reference_knowledge USING GIN(levers);

CREATE INDEX IF NOT EXISTS idx_refknow_expert
  ON reference_knowledge(expert_handle)
  WHERE expert_handle IS NOT NULL;

-- ── Client Platform Data ─────────────────────────────────────────
-- Raw aggregated metrics pulled daily from each client's platforms.
-- Each (stream_id, metric_date, granularity) is unique to prevent
-- duplicate rows on re-runs. stream_id format: "Platform-ClientName".

CREATE TABLE IF NOT EXISTS client_platform_data (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  client_id       TEXT NOT NULL,
  client_name     TEXT NOT NULL,
  platform        TEXT NOT NULL,
  stream_id       TEXT NOT NULL,
  metric_date     DATE NOT NULL,
  granularity     TEXT NOT NULL DEFAULT 'daily',
  metrics         JSONB NOT NULL DEFAULT '{}',
  raw_record_count INT DEFAULT 0,
  source_account  TEXT,
  ingestion_type  TEXT DEFAULT 'daily',
  created_at      TIMESTAMPTZ DEFAULT now(),

  UNIQUE(stream_id, metric_date, granularity)
);

CREATE INDEX IF NOT EXISTS idx_cpd_stream
  ON client_platform_data(stream_id, metric_date DESC);

CREATE INDEX IF NOT EXISTS idx_cpd_client
  ON client_platform_data(client_id, platform, metric_date DESC);

CREATE INDEX IF NOT EXISTS idx_cpd_platform
  ON client_platform_data(platform, metric_date DESC);
