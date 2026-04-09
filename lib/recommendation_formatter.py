"""
BrightMatter Recommendation Formatter

Deterministic template-based converter: raw SemanticPattern dicts
from Supabase -> actionable Recommendation dicts matching the schema
that client repos (MH-OS, DTC-OS) read from Airtable.

Output schema per recommendation:
    source:     "BrightMatter"
    type:       RecommendationType string
    summary:    plain English, <=120 chars
    details:    markdown with evidence + next step
    channels:   list of channel names
    confidence: "High" | "Medium" | "Low"
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

RECOMMENDATION_TYPES = {
    "budget_shift", "pause_campaign", "scale_campaign", "creative_test",
    "flow_optimization", "pricing_change", "inventory_alert",
    "seo_opportunity", "cro_fix", "retention_action", "marketplace_action",
    "other",
}

_DOMAIN_TO_DEFAULT_TYPE: Dict[str, str] = {
    "campaign": "budget_shift",
    "content": "creative_test",
    "revenue": "scale_campaign",
    "health": "retention_action",
    "generic": "other",
}

_ACTION_TO_TYPE: Dict[str, str] = {
    "reduce_budget": "budget_shift",
    "increase_budget": "budget_shift",
    "shift_budget": "budget_shift",
    "reallocate": "budget_shift",
    "pause": "pause_campaign",
    "stop": "pause_campaign",
    "scale": "scale_campaign",
    "increase_spend": "scale_campaign",
    "test_creative": "creative_test",
    "new_creative": "creative_test",
    "optimize_flow": "flow_optimization",
    "adjust_timing": "flow_optimization",
    "adjust_flow": "flow_optimization",
    "pricing": "pricing_change",
    "discount": "pricing_change",
    "inventory": "inventory_alert",
    "stock": "inventory_alert",
    "seo": "seo_opportunity",
    "cro": "cro_fix",
    "conversion": "cro_fix",
    "retention": "retention_action",
    "churn": "retention_action",
    "reactivate": "retention_action",
    "marketplace": "marketplace_action",
}

_PLATFORM_TO_CHANNEL: Dict[str, str] = {
    "google_ads": "Google",
    "google": "Google",
    "meta_ads": "Meta",
    "meta": "Meta",
    "facebook": "Meta",
    "klaviyo": "Klaviyo",
    "hubspot": "HubSpot",
    "shopify": "Shopify",
    "braze": "Braze",
    "iterable": "Iterable",
    "customer_io": "Customer.io",
    "amplitude": "Amplitude",
    "appsflyer": "AppsFlyer",
    "tiktok_ads": "TikTok",
    "linkedin_ads": "LinkedIn",
    "beehiiv": "Beehiiv",
    "triple_whale": "Triple Whale",
}


def format_pattern(pattern: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a raw semantic_patterns row into an actionable recommendation.

    Returns dict with keys: source, type, summary, details, channels, confidence.
    """
    domain = pattern.get("domain", "generic")
    skill = pattern.get("skill_name", "")
    condition = pattern.get("condition", {}) or {}
    recommendation = pattern.get("recommendation", {}) or {}
    conf_float = pattern.get("confidence", 0.5)
    evidence = pattern.get("evidence_count", 0)
    successes = pattern.get("successes", 0)

    if isinstance(condition, str):
        try:
            condition = json.loads(condition)
        except (json.JSONDecodeError, TypeError):
            condition = {}
    if isinstance(recommendation, str):
        try:
            recommendation = json.loads(recommendation)
        except (json.JSONDecodeError, TypeError):
            recommendation = {}

    rec_type = _infer_type(domain, recommendation)
    channels = _extract_channels(domain, condition, skill)
    summary = _build_summary(domain, skill, condition, recommendation, channels)
    details = _build_details(
        domain, skill, condition, recommendation,
        conf_float, evidence, successes, pattern.get("pattern_id", ""),
    )
    confidence = _confidence_label(conf_float)

    return {
        "source": "BrightMatter",
        "type": rec_type,
        "summary": summary,
        "details": details,
        "channels": channels,
        "confidence": confidence,
        "pattern_id": pattern.get("pattern_id", ""),
    }


def format_patterns(patterns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Batch convert patterns to recommendations."""
    results = []
    for p in patterns:
        try:
            results.append(format_pattern(p))
        except Exception as e:
            logger.warning(f"Failed to format pattern {p.get('pattern_id', '?')}: {e}")
    return results


def _infer_type(domain: str, recommendation: Dict[str, Any]) -> str:
    action = str(recommendation.get("action", "")).lower()
    for keyword, rec_type in _ACTION_TO_TYPE.items():
        if keyword in action:
            return rec_type
    return _DOMAIN_TO_DEFAULT_TYPE.get(domain, "other")


def _extract_channels(
    domain: str, condition: Dict[str, Any], skill: str,
) -> List[str]:
    channels: List[str] = []
    platform = condition.get("platform", "")
    if platform:
        ch = _PLATFORM_TO_CHANNEL.get(platform.lower())
        if ch and ch not in channels:
            channels.append(ch)

    for key in ("channel", "ad_platform", "source"):
        val = condition.get(key, "")
        if val:
            ch = _PLATFORM_TO_CHANNEL.get(val.lower())
            if ch and ch not in channels:
                channels.append(ch)

    if not channels:
        for token in (skill.lower(), domain.lower()):
            ch = _PLATFORM_TO_CHANNEL.get(token)
            if ch and ch not in channels:
                channels.append(ch)

    return channels


def _build_summary(
    domain: str,
    skill: str,
    condition: Dict[str, Any],
    recommendation: Dict[str, Any],
    channels: List[str],
) -> str:
    """Build a plain-English summary, max ~120 chars."""
    action_str = _action_phrase(recommendation)
    context_str = _condition_phrase(condition)
    channel_prefix = ", ".join(channels) if channels else domain.replace("_", " ").title()

    if context_str and action_str:
        summary = f"{channel_prefix}: {context_str} -- {action_str}"
    elif action_str:
        summary = f"{channel_prefix}: {action_str}"
    elif context_str:
        summary = f"{channel_prefix}: {context_str}"
    else:
        summary = f"{channel_prefix}: review {skill.replace('_', ' ')} pattern"

    if len(summary) > 120:
        summary = summary[:117] + "..."
    return summary


def _action_phrase(recommendation: Dict[str, Any]) -> str:
    for key in ("summary", "action", "description"):
        val = recommendation.get(key)
        if val and isinstance(val, str):
            phrase = val.replace("_", " ")
            return phrase[0].upper() + phrase[1:] if phrase else ""

    target = recommendation.get("target_metric", "")
    action = recommendation.get("action", "")
    if target and action:
        return f"{action.replace('_', ' ')} based on {target.replace('_', ' ')}"
    return ""


def _condition_phrase(condition: Dict[str, Any]) -> str:
    parts: List[str] = []

    for metric_key in ("cpc", "cpa", "roas", "ctr", "open_rate", "click_rate",
                        "conversion_rate", "bounce_rate", "spend", "revenue"):
        val = condition.get(metric_key)
        if val is None:
            continue
        label = metric_key.upper().replace("_", " ")
        if isinstance(val, dict):
            lo, hi = val.get("min"), val.get("max")
            if lo is not None and hi is not None:
                parts.append(f"{label} ${lo:.2f}-${hi:.2f}" if metric_key in ("cpc", "cpa", "spend")
                             else f"{label} {lo}-{hi}")
            elif lo is not None:
                parts.append(f"{label} >= {lo}")
        elif isinstance(val, (int, float)):
            parts.append(f"{label} {val}")

    range_keys = [k for k in condition if k.endswith("_range")]
    for rk in range_keys:
        val = condition[rk]
        label = rk.replace("_range", "").upper().replace("_", " ")
        if isinstance(val, dict):
            lo, hi = val.get("min"), val.get("max")
            if lo is not None and hi is not None:
                parts.append(f"{label} {lo}-{hi}")

    campaign_type = condition.get("campaign_type") or condition.get("flow_type")
    if campaign_type:
        parts.insert(0, campaign_type.replace("_", " "))

    return ", ".join(parts[:3])


def _build_details(
    domain: str,
    skill: str,
    condition: Dict[str, Any],
    recommendation: Dict[str, Any],
    confidence: float,
    evidence: int,
    successes: int,
    pattern_id: str,
) -> str:
    """Build markdown details block."""
    lines: List[str] = []

    lines.append(f"**Pattern** `{pattern_id}` | **Domain** {domain} | **Skill** {skill}")
    lines.append("")

    cond_phrase = _condition_phrase(condition)
    if cond_phrase:
        lines.append(f"**Detected:** {cond_phrase}")

    success_rate = (successes / evidence * 100) if evidence > 0 else 0
    lines.append(
        f"**Evidence:** {evidence} observations, "
        f"{successes} successes ({success_rate:.0f}%), "
        f"{confidence:.0%} confidence"
    )

    action = _action_phrase(recommendation)
    if action:
        lines.append(f"\n**Recommended action:** {action}")

    target = recommendation.get("target_metric", "")
    if target:
        lines.append(f"**Target metric:** {target.replace('_', ' ')}")

    skill_suggestion = _suggest_skill(domain, recommendation)
    if skill_suggestion:
        lines.append(f"\n**Suggested skill/module:** `{skill_suggestion}`")

    return "\n".join(lines)


def _suggest_skill(domain: str, recommendation: Dict[str, Any]) -> Optional[str]:
    """Map domain + action to a suggested MH1 skill."""
    action = str(recommendation.get("action", "")).lower()

    suggestions: Dict[str, str] = {
        "reduce_budget": "ads-budget-optimizer",
        "increase_budget": "ads-budget-optimizer",
        "shift_budget": "ads-budget-optimizer",
        "reallocate": "ads-budget-optimizer",
        "pause": "ads-account-audit-google",
        "test_creative": "ad-hook-extractor",
        "new_creative": "ad-hook-extractor",
        "optimize_flow": "lifecycle-audit",
        "adjust_timing": "lifecycle-audit",
        "retention": "at-risk-detection",
        "churn": "churn-prediction",
        "reactivate": "reactivation-detection",
        "cro": "page-cro",
        "conversion": "page-cro",
        "seo": "programmatic-seo",
        "pricing": "pricing-strategy",
    }
    for keyword, skill_name in suggestions.items():
        if keyword in action:
            return skill_name

    domain_fallback: Dict[str, str] = {
        "campaign": "ads-budget-optimizer",
        "content": "ad-hook-extractor",
        "health": "lifecycle-audit",
        "revenue": "pipeline-analysis",
    }
    return domain_fallback.get(domain)


def _confidence_label(conf: float) -> str:
    if conf >= 0.85:
        return "High"
    if conf >= 0.7:
        return "Medium"
    return "Low"
