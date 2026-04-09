"""Resolve platform credentials from Firebase client configs + global env vars."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from .adapters.base import PlatformConfig

logger = logging.getLogger(__name__)

# Platforms whose OAuth / developer tokens live in global env, not per-client
_GLOBAL_CREDENTIAL_PLATFORMS = {"google_ads", "meta_ads"}

# Map from datasources.json integration names → canonical platform keys
_INTEGRATION_ALIASES: Dict[str, str] = {
    "facebook": "meta_ads",
    "facebook_ads": "meta_ads",
    "meta": "meta_ads",
    "meta_ads": "meta_ads",
    "google": "google_ads",
    "google_ads": "google_ads",
    "klaviyo": "klaviyo",
    "hubspot": "hubspot",
    "shopify": "shopify",
    "braze": "braze",
    "iterable": "iterable",
    "beehiiv": "beehiiv",
    "triple_whale": "triple_whale",
    "amplitude": "amplitude",
    "customerio": "customerio",
    "customer.io": "customerio",
    "appsflyer": "appsflyer",
    "tiktok_ads": "tiktok_ads",
    "tiktok": "tiktok_ads",
    "ga4": "ga4",
    "polar_analytics": "polar_analytics",
    "polar": "polar_analytics",
    "ghl": "ghl",
    "gohighlevel": "ghl",
    "go_high_level": "ghl",
    "h_level": "ghl",
    "highlevel": "ghl",
}


def detect_platforms(datasources: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Return (canonical_platform, raw_config) pairs from a datasources.json dict."""
    found: List[Tuple[str, Dict[str, Any]]] = []
    seen = set()

    # 1. Check CRM block
    crm = datasources.get("crm") or {}
    crm_type = (crm.get("type") or "").lower()
    if crm_type and crm_type != "not_configured":
        canon = _INTEGRATION_ALIASES.get(crm_type, crm_type)
        if canon not in seen:
            seen.add(canon)
            found.append((canon, crm))

    # 2. Check primaryPlatform / primary_platform
    for key in ("primaryPlatform", "primary_platform"):
        pp = datasources.get(key) or {}
        pp_type = (pp.get("type") or "").lower()
        if pp_type:
            canon = _INTEGRATION_ALIASES.get(pp_type, pp_type)
            if canon not in seen:
                seen.add(canon)
                found.append((canon, pp))

    # 3. Check ecommerce block
    ecom = datasources.get("ecommerce") or {}
    ecom_type = (ecom.get("type") or "").lower()
    if ecom_type:
        canon = _INTEGRATION_ALIASES.get(ecom_type, ecom_type)
        if canon not in seen:
            seen.add(canon)
            found.append((canon, ecom))

    # 4. Check integrations (dict or list)
    integrations = datasources.get("integrations") or {}
    if isinstance(integrations, dict):
        for name, cfg in integrations.items():
            if not isinstance(cfg, dict):
                continue
            canon = _INTEGRATION_ALIASES.get(name.lower(), name.lower())
            status = (cfg.get("status") or "").lower()
            if status in ("pending", "not_configured", "disabled"):
                continue
            if canon not in seen:
                seen.add(canon)
                found.append((canon, cfg))
    elif isinstance(integrations, list):
        for item in integrations:
            if isinstance(item, str):
                canon = _INTEGRATION_ALIASES.get(item.lower(), item.lower())
                # Look for a top-level block with the actual config
                top_cfg = datasources.get(item) or datasources.get(canon) or {}
                if not isinstance(top_cfg, dict):
                    top_cfg = {}
                if canon not in seen:
                    seen.add(canon)
                    found.append((canon, top_cfg))
            elif isinstance(item, dict):
                itype = (item.get("type") or item.get("name") or "").lower()
                canon = _INTEGRATION_ALIASES.get(itype, itype)
                status = (item.get("status") or "").lower()
                if status in ("pending", "not_configured", "disabled"):
                    continue
                if canon not in seen:
                    seen.add(canon)
                    found.append((canon, item))

    # 5. Check top-level named platform blocks
    for key in ("klaviyo", "meta_ads", "google_ads", "shopify", "ga4",
                "polar_analytics", "outer_signal", "power_bi", "triple_whale",
                "ghl", "gohighlevel", "h_level"):
        if key in datasources and isinstance(datasources[key], dict):
            canon = _INTEGRATION_ALIASES.get(key, key)
            if canon not in seen:
                seen.add(canon)
                found.append((canon, datasources[key]))

    return found


def resolve_config(
    platform: str,
    raw_config: Dict[str, Any],
    client_id: str,
    client_name: str,
    datasources: Dict[str, Any],
) -> Optional[PlatformConfig]:
    """Build a PlatformConfig with resolved credentials."""

    creds: Dict[str, Any] = {}
    account_id: Optional[str] = None

    if platform == "google_ads":
        creds = {
            "developer_token": os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", ""),
            "client_id": os.environ.get("GOOGLE_ADS_CLIENT_ID", ""),
            "client_secret": os.environ.get("GOOGLE_ADS_CLIENT_SECRET", ""),
            "refresh_token": os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", ""),
        }
        account_id = (
            raw_config.get("customer_id")
            or raw_config.get("account_id")
            or ""
        )
        if not account_id:
            logger.debug(f"google_ads: no customer_id for {client_name}")
            return None
        if not creds["developer_token"]:
            logger.debug("google_ads: missing GOOGLE_ADS_DEVELOPER_TOKEN")
            return None

    elif platform == "meta_ads":
        token = (
            raw_config.get("access_token")
            or raw_config.get("token")
            or os.environ.get("META_ACCESS_TOKEN", "")
        )
        # Check for per-client token refs
        ref = raw_config.get("access_token_ref", "")
        if ref and not token:
            token = os.environ.get(ref, "")
        if not token:
            token = os.environ.get("META_ACCESS_TOKEN", "")

        account_id = (
            raw_config.get("ad_account_id")
            or raw_config.get("account_id")
            or ""
        )
        if not account_id:
            logger.debug(f"meta_ads: no ad_account_id for {client_name}")
            return None
        creds = {"access_token": token}

    elif platform == "klaviyo":
        api_key = (
            raw_config.get("api_key")
            or raw_config.get("private_key")
            or os.environ.get("KLAVIYO_API_KEY", "")
        )
        if not api_key:
            logger.debug(f"klaviyo: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key}

    elif platform == "hubspot":
        token = (
            raw_config.get("access_token")
            or raw_config.get("token")
            or os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
        )
        if not token:
            logger.debug(f"hubspot: no access_token for {client_name}")
            return None
        creds = {"access_token": token}

    elif platform == "shopify":
        token = (
            raw_config.get("access_token")
            or os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
        )
        store_url = raw_config.get("store_url") or raw_config.get("store_domain") or ""
        if not token or not store_url:
            logger.debug(f"shopify: missing token or store_url for {client_name}")
            return None
        creds = {"access_token": token, "store_url": store_url}

    elif platform == "braze":
        api_key = raw_config.get("api_key") or os.environ.get("BRAZE_API_KEY", "")
        endpoint = raw_config.get("endpoint") or raw_config.get("cluster") or ""
        if not api_key:
            logger.debug(f"braze: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key, "endpoint": endpoint}

    elif platform == "iterable":
        api_key = raw_config.get("api_key") or os.environ.get("ITERABLE_API_KEY", "")
        if not api_key:
            logger.debug(f"iterable: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key}

    elif platform == "beehiiv":
        api_key = raw_config.get("api_key") or os.environ.get("BEEHIIV_API_KEY", "")
        pub_id = raw_config.get("publication_id") or ""
        if not api_key:
            logger.debug(f"beehiiv: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key, "publication_id": pub_id}

    elif platform == "triple_whale":
        api_key = raw_config.get("api_key") or os.environ.get("TRIPLE_WHALE_API_KEY", "")
        shop = raw_config.get("shop") or ""
        if not api_key:
            logger.debug(f"triple_whale: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key, "shop": shop}

    elif platform == "amplitude":
        api_key = raw_config.get("api_key") or os.environ.get("AMPLITUDE_API_KEY", "")
        if not api_key:
            logger.debug(f"amplitude: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key}

    elif platform == "customerio":
        api_key = (
            raw_config.get("api_key")
            or raw_config.get("app_api_key")
            or os.environ.get("CUSTOMERIO_API_KEY", "")
        )
        if not api_key:
            logger.debug(f"customerio: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key, "base_url": raw_config.get("base_url", "")}

    elif platform == "appsflyer":
        api_key = raw_config.get("api_key") or os.environ.get("APPSFLYER_API_KEY", "")
        app_id = raw_config.get("app_id") or ""
        if not api_key:
            logger.debug(f"appsflyer: no api_key for {client_name}")
            return None
        creds = {"api_key": api_key, "app_id": app_id}

    elif platform == "ghl":
        api_key = (
            raw_config.get("api_key")
            or raw_config.get("pit_key")
            or os.environ.get("GHL_API_KEY", "")
        )
        loc_id = (
            raw_config.get("location_id")
            or raw_config.get("locationId")
            or os.environ.get("GHL_LOCATION_ID", "")
        )
        if not api_key:
            logger.debug(f"ghl: no api_key for {client_name}")
            return None
        if not loc_id:
            logger.debug(f"ghl: no location_id for {client_name}")
            return None
        creds = {"api_key": api_key, "location_id": loc_id}
        account_id = loc_id[:8]

    else:
        logger.debug(f"No resolver for platform: {platform}")
        return None

    return PlatformConfig(
        platform=platform,
        client_id=client_id,
        client_name=client_name,
        credentials=creds,
        account_id=account_id or creds.get("api_key", "")[:8],
        extra=raw_config,
    )
