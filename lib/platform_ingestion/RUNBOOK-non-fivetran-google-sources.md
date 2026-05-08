# Runbook — Wiring GSC + YouTube Analytics for a New Client (non-Fivetran path)

When a client grants `ai@marketerhire.com` only User-level access on Google
Search Console or Editor-level on YouTube — both insufficient for Fivetran's
Connect Card flow (which needs Owner / Manager) — route the source through
`mh-brain`'s platform_ingestion adapters instead.

## Prerequisites

- mh-brain repo cloned, Python deps installed (`pip install -e .`)
- Google OAuth client with these scopes enabled in GCP console:
  - `webmasters.readonly`
  - `yt-analytics.readonly`
  - `youtube.readonly` (just for channel-id discovery)

  Reusing the existing `GOOGLE_ADS_CLIENT_ID` works if the client is in
  "In production" status on the OAuth consent screen and has these scopes
  added. Otherwise create a new "Desktop app" OAuth client.

## Steps

### 1. Capture the refresh token

```bash
cd /Applications/MH1/mh-brain
export GOOGLE_OAUTH_CLIENT_ID="..."
export GOOGLE_OAUTH_CLIENT_SECRET="..."
python scripts/capture_google_refresh_token.py --scopes gsc,youtube --client-slug mindrx
```

Browser opens, log in as `ai@marketerhire.com`, click Allow on both scope
prompts. The script prints `refresh_token` and (if youtube was in scopes)
the `channel_id`.

### 2. Write the token

**Option A — per-client (preferred for client-specific OAuth principals):**

Edit `mh1-hq/clients/{slug}/config/datasources.json`. Set
`integrations.gsc.refresh_token` and `integrations.youtube_analytics.refresh_token`.
Replace the `[TBD ...]` placeholder; also fill in `channel_id` from the
script output.

**Option B — global Modal secret (when the same OAuth principal serves all
clients, which is the current MH-1 default):**

```bash
modal secret create mh-brain-google-oauth \
    GSC_REFRESH_TOKEN="..." \
    YT_REFRESH_TOKEN="..." \
    GOOGLE_OAUTH_CLIENT_ID="..." \
    GOOGLE_OAUTH_CLIENT_SECRET="..."
```

Then redeploy mh-brain so the Modal app picks up the secret:

```bash
modal deploy modal_app.py
```

### 3. Register the dataset + per-client config in mh1-middleware-sdk

mh-brain's orchestrator now fetches per-client dataset config from the
middleware first (`MH1_API_URL/api/clients/{id}/data/datasets`), with
Firebase + filesystem as fallbacks for clients that haven't migrated.

Edit `_CUSTOM_SYNC_DATASETS` in
`mh1-middleware-sdk/app/data_middleware.py` and add an entry for the
client. The new pattern includes a `config` block carrying the
adapter-specific fields (`site_url` for GSC, `channel_id` for YouTube):

```python
"mindrxgroup.com": [
    {"dataset": "mindrx_google_search_console",
     "platform": "Google Search Console",
     "config": {"site_url": "sc-domain:mindrxgroup.com"}},
    {"dataset": "mindrx_youtube_analytics",
     "platform": "YouTube Analytics",
     "config": {"channel_id": "UC..."}},
    {"dataset": "marts_mindrx", "platform": "dbt_marts"},
],
```

Reference PR: https://github.com/MarketerHire/mh1-middleware-sdk/pull/5
(adds the `config` field). Open a PR against `mh1-middleware-sdk`, get it
merged, then redeploy:

```bash
cd /Applications/MH1/mh1-middleware-sdk
modal deploy app/data_middleware.py
```

mh-brain converts the middleware response back to the legacy
`datasources.json` shape internally (see
`orchestrator._middleware_response_to_datasources`), so the rest of the
ingestion pipeline (`detect_platforms`, `resolve_config`) stays
unchanged. **No Firebase write is required** when the client is in
`_CUSTOM_SYNC_DATASETS`.

If you can't update the middleware (or for local dev), the orchestrator
still falls through to the legacy Firebase doc and finally
`mh1-hq/clients/{slug}/config/datasources.json`.

### 3a. Confirm mh-brain has middleware credentials

The Modal app reads `MH1_API_URL` and `MH1_API_KEY` from the
`mh1-shared-tools` secret (already mounted on `bm_secrets` in
`modal_app.py`). If they're missing, create or extend the secret:

```bash
modal secret create mh1-shared-tools \
    MH1_API_URL="https://marketerhire--mh1-data-middleware-serve.modal.run" \
    MH1_API_KEY="op_brightmatter_xxxxxxxxxxxx"
# or, if it already exists:
modal secret update mh1-shared-tools \
    MH1_API_URL="https://marketerhire--mh1-data-middleware-serve.modal.run" \
    MH1_API_KEY="op_brightmatter_xxxxxxxxxxxx"
```

Generate the operator key for mh-brain via
`mh1-middleware-sdk/bin/operators create brightmatter`.

### 4. Trigger a backfill

```bash
cd /Applications/MH1/mh-brain
python scripts/backfill_platforms.py --client {client-name} --platform gsc --years 1
python scripts/backfill_platforms.py --client {client-name} --platform youtube_analytics --years 1
```

Use `--dry-run` first to verify the orchestrator detects the platform and
resolves credentials before pulling data.

### 5. Verify in BigQuery

```bash
bq ls moe-platform-479917:{client_slug}_google_search_console
bq query --use_legacy_sql=false \
    'SELECT COUNT(*) FROM `moe-platform-479917.{client_slug}_google_search_console.daily_metrics` WHERE date >= CURRENT_DATE() - 30'
```

Expect non-zero rows. If the table doesn't exist, check the orchestrator
logs in Modal — most failures are credential-resolution issues.

### 6. Confirm middleware reports the source as connected

After step 3 (registering in `_CUSTOM_SYNC_DATASETS`) the middleware will
return the new dataset to consumers querying `/api/clients/{id}/data/datasets`.
Sanity-check with:

```bash
curl -s -H "x-api-key: $MH1_API_KEY" \
    "$MH1_API_URL/api/clients/{client-domain}/data/datasets" | jq .
```

You should see your new entry in `datasets[]` with the `service` label
and `config` block intact.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `gsc: missing OAuth credentials for {name}` | env vars + per-client `refresh_token` both empty | Set Modal secret OR per-client field |
| `gsc: no site_url for {name}` | datasources.json missing `gsc.site_url` | Add `"sc-domain:{domain}"` (Domain property) or `"https://{domain}/"` (URL prefix) |
| `youtube_analytics: no channel_id` | datasources.json missing `youtube_analytics.channel_id` | Run capture script with `--scopes youtube` to discover; format `UC...` |
| OAuth flow returns no refresh_token | OAuth grant already issued for these scopes | Revoke at https://myaccount.google.com/permissions, retry |
| 403 on first sync | scope insufficient for that endpoint | Confirm GSC property allows the OAuth user (User-level is enough); for YouTube confirm Editor or Manager |
| Tokens stop working after 7 days | OAuth consent screen in "Testing" mode | Move to "In production" in GCP console |

## Reference

- mh-brain adapter: `lib/platform_ingestion/adapters/gsc.py`, `youtube_analytics.py`
- Config resolver: `lib/platform_ingestion/config_resolver.py` (search "elif platform == \"gsc\"")
- TS reference for API call shape: `/Applications/MH1/mh-os/dashboard/api/search-console.ts`
- Capture script: `mh-brain/scripts/capture_google_refresh_token.py`
- Decision criteria for Fivetran vs. mh-brain routing: see `mh-data-plane/docs/SOURCE-ROUTING.md` (TBD; non-Fivetran route is correct when client cannot or will not grant Owner/Manager)
