# Secure Supabase Proxy

This server keeps `SUPABASE_SERVICE_ROLE_KEY` on the backend and never exposes it to the browser.

## 1) Set environment variables

Run these before starting the server:

```bash
export SUPABASE_URL="https://lnfmogsjvdkqgwprlmtn.supabase.co"
export SUPABASE_ANON_KEY="<your-anon-key>"
export SUPABASE_SERVICE_ROLE_KEY="<your-service-role-key>"
export PORT="8080"
```

Optional:

```bash
export REPAIR_HISTORY_ALLOWED_ROLES="administrator,moderator"
```

Leave `REPAIR_HISTORY_ALLOWED_ROLES` empty to allow any authenticated user.
If it was already set in your shell, run `unset REPAIR_HISTORY_ALLOWED_ROLES` before starting the proxy.

This proxy is hard-locked to project ref `lnfmogsjvdkqgwprlmtn` and will refuse another Supabase host/key ref.

## 2) Start server

From project root:

```bash
node ./server/secure-supabase-proxy.mjs
```

Open:

```text
http://127.0.0.1:8080
```

## 3) Security notes

- Do not commit `SUPABASE_SERVICE_ROLE_KEY` to git.
- Do not place `SUPABASE_SERVICE_ROLE_KEY` in frontend files.
- If the key was shared in chat, rotate it in Supabase Dashboard.
