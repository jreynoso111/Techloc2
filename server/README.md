# Secure Supabase Proxy

This server keeps `SUPABASE_SERVICE_ROLE_KEY` on the backend and injects frontend Supabase runtime config from environment variables (no hardcoded keys in repo).

## 1) Set environment variables

Run these before starting the server:

```bash
export SUPABASE_URL="https://lnfmogsjvdkqgwprlmtn.supabase.co"
export SUPABASE_ANON_KEY="<your-anon-key>"
export SUPABASE_SERVICE_ROLE_KEY="<your-service-role-key>"
export PORT="8080"
```

You can also copy values from `/.env.example` into a local `.env` file (ignored by git) and export them in your shell.

`SUPABASE_SERVICE_ROLE_KEY` is required for privileged proxy endpoints (`/api/repair-history`, `/api/admin/password-reset`).  
The frontend can still boot with only `SUPABASE_URL` + `SUPABASE_ANON_KEY`.

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
- Do not commit `.env` files or any credential JSON files.
- If the key was shared in chat, rotate it in Supabase Dashboard.

## 4) Admin password reset endpoint

The proxy now exposes:

```text
POST /api/admin/password-reset
```

Requirements:

- `Authorization: Bearer <admin-access-token>`
- Caller must be an active administrator in `profiles`.
- JSON body with `userId` and/or `email`.

Example:

```bash
curl -X POST "http://127.0.0.1:8080/api/admin/password-reset" \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"userId":"<profile-id>","email":"user@techloc.io"}'
```

This triggers Supabase recovery link generation for the target user without exposing passwords.
