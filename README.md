# PortPing VI

Free, account-less Discord alerts for GTA VI PC port progress. People paste a Discord
webhook on the site; the server polls the PortWatch VI feed and posts an embed to every
webhook when something changes.

Zero dependencies — Python 3.10+ standard library only (HTTP server, SQLite, poller in one process).
Discord messages are sent in each subscriber's chosen language (strings live in `i18n.py`); the site itself is English.

## Run locally

```bash
python app.py            # http://localhost:8080
```

## Deploy (Railway)

1. Push this folder to a GitHub repo and create a service from it (the Dockerfile is picked up automatically).
2. Add a **Volume** mounted at `/data` — subscribers and state live in `/data/portping.db`.
3. Generate a domain and set `SITE_URL=https://your-domain` (shown in Discord footers).

## Env vars

| Var | Default | |
|---|---|---|
| `PORT` | 8080 | set by Railway |
| `DATA_DIR` | `./data` (`/data` in Docker) | SQLite location |
| `SITE_URL` | `http://localhost:8080` | public URL |
| `CHECK_INTERVAL_MIN` | 10 | feed poll interval |
| `MIN_SCORE_DELTA` | 0.1 | min % change to report |
| `BROADCAST_WORKERS` | 5 | parallel webhook senders |
| `AVATAR_URL` | `SITE_URL/avatar.png` | png/jpg avatar for messages |

## API

- `POST /api/subscribe` `{url, mode: "all" | "major", lang}` — validates the webhook with Discord, posts a confirmation, stores it.
  `lang` is one of `en pt-BR es fr de it pl ru tr` (anything else falls back to English); the site defaults it to the browser language.
- `POST /api/unsubscribe` `{url}`
- `GET /api/status` — leaderboard, stats, recent broadcasts
- `GET /healthz`

Webhooks returning 401/403/404 (deleted in Discord) are removed automatically; ones that
fail 20 times in a row are dropped too. First run only saves a baseline (no broadcast).
