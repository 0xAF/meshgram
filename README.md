# 🌐 meshgram-plus: Bridging Meshtastic and Telegram 🚀

Connect your Meshtastic mesh network with Telegram group chats! 📡💬

## 🌟 Features

- 🔌 Serial and TCP support with auto‑reconnect
- 📬 Reliable delivery: outbound queue, retries, ACK tracking, chunking (send delay + truncation notice)
- 📨 Telegram: commands, topics/threads, reactions, locations, optional message forwarding
- 🛰️ Mesh commands: /ping, /help, /travel, /ai, /aireset, /admin (via admin_nodes)
- 🧠 AI chat: Ollama or OpenAI; local tool (weather script), system prompt, chain‑of‑thought stripping, threaded replies
- � AI chat: Ollama or OpenAI (incl. Cloudflare Workers AI via Responses API); local tool (weather script), system prompt, chain‑of‑thought stripping, threaded replies
- �🧩 Triggers engine: regex replace/prepend (mesh + Telegram) and reply (mesh) with placeholders (signal, RSSI/SNR, hops, MQTT, channel)
- ✈️ Travel reply template with placeholders
- 🔀 Per‑channel control: channel names, ignored_channels, receive_only_channels; default node/channel targeting
- ✉️ BBS (beta): DM‑only private messages with 200B limit, queued delivery, nudges on appearance, and first‑seen recipient selection
- ⚙️ Layered configuration with env vars: split files auto‑discovered from `config/` (ignores `example.*`), optional minimal root `config/config.yaml` and `config/config.local.yaml`, plus secrets via environment variables

## 🛠 Requirements

- Python 3.11+ 🐍

1. **Clone the project:**

  ```bash
  git clone https://github.com/0xAF/meshgram-plus
  cd meshgram-plus
  ```

1. **Set up a virtual environment (or use Make):**

  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```

1. **Install dependencies:**

  ```bash
  pip install -r requirements.txt
  ```

1. **Configure (split files):**

  ```bash
  # minimal root config (optional)
  printf "config_version: 2\ntelemetry:\n  environment_enabled: false\n  environment_script: ./data/ha.sh\n  environment_send_interval: 300\n" > config/config.yaml

  # copy split examples as needed (drop the example. prefix)
  cp config/example.telegram.yaml    config/telegram.yaml
  cp config/example.meshtastic.yaml  config/meshtastic.yaml
  cp config/example.channels.yaml    config/channels.yaml
  cp config/example.logging.yaml     config/logging.yaml
  cp config/example.telemetry.yaml   config/telemetry.yaml
  cp config/example.ai.yaml          config/ai.yaml
  cp config/example.bbs.yaml         config/bbs.yaml
  cp config/example.triggers.yaml    config/triggers.yaml
  $EDITOR config/*.yaml
  ```

1. **Run (or use Make):**

  ```bash
  # via Python
  python src/meshgram.py
  # or via Make
  make run
  ```

On Linux using a serial device, ensure your user can access the port (e.g. /dev/ttyUSB0): add your user to the dialout group and re‑login: `sudo usermod -a -G dialout "$USER"`.

## ⚙️ Configuration

Split example files (copy and adapt):

- `config/example.telegram.yaml` → `config/telegram.yaml`: bot token, chat id, topics/threads, optional triggers (from example.triggers.yaml), AI toggle
- `config/example.meshtastic.yaml` → `config/meshtastic.yaml`: serial/tcp device, default node id, send chunking, health policy
- `config/example.triggers.yaml` → `config/triggers.yaml`: regex rules and `meshtastic.travel_template` for the /travel reply
- `config/example.channels.yaml` → `config/channels.yaml`: channel names, reports, topics, and top‑level `default_channel_id`, `ignored_channels`, `receive_only_channels`
- `config/example.triggers.yaml` → `config/triggers.yaml`: regex rules with YAML‑escaped backslashes (e.g., "\\b"), ops `replace|prepend|reply` (reply on mesh only)
- `config/example.logging.yaml` → `config/logging.yaml`: per‑lib levels, syslog/file options
- `config/example.ai.yaml` → `config/ai.yaml`: provider (ollama/openai), model/base_url, system prompt, tools
- `config/example.bbs.yaml` → `config/bbs.yaml`: BBS/private message settings
- `config/example.telemetry.yaml` → `config/telemetry.yaml`: telemetry runner settings

### Quickstart (layered config + env)

1) Copy `.env.example` to `.env` and fill in secrets:
   - `TELEGRAM_BOT_TOKEN`, `OPENAI_API_KEY` (if using OpenAI/Cloudflare)

2) Create a minimal `config/config.yaml` with only overrides (optional):

```yaml
config_version: 1
telegram:
  bot_token: "${TELEGRAM_BOT_TOKEN}"
  chat_id: -1001234567890
meshtastic:
  connection_type: tcp
  device: "192.168.1.100:4403"
  default_node_id: "^all"
  default_channel_id: 0
  on_disconnect: exit
  health_max_failures: 3
bbs:
  enabled: true
```

Auto‑discovery loader:

- Loads `config/config.yaml` (optional) and overlays `config/config.local.yaml` (optional, gitignored).
- Then loads all other `*.yaml` files in `config/` automatically (any order).
- Files starting with `example.` are ignored by the loader.
- Special handling:
  - `telegram.triggers` and `meshtastic.triggers` are merged without clobbering other keys.
  - If a file defines `channels`, `reports`, `topics`, or top‑level `default_channel_id`, `ignored_channels`, `receive_only_channels`, they’re also exposed at top level for convenience.

The app validates the configuration at startup and reports clear errors for missing or invalid fields.

### AI providers and Cloudflare notes

You can choose between a local Ollama instance or an OpenAI‑compatible endpoint.

- Ollama: simple to try locally.
  - `ai.provider: ollama`
  - `ai.ollama.base_url: http://127.0.0.1:11434`
  - `ai.ollama.model: llama3`

- OpenAI/Cloudflare (Responses API):
  - `ai.provider: openai`
  - For Cloudflare Workers AI, set your account Responses endpoint as `ai.openai.base_url`, e.g.:
    - `https://api.cloudflare.com/client/v4/accounts/<account_id>/ai/v1`
  - Set a model available on Cloudflare, e.g.: `@cf/openai/gpt-oss-120b`
  - Enable the Responses API switch: `ai.openai.use_responses_api: true`
  - Provide `ai.openai.api_key` (your CF API Token with Workers AI permissions) via env.

Tool support auto‑detection:

- Some models don’t support tool/function calls. We auto‑detect this from provider errors
  (e.g., Cloudflare invalid_prompt Unknown_recipient) and remember per model to stop
  sending tools next time. This removes repeated 400s and speeds up replies.

Weather fallback without tools:

- If you ask for weather and the current model doesn’t support tools, we’ll inject a compact
  local weather context (via your `telemetry.environment_script`) so the model can still answer.
  If the provider still errors, we’ll return the local weather summary directly instead of an error.

Telegram empty‑message guard:

- Telegram refuses empty text. We guard against that and fall back to a minimal non‑empty
  reply if a model returned nothing after stripping internal “thinking”.

Troubleshooting:

- Cloudflare 400 invalid_prompt with “Unknown_recipient: …” indicates the model doesn’t support tools.
  We’ll auto‑disable tools for that model. You’ll see logs like:
  - `[openai_http_retry_cf_string_input]` – retrying with plain string input
  - `[openai_disable_tools_for_model]` – caching that tools are off for this model
  - `[openai_http_error]` – HTTP error details (status + response snippet)
- Ensure `telemetry.environment_script` points to a script that prints key: value lines. The weather
  helper parses that output and builds a summary (see `config/example.telemetry.yaml`).

### Makefile targets

For a smoother developer workflow, use the provided Makefile:

```bash
# create venv and install requirements
make install

# run tests
make test

# run the app
make run

# lint and format (requires ruff; installed on first run if present in venv)
make lint
make format

# docker compose helpers
make compose-build
make compose-up
make compose-logs
make compose-down
```
  
## 🐳 Docker (compose)

Run the bot in a container with Docker Compose (includes serial device access and persistence):

1. Prepare config and data directory

  ```bash
  # from the repo root
  mkdir -p data
  printf "config_version: 2\ntelemetry:\n  environment_enabled: false\n  environment_script: ./data/ha.sh\n  environment_send_interval: 300\n" > config/config.yaml
  cp config/example.telegram.yaml    config/telegram.yaml
  cp config/example.meshtastic.yaml  config/meshtastic.yaml
  cp config/example.channels.yaml    config/channels.yaml
  cp config/example.logging.yaml     config/logging.yaml
  cp config/example.ai.yaml          config/ai.yaml
  cp config/example.bbs.yaml         config/bbs.yaml
  cp config/example.triggers.yaml    config/triggers.yaml
  $EDITOR config/*.yaml
  ```

Tips:

- If using a serial device, set in `config/meshtastic.yaml`:
  - `meshtastic.connection_type: serial`
  - `meshtastic.device: "/dev/ttyUSB0"` (or your actual path; stable options under `/dev/serial/by-id/*`)
- Ensure the same device path is mapped in `docker-compose.yml` under `services.meshgram-plus.devices`.
  - Optional file logging: set `logging.file_log: true` to write `./data/meshgram.log` (inside container: `/app/data/meshgram.log`).

1. Build and run

  ```bash
  docker compose build
  docker compose up -d
  ```

1. View logs and stop

  ```bash
  docker compose logs -f
  docker compose down
  ```

Notes

- The compose file mounts:
  - `./config/` into the container as `/app/config/` (read-only) so all split YAML files are available
  - `./data/` into the container as `/app/data/` for SQLite databases and logs
- Environment variables referenced in config (e.g., `${TELEGRAM_BOT_TOKEN}`) can be provided via the `environment:` section in `docker-compose.yml` or your shell.
- On Linux, ensure your user has permissions to the serial device (often group `dialout`), and that the device path exists before starting the container.

## 📡 Telegram Commands

- `/start` – See available commands
- `/help` – Help message
- `/status` – Current status
- `/node [node_id]` – Node info
- `/bell [node_id]` – Bell a node
- `/user` – Your Telegram user info
- `/enable <feature>` / `/disable <feature>` – Feature toggles (`forwarding`, `telemetry`, `location`, `nodes`, `ait`, `aim`)
- `/features` – Show feature flags
- `/listnodes` – List known nodes
- `/ai <prompt>` – Ask the AI (if enabled)
- `/aireset` – Reset your AI context
- `/aidiagnose` – Show AI provider/model, tool support cache, Responses API mode, and a tiny round-trip test (aliases: /aiinfo, /ai_diag)
- `/aidiagnose` – Show AI provider/model, tool support cache, Responses API mode, and a tiny round-trip test (aliases: /aiinfo, /ai_diag)

### 🔍 AI diagnose

Use `/aidiagnose` in Telegram to print helpful runtime info:

- Provider and whether it’s Cloudflare
- Model name and base_url
- Responses API on/off (for OpenAI‑compatible)
- Cached tool support for the current model (if we detected unsupported tools)
- Environment script configured or not
- A tiny round‑trip test so you can see that the endpoint responds

Aliases: `/aiinfo`, `/ai_diag`.


## 🛰️ Mesh Commands

Send slash commands as normal text from the Meshtastic Text Message App:

- `/ping`, `/help`, `/travel`, `/ai <prompt>`, `/aireset`, `/admin <cmd>` (for admin_nodes)

Notes: `meshtastic.reply_directly` controls DM vs channel replies; `receive_only_channels` forwards to Telegram but skips triggers and replies on mesh.

## ✉️ BBS: Private Messages (beta)

BBS provides DM-only, store-and-forward private messages with strict 200-byte limits for mesh packets. Messages are always queued first; recipients are nudged to fetch them with `!mi` instead of receiving content automatically.

How it works:

- DM the bot with `!ms <target> <message>` to queue a PM.
  - Target can be a `!nodeId` or a node short name (case-insensitive exact match).
  - If there’s exactly one match, the recipient is finalized immediately and gets a nudge: "You have N PM(s). Send !mi".
  - If there are multiple matches, you’ll be shown a numbered list. Reply with:
    - `!ms N` to choose a specific recipient, or
    - `!ms 0` to enable first-seen: the first matching node that appears online will be selected automatically and nudged.
- When a recipient appears (any non-ringtone packet) and messages are first-seen enabled for them, the recipient is finalized and nudged. Content is never auto-sent; recipients fetch with `!mi`.
- When a recipient runs `!mi`, messages are marked as delivered (for sender outbox visibility). Inbox shows statuses only as `unread`/`read`.

Constraints and behavior:

- 200-byte limit per mesh packet. The message text you send is capped at 200 UTF‑8 bytes. Inbox read replies use single packets when they fit; otherwise, header and body are sent as two 200B packets.
- Inbox/Outbox listings are session-based: actions use indices from the last list (`!mi`/`!mo`).
- Reading (`!mr N`) marks as read even if a mesh ACK isn't observed.
- Outbox shows status as `queued`, `sent` (recipient ran `!mi`), or `read`. Inbox shows `unread`/`read` only.

Commands (DM to the bot):

- `!h` – BBS help overview
- `!hm` – Help for private messages
- `!ms <shortname|!nodeId> <message>` – Queue a PM (200B max)
- `!mi` – List inbox (date, from short/long/!id, status read/unread)
- `!mr N` – Read inbox message N (alias: `!mri N`)
- `!mdi N` – Delete inbox message N (alias: `!md N`)
- `!mo [-a]` – List outbox (use `-a` to include deleted/history)
- `!mro N` – Show content of outbox message N
- `!mdo N` – Delete/unsend outbox message N
- `!moa` – Delete all read outbox messages

Tips:

- First-seen flow: after `!ms <name> <msg>` with multiple matches, reply `!ms 0` to deliver to the first candidate seen online; otherwise, pick a specific one with `!ms N`.
- Outbox entries targeting first-seen show candidate node IDs in parentheses and a `[first-seen]` tag until finalized.
- Non-command DMs to the bot return a short BBS help tip.

Config (excerpt):

```yaml
bbs:
  enabled: true
  send_message_expire_days: 7
  outbox:
    max_per_sender: 10
  notify:
    cooldown_hours: 6   # general nudge cooldown; first-seen finalization bypasses once
  session_index_ttl_seconds: 180
```


## 📜 Project history and credits

Originally started as “meshgram” by Tom Hansel. The original repo: <https://github.com/gretel/meshgram>.

Huge thanks to Tom for the great foundation and all his work on the original project.
