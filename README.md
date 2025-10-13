# 🌐 meshgram-plus: Bridging Meshtastic and Telegram 🚀

Connect your Meshtastic mesh network with Telegram group chats! 📡💬

## 🌟 Features

- 🔌 Serial and TCP support with auto‑reconnect
- 📬 Reliable delivery: outbound queue, retries, ACK tracking, chunking (send delay + truncation notice)
- 📨 Telegram: commands, topics/threads, reactions, locations, optional message forwarding
- 🛰️ Mesh commands: /ping, /help, /travel, /ai, /aireset, /admin (via admin_nodes)
- 🧠 AI chat: Ollama or OpenAI; local tool (weather script), system prompt, chain‑of‑thought stripping, threaded replies
- 🧩 Triggers engine: regex replace/prepend (mesh + Telegram) and reply (mesh) with placeholders (signal, RSSI/SNR, hops, MQTT, channel)
- ✈️ Travel reply template with placeholders
- 🔀 Per‑channel control: channel names, ignored_channels, receive_only_channels; default node/channel targeting
- ✉️ BBS (beta): DM‑only private messages with 200B limit, queued delivery, nudges on appearance, and first‑seen recipient selection
- ⚙️ Layered configuration with env vars: minimal overrides in `config/config.yaml`, optional `config/config.local.yaml`, external `config/triggers.yaml` and `config/topics.yaml`, and secrets via environment variables

## 🛠 Requirements

- Python 3.11+ 🐍

1. **Clone the project:**

  ```bash
  git clone https://github.com/0xAF/meshgram-plus
  cd meshgram-plus
  ```

1. **Set up a virtual environment:**

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
  # minimal root config
  printf "config_version: 2\ntelemetry:\n  environment_enabled: false\n  environment_script: ./data/ha.sh\n  environment_send_interval: 300\n" > config/config.yaml

  # copy split examples as needed (drop the example. prefix)
  cp config/example.telegram.yaml    config/telegram.yaml
  cp config/example.meshtastic.yaml  config/meshtastic.yaml
  cp config/example.channels.yaml    config/channels.yaml
  cp config/example.logging.yaml     config/logging.yaml
  cp config/example.ai.yaml          config/ai.yaml
  cp config/example.bbs.yaml         config/bbs.yaml
  cp config/example.triggers.yaml    config/triggers.yaml
  $EDITOR config/*.yaml
  ```

1. **Run:**

  ```bash
  python src/meshgram.py
  ```

On Linux using a serial device, ensure your user can access the port (e.g. /dev/ttyUSB0): add your user to the dialout group and re‑login: `sudo usermod -a -G dialout "$USER"`.

## ⚙️ Configuration

Split example files (copy and adapt):

- `config/example.telegram.yaml` → `config/telegram.yaml`: bot token, chat id, topics/threads, optional triggers (from example.triggers.yaml), AI toggle
- `config/example.meshtastic.yaml` → `config/meshtastic.yaml`: serial/tcp device, default node id, send chunking, travel_template, health policy
- `config/example.channels.yaml` → `config/channels.yaml`: channel names, reports, topics, and top‑level `default_channel_id`, `ignored_channels`, `receive_only_channels`
- `config/example.triggers.yaml` → `config/triggers.yaml`: regex rules with YAML‑escaped backslashes (e.g., "\\b"), ops `replace|prepend|reply` (reply on mesh only)
- `config/example.logging.yaml` → `config/logging.yaml`: per‑lib levels, syslog/file options
- `config/example.ai.yaml` → `config/ai.yaml`: provider (ollama/openai), model/base_url, system prompt, tools
- `config/example.bbs.yaml` → `config/bbs.yaml`: BBS/private message settings

### Quickstart (layered config + env)

1) Copy `.env.example` to `.env` and fill in secrets:
   - `TELEGRAM_BOT_TOKEN`, `OPENAI_API_KEY` (if using OpenAI/Cloudflare)

2) Create a minimal `config/config.yaml` with only overrides:

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

- Loads `config/config.yaml` (required) and overlays `config/config.local.yaml` (optional, gitignored).
- Then loads all other `*.yaml` files in `config/` automatically (any order).
- Files starting with `example.` are ignored by the loader.
- Special handling:
  - `telegram.triggers` and `meshtastic.triggers` are merged without clobbering other keys.
  - If a file defines `channels`, `reports`, `topics`, or top‑level `default_channel_id`, `ignored_channels`, `receive_only_channels`, they’re also exposed at top level for convenience.

The app validates the configuration at startup and reports clear errors for missing or invalid fields.
  
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

- If using a serial device, set in `config/config.yaml`:
  - `meshtastic.connection_type: serial`
  - `meshtastic.device: "/dev/ttyUSB0"` (or your actual path; stable options under `/dev/serial/by-id/*`)
- Ensure the same device path is mapped in `docker-compose.yml` under `services.meshgram.devices`.
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
  - `./config/config.yaml` into the container (read-only)
  - `./data/messages.db`, `./data/cache.db`, and `./data/meshgram.log` for persistence
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
