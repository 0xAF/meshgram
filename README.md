# 🌐 Meshgram: Bridging Meshtastic and Telegram 🚀

Connect your Meshtastic mesh network with Telegram group chats! 📡💬

## 🌟 Features

- 🔌 Supports both serial and TCP connections to Meshtastic devices
- 🔄 Automatic reconnection to Meshtastic device
- 🚦 Message queuing and retry mechanism
- 🔔 Command to send bell notifications to Meshtastic nodes
- 📊 Real-time status updates for nodes (telemetry, position, routing, neighbors)
- 🗺️ Location sharing between Telegram and Meshtastic
- 🔐 User authorization for Telegram commands
- 📝 Optional logging to file and syslog
- Cache learned nodes to cache.db (sqlite)
- Respond to /ping command from mestastic node
- Optional local AI chat via Ollama (/ai on mesh & Telegram)

## 🛠 Requirements

- Python 3.11+ 🐍
- Dependencies:
  - `envyaml`: For YAML configuration file parsing with environment variable support
  - `meshtastic`: Python API for Meshtastic devices
  - `python-telegram-bot`: Telegram Bot API wrapper
  - `pubsub`: For publish-subscribe messaging pattern
  - `sqlitedict`: Lightweight persistent dict used for node cache

## 🚀 Quick Start

1. **Clone the repo:**
   
  ```bash
   git clone https://github.com/gretel/meshgram.git
   cd meshgram
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

1. **Configure the project:**
   
  Create a `config.yaml` file in the `config` directory:

  ```yaml
   telegram:
     bot_token: "your_bot_token_here"
     chat_id: -1001234567890 
     authorized_users:
       - 123456789
     enable_message_forwarding: False # enable/disable message forwarding from telegram to meshtastic default_node_id
     use_topics: False # map meshtastic channels to different topics in Telegram

   meshtastic:
     connection_type: "serial"  # or "tcp"
     device: "/dev/ttyUSB0"  # or "hostname:port" for TCP
     #default_node_id: "!abcdef12" # or "^all"
     default_node_id: "^all"
     default_channel_id: 0 # the mesh channel id to send the messages to
     commands:
       ping: True # /ping command to get your hop count and node id back
     #ignored_channels: # channels to ignore (do not send them to telegram)
     #  - 0
     #  - 1
     #  - 2

   reports:
      telemetry: True
      location: True

   channels:
      - "LongFast" # 0
      - "MySecondaryChannel" # 1

  # set the message_thread_id for each topic
  # read: https://stackoverflow.com/a/75178418/420585 to learn how to get the message_thread_id
   topics:
     telemetry: 907
     location: 909
     nodes: 918
     channel0: 914
     channel1: 911

   telemetry:
     environment_enabled: False
     environment_script: "./ha.sh"
     environment_send_interval: 300  # in seconds, how often to send telemetry data

   logging:
     level: "info"          # root level
     level_telegram: "warn"  # fine-tune noisy libs
     level_httpx: "warn"
     use_syslog: false
     syslog_host: "localhost"
     syslog_port: 514
     syslog_protocol: "udp"
   ```

1. **Run Meshgram:**

  ```bash
  python src/meshgram.py
  ```

On Linux using a serial device, ensure your user can access the port (e.g. /dev/ttyUSB0):

- Add your user to the dialout group and re-login: `sudo usermod -a -G dialout "$USER"`
- Verify permissions: `ls -l /dev/ttyUSB0`

## 📡 Telegram Commands

- `/start` - Start the bot and see available commands
- `/help` - Show help message
- `/status` - Check the current status of Meshgram and Meshtastic
- `/node [node_id]` - Get information about a specific node
- `/bell [node_id]` - Send a bell notification to a Meshtastic node
- `/user` - Get information about your Telegram user
- `/enable <feature>` - Enable a feature
- `/disable <feature>` - Disable a feature
- `/features` - Show features status
- `/listnodes` - List known nodes
- `/ai <prompt>` - Ask the local AI model (if enabled)
- `/aireset` - Reset your AI conversation context (Telegram user)

## 🛰️ Mesh Commands

Mesh-side slash commands are sent as normal text messages beginning with `/` from a Meshtastic node (Text Message App). The bridge intercepts and (optionally) replies directly back to the sender or the channel depending on configuration.

Enabled via `meshtastic.commands.*` and AI feature flag `aim`:

- `/ping` – Returns hop metrics (HopsAway, HStart, HLimit) plus RSSI/SNR and signal quality emoji.
- `/help` – Lists the mesh commands currently available (respects enabled/disabled status).
- `/travel` – Sends a short safety reminder (toggle via `meshtastic.commands.travel`).
- `/ai <prompt>` – Chat with the local AI model (requires both `meshtastic.commands.ai: true` and `meshtastic.ai_enabled: true`). History is per node shortName (fallback node id).
- `/aireset` – Reset this node's AI conversation context (same enablement requirements as `/ai`).

Notes:

1. Set `meshtastic.commands.ping: true`, `meshtastic.commands.help: true`, `meshtastic.commands.ai: true`, `meshtastic.commands.travel: true` as needed.
2. Turn mesh AI on/off at runtime with `/enable aim` or `/disable aim` from Telegram (admin/authorized user).
3. If `meshtastic.reply_directly` is true in config, command replies are sent as a direct message instead of channel broadcast.
4. Ignored channels (`meshtastic.ignored_channels`) suppress command execution and forwarding.

Feature toggles you can control at runtime:

- `forwarding` — bridge Telegram → Meshtastic messages on/off
- Reporting features: `telemetry`, `location`, `nodes`
- AI features: `ait` (Telegram AI), `aim` (mesh /ai command)
  - Example: `/enable forwarding`, `/disable telemetry`

## 🤝 Contributing

We welcome contributions! 💖 Please open an issue or submit a pull request if you have any improvements or bug fixes.

## 🧾 Structured Logging

Meshgram emits concise key=value structured log lines optimized for grep, indexing, and correlation. Human-readable chat messages are minimized in favor of consistent event names and correlated identifiers.

Core identifiers:

- run_id: Monotonic id for each Meshgram process start.
- instance: Per-component id (meshtastic interface, telegram interface, processor).
- bridge_id: Correlates a single bridged flow (Meshtastic → Telegram or Telegram → Meshtastic) including retries and ACK.

Caller information:

- Each log line includes a caller field in the format `filename:lineno` padded to a minimum width of 30 characters for easy scanning in terminals and log systems.
- Caller attribution points to the original call site even when using the structured logger helpers.

Selected event catalog (grouped):

- Lifecycle: startup_begin / startup_complete / processor_start / processor_stop_begin / processor_stop_complete
- Setup: meshtastic_setup_begin / meshtastic_setup_complete / telegram_setup_begin / telegram_setup_complete
- Bridging (both directions): bridge_start / bridge_meta / bridge_render / bridge_sent / bridge_complete / bridge_error
- Ping: ping_command_rx / ping_command_reply_sent
- ACKs & timers: ack_processed / ack_missing_id / ack_timeout
- Meshtastic send: mt_send_attempt / mt_send_success / mt_send_failure
- Retries: mt_retry_attempt / mt_retry_success / mt_retry_failed / mt_retry_giveup
- Node info: mt_node_info_received / mt_node_info_missing_id / mt_node_info_error
- Telemetry script: mt_telemetry_script_start / mt_telemetry_script_error / mt_telemetry_script_exception / mt_telemetry_parse_error / mt_telemetry_unknown_field / mt_telemetry_publish / mt_telemetry_disabled
- Health (if enabled elsewhere): mt_health_check / mt_health_ok / mt_health_error / mt_health_timeout
- Telegram inbound: tg_message_rx / tg_loc_rx / tg_reaction_rx

Field conventions:

- message_text in render/sent events is truncated to 160 chars and newline-normalized ("\n" → "\\n").
- from_short / from_long / to_short / to_long are resolved from node metadata when available (fallback to raw IDs).
- hops_away = hopStart - hopLimit (safe-fallback to 0 on anomalies).
- mqtt is a boolean indicator of whether the packet arrived via an MQTT gateway.

Tips:

- Correlate an entire bridge journey by grepping bridge_id.
- Filter all enriched mesh→Telegram flows: `grep 'direction=mesh_to_tg'`.
- Separate application runs by run_id if you archive logs across restarts.

## ✍️ Message Formatting (Telegram)

- Meshgram uses Telegram Markdown V2. Plain text is safely escaped to avoid parse errors.
- A limited set of simple tags are supported and mapped to Markdown:
  - `<b>…</b>` → `*…*` (bold)
  - `<i>…</i>` → `_…_` (italic)
  - `<u>…</u>` → `__…__` (underline)
- Markdown links like `[title](https://example.com)` are preserved.

## 🧪 Testing

Run unit tests with pytest:

```bash
pytest -q
```

## ❗ Notes

- Serial vs TCP: set `meshtastic.connection_type` and `meshtastic.device` accordingly.
- Topic mapping: if `telegram.use_topics` is true, map `channelN` to Telegram thread IDs under `topics:`.
- Caching: learned nodes are persisted to `cache.db` (sqlite) for faster startup.

Happy meshing! 🎉

## 🤖 AI (Ollama) Integration

Meshgram can connect to a local [Ollama](https://ollama.com) server to provide AI chat responses:

1. Install and run Ollama locally (ensure the model is pulled, e.g. `ollama pull llama3`).
2. Enable features:
   - In config: set `telegram.ai_enabled: true` (feature key `ait`) to allow `/ai` in Telegram.
   - Set `meshtastic.ai_enabled: true` (feature key `aim`) plus `meshtastic.commands.ai: true` for mesh `/ai`.
3. Configure AI section:

```yaml
ai:
  base_url: "http://127.0.0.1:11434"
  model: "gemma3"
  system_prompt: |
    You are a helpful assistant running on Meshtastic <-> Telegram bridge. Keep answers concise.
    You speak only in Bulgarian, unless the user specifically asks you to respond in English.
    If the user asks you to translate something, you can do so, but only if it is a short phrase or sentence. Do not translate long texts or documents.
    If the user asks you to write code, you can do so, but only if it is a short snippet. Do not write long programs or scripts.
    If the user asks you to generate text, you can do so, but only if it is a short paragraph or two. Do not generate long articles or essays.
    Always be funny and picky.
    Always answer short and concise.
```

Per-user / per-node context & resets:

- Telegram: conversation history is keyed by the Telegram user id. Use `/aireset` to clear your own history.
- Mesh: history is keyed by the sender shortName (fallback raw node id). Use `/aireset` on the mesh (if `meshtastic.commands.ai` and `aim` enabled) to reset that node's context.

Restarting the service clears all histories globally.
