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

## 🛠 Requirements

- Python 3.11+ 🐍

1. **Clone the project:**

  ```bash
  git clone https://github.com/0xAF/meshgram-plus
  cd meshgram-plus
  ```

2. **Set up a virtual environment:**

  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```

3. **Install dependencies:**

  ```bash
  pip install -r requirements.txt
  ```

4. **Configure:**

  ```bash
  cp config/example.config.yaml config/config.yaml
  $EDITOR config/config.yaml
  ```

5. **Run:**

  ```bash
  python src/meshgram.py
  ```

On Linux using a serial device, ensure your user can access the port (e.g. /dev/ttyUSB0): add your user to the dialout group and re‑login: `sudo usermod -a -G dialout "$USER"`.

## ⚙️ Configuration

Use `config/example.config.yaml` as a starting point. It’s fully commented and includes:

- Telegram: bot token, chat id, topics/threads, optional triggers, AI toggle
- Meshtastic: serial/tcp device, default node/channel, admin_nodes, send chunking controls, travel_template
- Triggers: regex rules with YAML‑escaped backslashes (e.g., `"\\b"`), ops `replace|prepend|reply` (reply on mesh only)
- Per‑channel control: names, ignored_channels, receive_only_channels
- Reports: telemetry, location, nodes
- Logging: per‑lib levels, syslog/file options
- AI: provider (ollama/openai), model/base_url, system prompt, tools (local weather)
   
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

## 📜 Project history and credits

Originally started as “meshgram” by Tom Hansel. The original repo: <https://github.com/gretel/meshgram>.

Huge thanks to Tom for the great foundation and all his work on the original project.
