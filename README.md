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
- Cache learned nodes to nodes.json
- Respond to /ping command from mestastic node

## 🛠 Requirements

- Python 3.12+ 🐍
- Dependencies:
  - `envyaml`: For YAML configuration file parsing with environment variable support
  - `meshtastic`: Python API for Meshtastic devices
  - `python-telegram-bot`: Telegram Bot API wrapper
  - `pubsub`: For publish-subscribe messaging pattern

## 🚀 Quick Start

1. **Clone the repo:**
   ```bash
   git clone https://github.com/gretel/meshgram.git
   cd meshgram
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

4. **Configure the project:**
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
     #local_nodes:
     #  - "!abcdef12"
     #  - "!12345678"

   reports:
      telemetry: True
      location: True

   channels:
      - "LongFast" # 0
      - "MySecondaryChannel" # 1

   # set the message_thread_id for each topic
   # read this https://stackoverflow.com/a/75178418/420585 to learn how to get the message_thread_id
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
     level: "info"
     level_telegram: "warn"
     level_httpx: "warn"
     use_syslog: false
     syslog_host: "localhost"
     syslog_port: 514
     syslog_protocol: "udp"
   ```

5. **Run Meshgram:**
   ```bash
   python src/meshgram.py
   ```

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

## 🤝 Contributing

We welcome contributions! 💖 Please open an issue or submit a pull request if you have any improvements or bug fixes.

## 🧾 Structured Logging

Meshgram emits concise key=value structured log lines optimized for grep, indexing, and correlation. Human-readable chat messages are minimized in favor of consistent event names and correlated identifiers.

Core identifiers:

- run_id: Monotonic id for each Meshgram process start.
- instance: Per-component id (meshtastic interface, telegram interface, processor).
- bridge_id: Correlates a single bridged flow (Meshtastic → Telegram or Telegram → Meshtastic) including retries and ACK.

Bridge example (Telegram → Meshtastic):

```text
event=bridge_start instance=5 bridge_id=91 direction=tg_to_mesh
event=bridge_sent instance=5 bridge_id=91 direction=tg_to_mesh meshtastic_message_id=517
event=ack_processed instance=5 bridge_id=91 message_id=517 telegram_message_id=1234
event=bridge_complete instance=5 bridge_id=91 direction=tg_to_mesh
```

Bridge example (Meshtastic → Telegram, enriched metadata):

```text
event=bridge_start bridge_id=42 direction=mesh_to_tg from_id=!abcd1234 to_id=^all from_short=Base from_long=BasementNode
event=bridge_meta bridge_id=42 direction=mesh_to_tg from_short=Base hops_away=1 hop_limit=3 hop_start=4 rssi=-72 snr=7.3 mqtt=false
event=bridge_render bridge_id=42 direction=mesh_to_tg message_text="Hello world"
event=bridge_sent bridge_id=42 direction=mesh_to_tg
event=bridge_complete bridge_id=42 direction=mesh_to_tg
```

Ping command flow (from mesh):

```text
event=ping_command_rx bridge_id=77 from_short=NodeA channel=0 hops_away=0
event=ping_command_reply_sent bridge_id=77 meshtastic_message_id=612 from_short=NodeA
```

ACK lifecycle:

```text
event=mt_send_attempt recipient=^all channel=0 size=42
event=mt_send_success recipient=^all channel=0 message_id=517
event=ack_processed bridge_id=91 message_id=517 telegram_message_id=1234
```

Retry lifecycle:

```text
event=mt_retry_attempt recipient=!abcd1234 attempts=2
event=mt_retry_success recipient=!abcd1234
```
Or failure path:

```text
event=mt_retry_failed recipient=!abcd1234 attempts=5
event=mt_retry_giveup recipient=!abcd1234
```

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

Happy meshing! 🎉
