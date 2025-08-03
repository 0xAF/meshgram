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

Happy meshing! 🎉
