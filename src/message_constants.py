"""Central definitions for message queue keys and types.

Import these constants instead of hardcoding literals across modules.
This helps avoid typos and enables static analysis improvements.
"""
from __future__ import annotations

from typing import Literal, TypedDict, NotRequired
from datetime import datetime

# --- Generic Keys ---
MSG_TYPE = "type"
MSG_TEXT = "text"
MSG_SENDER = "sender"
MSG_LOCATION = "location"
MSG_MESSAGE_ID = "message_id"
MSG_THREAD_ID = "thread_id"
MSG_USER_ID = "user_id"
MSG_COMMAND = "command"
MSG_ARGS = "args"
MSG_UPDATE = "update"
MSG_EMOJI = "emoji"
MSG_ORIGINAL_MESSAGE_ID = "original_message_id"
MSG_RECIPIENT = "recipient"
MSG_REQUEST_ID = "request_id"
MSG_TO = "to"
MSG_FROM = "from"

# --- Message Type Literals ---
MessageKind = Literal[
    "ack",
    "telegram",
    "location",
    "reaction",
    "command",
]

# --- Queue Message TypedDicts ---
class AckMessage(TypedDict):
    type: Literal["ack"]
    from_: str | None
    to: str | None
    message_id: str | None
    request_id: int | str | None

class TelegramQueueMessage(TypedDict, total=False):
    type: Literal["telegram", "location", "reaction", "command"]
    text: NotRequired[str]
    sender: NotRequired[str]
    message_id: NotRequired[int]
    thread_id: NotRequired[int]
    user_id: NotRequired[int]
    command: NotRequired[str]
    args: NotRequired[list[str]]
    location: NotRequired[dict[str, float]]
    emoji: NotRequired[str]
    original_message_id: NotRequired[int]
    # update intentionally omitted to avoid heavy coupling in types

class PendingAckInfo(TypedDict):
    telegram_message_id: int
    telegram_thread_id: int
    timestamp: datetime

__all__ = [
    # keys
    "MSG_TYPE", "MSG_TEXT", "MSG_SENDER", "MSG_LOCATION", "MSG_MESSAGE_ID", "MSG_THREAD_ID",
    "MSG_USER_ID", "MSG_COMMAND", "MSG_ARGS", "MSG_UPDATE", "MSG_EMOJI", "MSG_ORIGINAL_MESSAGE_ID",
    "MSG_RECIPIENT", "MSG_REQUEST_ID", "MSG_TO", "MSG_FROM",
    # types
    "MessageKind", "AckMessage", "TelegramQueueMessage", "PendingAckInfo"
]
