
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class Message:
    type: str
    msg_id: str
    sender_id: str
    payload: Dict[str, Any]
