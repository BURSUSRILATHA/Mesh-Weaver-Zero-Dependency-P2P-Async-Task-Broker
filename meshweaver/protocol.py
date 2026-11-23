
from dataclasses import dataclass
from typing import Any, Dict
import uuid
import json


def generate_message_id():
    return str(uuid.uuid4())


@dataclass
class Message:
    type: str
    msg_id: str
    sender_id: str
    payload: Dict[str, Any]


# Convert a message into JSON
def message_to_json(message):

    message_data = {
        "type": message.type,
        "msg_id": message.msg_id,
        "sender_id": message.sender_id,
        "payload": message.payload
    }

    return json.dumps(message_data)


# Convert JSON back into a message
def message_from_json(json_data):

    data = json.loads(json_data)

    if not isinstance(data, dict):
        raise ValueError("Message data must be a JSON object")

    required_fields = ["type", "msg_id", "sender_id", "payload"]

    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing field: {field}")

    if not isinstance(data["payload"], dict):
        raise ValueError("Payload must be a dictionary")

    return Message(
        type=data["type"],
        msg_id=data["msg_id"],
        sender_id=data["sender_id"],
        payload=data["payload"]
    )
