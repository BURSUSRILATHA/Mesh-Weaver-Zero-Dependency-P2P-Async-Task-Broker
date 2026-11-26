
import json
import uuid


def create_message(message_type, sender, receiver, payload):
    """Create a standard MeshWeaver message."""

    message = {
        "message_id": str(uuid.uuid4()),
        "type": message_type,
        "sender": sender,
        "receiver": receiver,
        "payload": payload
    }

    return message


def encode_message(message):
    """Convert a message dictionary into JSON bytes."""

    return json.dumps(message).encode("utf-8")


def decode_message(data):
    """Convert JSON bytes into a Python dictionary."""

    try:
        message = json.loads(data.decode("utf-8"))

        required_fields = [
            "message_id",
            "type",
            "sender",
            "receiver",
            "payload"
        ]

        for field in required_fields:
            if field not in message:
                raise ValueError(f"Missing field: {field}")

        return message

    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Invalid message format") from error
