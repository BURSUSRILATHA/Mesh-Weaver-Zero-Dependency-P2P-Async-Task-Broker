
from meshweaver.protocol import (
    Message,
    generate_message_id,
    message_to_json,
    message_from_json
)


class Node:

    def __init__(self, node_id):
        self.node_id = node_id

        self.handlers = {
            "PING": self.handle_ping,
            "PONG": self.handle_pong
        }

    # Validate incoming message
    def validate_message(self, message):

        if not isinstance(message, Message):
            return False, "Invalid message object"

        if not isinstance(message.type, str) or not message.type.strip():
            return False, "Message type is missing"

        if not isinstance(message.msg_id, str) or not message.msg_id.strip():
            return False, "Message ID is missing"

        if not isinstance(message.sender_id, str) or not message.sender_id.strip():
            return False, "Sender ID is missing"

        if not isinstance(message.payload, dict):
            return False, "Payload must be a dictionary"

        return True, "Message is valid"

    # Send PING message
    def send_ping(self, other_node):

        print(f"\n{self.node_id} is sending PING to {other_node.node_id}")

        message = Message(
            type="PING",
            msg_id=generate_message_id(),
            sender_id=self.node_id,
            payload={"receiver": other_node.node_id}
        )

        print("PING Message ID:", message.msg_id)

        response = other_node.dispatch_message(message)

        if response:
            self.dispatch_message(response)

    # Dispatch incoming messages
    def dispatch_message(self, message):

        print(f"\n{self.node_id} dispatching message")

        is_valid, result = self.validate_message(message)

        if not is_valid:
            print("Invalid message:", result)
            return None

        print("Message validation:", result)

        handler = self.handlers.get(message.type)

        if handler:
            return handler(message)

        else:
            print("Unknown message type:", message.type)
            return None

    # Handle PING
    def handle_ping(self, message):

        print(f"{self.node_id} received PING from {message.sender_id}")

        response = Message(
            type="PONG",
            msg_id=generate_message_id(),
            sender_id=self.node_id,
            payload={"reply_to": message.sender_id}
        )

        print("PONG Message ID:", response.msg_id)

        return response

    # Handle PONG
    def handle_pong(self, message):

        print(f"{self.node_id} received PONG from {message.sender_id}")


# Create two nodes
node1 = Node("NODE001")
node2 = Node("NODE002")


# Test PING-PONG communication
node1.send_ping(node2)


# Test unknown message
unknown_message = Message(
    type="HELLO",
    msg_id=generate_message_id(),
    sender_id="NODE002",
    payload={}
)

node1.dispatch_message(unknown_message)


# Test invalid message
invalid_message = Message(
    type="PING",
    msg_id="",
    sender_id="NODE002",
    payload={}
)

node1.dispatch_message(invalid_message)


# Day 6: Serialization and Deserialization
print("\n========== DAY 6 TEST ==========")

test_message = Message(
    type="PING",
    msg_id=generate_message_id(),
    sender_id="NODE001",
    payload={"receiver": "NODE002"}
)

# Serialization: Message to JSON
json_data = message_to_json(test_message)

print("\nSerialized JSON:")
print(json_data)


# Deserialization: JSON to Message
restored_message = message_from_json(json_data)

print("\nDeserialized Message:")
print(restored_message)


# Check whether original and restored messages match
if test_message == restored_message:
    print("\nDay 6 Test Passed!")
else:
    print("\nDay 6 Test Failed!")
