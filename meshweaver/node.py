
from meshweaver.protocol import Message

message = Message(
    type="PING",
    msg_id="MSG001",
    sender_id="NODE001",
    payload={"text": "Hello MeshWeaver"}
)

print("MeshWeaver Project Started!")
print("Message Type:", message.type)
print("Message ID:", message.msg_id)
print("Sender:", message.sender_id)
print("Payload:", message.payload)
