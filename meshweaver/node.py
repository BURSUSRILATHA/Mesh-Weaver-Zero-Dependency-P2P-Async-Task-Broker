
from meshweaver.protocol import Message


class Node:

    def __init__(self, node_id):
        self.node_id = node_id

    def send_ping(self, other_node):
        print(f"\n{self.node_id} is sending PING to {other_node.node_id}")

        ping_message = Message(
            type="PING",
            msg_id="MSG001",
            sender_id=self.node_id,
            payload={"receiver": other_node.node_id}
        )

        response = other_node.receive_message(ping_message)

        self.receive_message(response)

    def receive_message(self, message):
        print(f"{self.node_id} received {message.type} from {message.sender_id}")

        if message.type == "PING":
            pong_message = Message(
                type="PONG",
                msg_id="MSG002",
                sender_id=self.node_id,
                payload={"reply_to": message.sender_id}
            )
            return pong_message



node1 = Node("NODE001")
node2 = Node("NODE002")

# Send PING from Node 1 to Node 2
node1.send_ping(node2)
