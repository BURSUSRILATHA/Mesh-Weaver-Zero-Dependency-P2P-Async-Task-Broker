
from meshweaver.protocol import Message


class Node:

    def __init__(self, node_id):
        self.node_id = node_id

        
        self.handlers = {
            "PING": self.handle_ping,
            "PONG": self.handle_pong
        }

    
    def send_ping(self, other_node):
        print(f"\n{self.node_id} is sending PING to {other_node.node_id}")

        message = Message(
            type="PING",
            msg_id="MSG001",
            sender_id=self.node_id,
            payload={"receiver": other_node.node_id}
        )

        response = other_node.dispatch_message(message)

        if response:
            self.dispatch_message(response)

    
    def dispatch_message(self, message):
        print(f"\n{self.node_id} dispatching {message.type} message")

        handler = self.handlers.get(message.type)

        if handler:
            return handler(message)
        else:
            print("Unknown message type:", message.type)
            return None

    def handle_ping(self, message):
        print(f"{self.node_id} received PING from {message.sender_id}")

        response = Message(
            type="PONG",
            msg_id="MSG002",
            sender_id=self.node_id,
            payload={"reply_to": message.sender_id}
        )

        return response

    
    def handle_pong(self, message):
        print(f"{self.node_id} received PONG from {message.sender_id}")



node1 = Node("NODE001")
node2 = Node("NODE002")



node1.send_ping(node2)



unknown_message = Message(
    type="HELLO",
    msg_id="MSG003",
    sender_id="NODE002",
    payload={}
)

node1.dispatch_message(unknown_message)
