
import asyncio

from meshweaver.protocol import (
    create_message,
    encode_message,
    decode_message
)


class MeshWeaverNode:

    def __init__(self, node_name):
        self.node_name = node_name
        self.message_queue = asyncio.Queue()
        self.running = False

    async def send_message(self, receiver, message_type, payload):
        """Create and send a message to the queue."""

        message = create_message(
            message_type,
            self.node_name,
            receiver,
            payload
        )

        encoded_message = encode_message(message)

        await self.message_queue.put(encoded_message)

        print(
            f"[{self.node_name}] Message sent: "
            f"{message_type}"
        )

    async def receive_message(self):
        """Receive and process queued messages."""

        while self.running:

            encoded_message = await self.message_queue.get()

            try:
                message = decode_message(encoded_message)

                print(
                    f"[{self.node_name}] Message received from "
                    f"{message['sender']}"
                )

                await self.process_message(message)

            except ValueError as error:
                print(f"Message error: {error}")

            finally:
                self.message_queue.task_done()

    async def process_message(self, message):
        """Process an individual message."""

        message_type = message["type"]
        payload = message["payload"]

        if message_type == "TASK":
            print(
                f"[{self.node_name}] Processing task: {payload}"
            )

            await asyncio.sleep(1)

            print(
                f"[{self.node_name}] Task completed: {payload}"
            )

        elif message_type == "PING":
            print(f"[{self.node_name}] PING received")

        elif message_type == "PONG":
            print(f"[{self.node_name}] PONG received")

        else:
            print(
                f"[{self.node_name}] Unknown message type: "
                f"{message_type}"
            )

    async def start(self):
        """Start the MeshWeaver node."""

        self.running = True

        print(f"\n{self.node_name} started successfully!")

        receiver_task = asyncio.create_task(
            self.receive_message()
        )

        try:
            await self.send_message(
                "Node-2",
                "TASK",
                "Task Request 1"
            )

            await self.send_message(
                "Node-2",
                "TASK",
                "Task Request 2"
            )

            await self.send_message(
                "Node-2",
                "PING",
                "Check connection"
            )

            await self.message_queue.join()

        finally:
            self.running = False
            receiver_task.cancel()

            try:
                await receiver_task
            except asyncio.CancelledError:
                pass

        print("\nAll messages processed successfully!")


async def main():

    node = MeshWeaverNode("MeshWeaver-Node-1")

    await node.start()


if __name__ == "__main__":
    asyncio.run(main())
