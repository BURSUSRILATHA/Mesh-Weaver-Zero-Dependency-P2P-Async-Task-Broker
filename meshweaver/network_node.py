
import asyncio
import sys

from meshweaver.protocol import (
    create_message,
    encode_message,
    decode_message
)


HOST = "127.0.0.1"
PORT = 8888


async def process_message(message):
    """Process messages received from another node."""

    sender = message["sender"]
    message_type = message["type"]
    payload = message["payload"]

    print(f"\n[Node 1] Message received from {sender}")
    print(f"[Node 1] Message type: {message_type}")

    if message_type == "TASK":
        print(f"[Node 1] Processing task: {payload}")

        await asyncio.sleep(1)

        print(f"[Node 1] Task completed: {payload}")

    elif message_type == "PING":
        print("[Node 1] PING received")

    else:
        print(f"[Node 1] Unknown message: {message_type}")


async def handle_client(reader, writer):
    """Receive messages from a connected node."""

    address = writer.get_extra_info("peername")

    print(f"\n[Node 1] Node connected: {address}")

    try:
        while True:
            data = await reader.readline()

            if not data:
                break

            try:
                message = decode_message(data.strip())
                await process_message(message)

            except ValueError as error:
                print(f"[Node 1] Invalid message: {error}")

    finally:
        print("[Node 1] Node disconnected")
        writer.close()
        await writer.wait_closed()


async def start_server():
    """Start Node 1 as a TCP server."""

    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT
    )

    print("[Node 1] MeshWeaver server started!")
    print(f"[Node 1] Listening on {HOST}:{PORT}")

    async with server:
        await server.serve_forever()


async def start_client():
    """Start Node 2 as a TCP client."""

    print("[Node 2] Connecting to Node 1...")

    reader, writer = await asyncio.open_connection(
        HOST,
        PORT
    )

    print("[Node 2] Connected successfully!")

    try:
        messages = [
            ("TASK", "Task Request 1"),
            ("TASK", "Task Request 2"),
            ("PING", "Check connection")
        ]

        for message_type, payload in messages:

            message = create_message(
                message_type,
                "MeshWeaver-Node-2",
                "MeshWeaver-Node-1",
                payload
            )

            encoded_message = encode_message(message)

            writer.write(encoded_message + b"\n")
            await writer.drain()

            print(f"[Node 2] Message sent: {message_type}")

    finally:
        writer.close()
        await writer.wait_closed()

    print("[Node 2] Communication completed!")


async def main():

    if len(sys.argv) < 2:
        print("Usage:")
        print("python -m meshweaver.network_node server")
        print("python -m meshweaver.network_node client")
        return

    role = sys.argv[1].lower()

    if role == "server":
        await start_server()

    elif role == "client":
        await start_client()

    else:
        print("Invalid role. Choose server or client.")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except ConnectionRefusedError:
        print("[Error] Server is not running. Start Node 1 first.")
    except KeyboardInterrupt:
        print("\nMeshWeaver stopped.")
