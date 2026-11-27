
import asyncio
import sys

from meshweaver.protocol import (
    create_message,
    encode_message,
    decode_message
)


HOST = "127.0.0.1"
PORT = 8888

task_queue = asyncio.Queue()


async def process_task(message):
    """Process one task from the queue."""

    sender = message["sender"]
    payload = message["payload"]

    print(f"\n[Worker] Processing task: {payload}")

    await asyncio.sleep(2)

    print(f"[Worker] Task completed: {payload}")

    return create_message(
        "TASK_COMPLETED",
        "MeshWeaver-Node-1",
        sender,
        f"{payload} completed successfully"
    )


async def task_worker():
    """Take tasks from the queue and process them."""

    while True:

        message, writer = await task_queue.get()

        try:
            response = await process_task(message)

            encoded_response = encode_message(response)

            writer.write(encoded_response + b"\n")

            await writer.drain()

            print("[Worker] Completion response sent!")

        except (ConnectionError, OSError) as error:
            print(f"[Worker] Could not send response: {error}")

        finally:
            task_queue.task_done()


async def handle_client(reader, writer):
    """Receive messages from connected nodes."""

    address = writer.get_extra_info("peername")

    print(f"\n[Node 1] Node connected: {address}")

    try:
        while True:

            data = await reader.readline()

            if not data:
                break

            try:
                message = decode_message(data.strip())

                message_type = message["type"]

                if message_type == "TASK":

                    await task_queue.put((message, writer))

                    print(
                        f"[Node 1] Task added to queue: "
                        f"{message['payload']}"
                    )

                elif message_type == "PING":

                    response = create_message(
                        "PONG",
                        "MeshWeaver-Node-1",
                        message["sender"],
                        "Connection is active"
                    )

                    writer.write(encode_message(response) + b"\n")

                    await writer.drain()

                    print("[Node 1] PONG response sent!")

                else:
                    print(
                        f"[Node 1] Unknown message type: "
                        f"{message_type}"
                    )

            except ValueError as error:
                print(f"[Node 1] Invalid message: {error}")

    finally:
        print("[Node 1] Node disconnected")

        writer.close()

        await writer.wait_closed()


async def start_server():
    """Start Node 1 as a TCP server."""

    worker = asyncio.create_task(task_worker())

    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT
    )

    print("[Node 1] MeshWeaver server started!")
    print(f"[Node 1] Listening on {HOST}:{PORT}")
    print("[Node 1] Task queue worker started!")

    try:
        async with server:
            await server.serve_forever()

    finally:
        worker.cancel()

        try:
            await worker
        except asyncio.CancelledError:
            pass


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
            ("TASK", "Task Request 3"),
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

            print(f"\n[Node 2] Message sent: {message_type}")

        for message_type, payload in messages:

            if message_type == "PING":
                response_data = await reader.readline()

                if response_data:
                    response = decode_message(response_data.strip())

                    print(
                        f"[Node 2] Response received: "
                        f"{response['type']}"
                    )

                    print(f"[Node 2] {response['payload']}")

        for _ in range(3):

            response_data = await reader.readline()

            if not response_data:
                print("[Node 2] Server disconnected.")
                break

            response = decode_message(response_data.strip())

            print(
                f"[Node 2] Response received: "
                f"{response['type']}"
            )

            print(f"[Node 2] {response['payload']}")

    finally:

        writer.close()

        await writer.wait_closed()

    print("\n[Node 2] Communication completed!")


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
