
import asyncio
import sys

from meshweaver.protocol import (
    create_message,
    encode_message,
    decode_message
)

HOST = "127.0.0.1"
PORT = 8888

# Shared task queue
task_queue = asyncio.Queue()

# Number of workers
NUM_WORKERS = 3


# Process an individual task
async def process_task(message):
    sender = message["sender"]
    payload = message["payload"]

    print(f"\n[Worker] Processing task: {payload}")

    # Simulate task processing
    await asyncio.sleep(3)

    print(f"[Worker] Task completed: {payload}")

    response = create_message(
        "TASK_COMPLETED",
        "MeshWeaver-Node-1",
        sender,
        f"{payload} completed successfully"
    )

    return response


# Multiple asynchronous workers
async def task_worker(worker_id):

    print(f"[Worker {worker_id}] Worker started!")

    while True:

        message, writer = await task_queue.get()

        try:
            print(
                f"\n[Worker {worker_id}] "
                f"Picked up task: {message['payload']}"
            )

            response = await process_task(message)

            encoded_response = encode_message(response)

            writer.write(encoded_response + b"\n")

            await writer.drain()

            print(
                f"[Worker {worker_id}] "
                "Completion response sent!"
            )

        except (ConnectionError, OSError) as error:

            print(
                f"[Worker {worker_id}] "
                f"Could not send response: {error}"
            )

        finally:
            task_queue.task_done()


# Handle incoming client connections
async def handle_client(reader, writer):

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

                # Add task to shared queue
                if message_type == "TASK":

                    await task_queue.put((message, writer))

                    print(
                        f"[Node 1] Task added to queue: "
                        f"{message['payload']}"
                    )

                # Respond to PING
                elif message_type == "PING":

                    response = create_message(
                        "PONG",
                        "MeshWeaver-Node-1",
                        message["sender"],
                        "Connection is active"
                    )

                    writer.write(
                        encode_message(response) + b"\n"
                    )

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


# Start the server and three workers
async def start_server():

    workers = []

    for worker_id in range(1, NUM_WORKERS + 1):

        worker = asyncio.create_task(
            task_worker(worker_id)
        )

        workers.append(worker)

    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT
    )

    print("\n[Node 1] MeshWeaver server started!")

    print(f"[Node 1] Listening on {HOST}:{PORT}")

    print(
        f"[Node 1] Started {NUM_WORKERS} task workers!"
    )

    try:

        async with server:

            await server.serve_forever()

    finally:

        for worker in workers:
            worker.cancel()

        await asyncio.gather(
            *workers,
            return_exceptions=True
        )


# Client sends tasks to server
async def start_client():

    print("[Node 2] Connecting to Node 1...")

    reader, writer = await asyncio.open_connection(
        HOST,
        PORT
    )

    print("[Node 2] Connected successfully!")

    task_count = 5

    try:

        # Send five tasks
        for number in range(1, task_count + 1):

            message = create_message(
                "TASK",
                "MeshWeaver-Node-2",
                "MeshWeaver-Node-1",
                f"Task Request {number}"
            )

            writer.write(
                encode_message(message) + b"\n"
            )

            await writer.drain()

            print(
                f"[Node 2] Task sent: Task Request {number}"
            )

        # Send PING message
        ping_message = create_message(
            "PING",
            "MeshWeaver-Node-2",
            "MeshWeaver-Node-1",
            "Check connection"
        )

        writer.write(
            encode_message(ping_message) + b"\n"
        )

        await writer.drain()

        print("[Node 2] PING message sent!")

        # Receive PONG response
        response_data = await reader.readline()

        if response_data:

            response = decode_message(
                response_data.strip()
            )

            print(
                f"[Node 2] Response received: "
                f"{response['type']}"
            )

            print(f"[Node 2] {response['payload']}")

        # Receive task completion responses
        for _ in range(task_count):

            response_data = await reader.readline()

            if not response_data:

                print("[Node 2] Server disconnected.")

                break

            response = decode_message(
                response_data.strip()
            )

            print(
                f"[Node 2] Response received: "
                f"{response['type']}"
            )

            print(f"[Node 2] {response['payload']}")

    finally:

        writer.close()

        await writer.wait_closed()

    print("\n[Node 2] Communication completed!")


# Main function
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

        print(
            "[Error] Server is not running. "
            "Start Node 1 first."
        )

    except KeyboardInterrupt:

        print("\nMeshWeaver stopped.")
