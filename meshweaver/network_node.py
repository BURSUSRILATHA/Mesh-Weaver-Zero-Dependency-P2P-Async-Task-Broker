
import asyncio
import sys
import itertools

from meshweaver.protocol import (
    create_message,
    encode_message,
    decode_message
)

HOST = "127.0.0.1"
PORT = 8888

# Priority queue and worker settings
task_queue = asyncio.PriorityQueue()
NUM_WORKERS = 3

# Ensures tasks with equal priority have a unique ordering
task_counter = itertools.count()


# Process an individual task
async def process_task(message):

    sender = message["sender"]
    payload = message["payload"]

    print(f"\n[Worker] Processing task: {payload}")

    await asyncio.sleep(3)

    print(f"[Worker] Task completed: {payload}")

    return create_message(
        "TASK_COMPLETED",
        "MeshWeaver-Node-1",
        sender,
        f"{payload} completed successfully"
    )


# Multiple asynchronous workers
async def task_worker(worker_id):

    print(f"[Worker {worker_id}] Worker started!")

    while True:

        priority, count, message, writer = await task_queue.get()

        try:

            print(
                f"\n[Worker {worker_id}] "
                f"Processing priority {priority}: "
                f"{message['payload']}"
            )

            response = await process_task(message)

            writer.write(encode_message(response) + b"\n")
            await writer.drain()

            print(
                f"[Worker {worker_id}] "
                "Completion response sent!"
            )

        except (ConnectionError, OSError) as error:

            print(f"[Worker {worker_id}] Error: {error}")

        finally:

            task_queue.task_done()


# Handle client connections
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

                if message_type == "TASK":

                    payload = message["payload"]

                    # Read priority from the message
                    priority = message.get("priority", 3)

                    if not isinstance(priority, int) or not 1 <= priority <= 5:
                        print("[Node 1] Invalid task priority")
                        continue

                    await task_queue.put(
                        (
                            priority,
                            next(task_counter),
                            message,
                            writer
                        )
                    )

                    print(
                        f"[Node 1] Task queued: {payload} "
                        f"(Priority: {priority})"
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

                    print(f"[Node 1] Unknown message: {message_type}")

            except ValueError as error:

                print(f"[Node 1] Invalid message: {error}")

    finally:

        print("[Node 1] Node disconnected")

        writer.close()

        await writer.wait_closed()


# Start server and workers
async def start_server():

    workers = [
        asyncio.create_task(task_worker(i))
        for i in range(1, NUM_WORKERS + 1)
    ]

    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT
    )

    print("\n[Node 1] MeshWeaver server started!")
    print(f"[Node 1] Listening on {HOST}:{PORT}")
    print(f"[Node 1] Started {NUM_WORKERS} priority workers!")

    try:

        async with server:
            await server.serve_forever()

    finally:

        for worker in workers:
            worker.cancel()

        await asyncio.gather(*workers, return_exceptions=True)


# Client sends priority-based tasks
async def start_client():

    print("[Node 2] Connecting to Node 1...")

    reader, writer = await asyncio.open_connection(HOST, PORT)

    print("[Node 2] Connected successfully!")

    # Lower number means higher priority
    tasks = [
        ("Low Priority Task", 5),
        ("Urgent Task", 1),
        ("Normal Task", 3),
        ("Important Task", 2),
        ("Another Normal Task", 3)
    ]

    try:

        for payload, priority in tasks:

            message = create_message(
                "TASK",
                "MeshWeaver-Node-2",
                "MeshWeaver-Node-1",
                payload
            )

            # Add priority field
            message["priority"] = priority

            writer.write(encode_message(message) + b"\n")
            await writer.drain()

            print(
                f"[Node 2] Task sent: {payload} "
                f"(Priority: {priority})"
            )

        # Send PING
        ping_message = create_message(
            "PING",
            "MeshWeaver-Node-2",
            "MeshWeaver-Node-1",
            "Check connection"
        )

        writer.write(encode_message(ping_message) + b"\n")
        await writer.drain()

        print("[Node 2] PING message sent!")

        # Read PONG first
        response_data = await reader.readline()

        if response_data:

            response = decode_message(response_data.strip())

            print(
                f"[Node 2] Response received: "
                f"{response['type']}"
            )

            print(f"[Node 2] {response['payload']}")

        # Read all task completion responses
        for _ in range(len(tasks)):

            response_data = await reader.readline()

            if not response_data:
                print("[Node 2] Server disconnected")
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

        print("[Error] Server is not running.")

    except KeyboardInterrupt:

        print("\nMeshWeaver stopped.")
