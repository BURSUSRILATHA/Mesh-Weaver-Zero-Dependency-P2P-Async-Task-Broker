
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

NUM_WORKERS = 3
MAX_ATTEMPTS = 3

task_queue = asyncio.PriorityQueue()
task_counter = itertools.count()


async def process_task(message, attempt):
    payload = message["payload"]

    print(
        f"\n[Worker] Processing: {payload} "
        f"(Attempt {attempt}/{MAX_ATTEMPTS})"
    )

    await asyncio.sleep(1)

    # Demonstrate a task that succeeds after two retries.
    if "Retry" in payload and attempt < 3:
        raise RuntimeError("Temporary task error")

    # Demonstrate a task that always fails.
    if "Always Fail" in payload:
        raise RuntimeError("Permanent task error")

    print(f"[Worker] Task completed: {payload}")

    return create_message(
        "TASK_COMPLETED",
        "MeshWeaver-Node-1",
        message["sender"],
        f"{payload} completed successfully"
    )


async def send_response(writer, response):
    try:
        writer.write(encode_message(response) + b"\n")
        await writer.drain()
    except (ConnectionError, OSError) as error:
        print(f"[Node 1] Could not send response: {error}")


async def task_worker(worker_id):
    print(f"[Worker {worker_id}] Worker started!")

    while True:
        priority, count, message, writer, attempt = (
            await task_queue.get()
        )

        try:
            print(
                f"[Worker {worker_id}] Picked up: "
                f"{message['payload']}"
            )

            try:
                response = await process_task(message, attempt)

                await send_response(writer, response)

            except Exception as error:
                print(
                    f"[Worker {worker_id}] Task failed: {error}"
                )

                if attempt < MAX_ATTEMPTS:
                    print(
                        f"[Worker {worker_id}] Retrying task... "
                        f"({attempt + 1}/{MAX_ATTEMPTS})"
                    )

                    await task_queue.put(
                        (
                            priority,
                            next(task_counter),
                            message,
                            writer,
                            attempt + 1
                        )
                    )

                else:
                    print(
                        f"[Worker {worker_id}] Maximum attempts reached!"
                    )

                    failure_response = create_message(
                        "TASK_FAILED",
                        "MeshWeaver-Node-1",
                        message["sender"],
                        f"{message['payload']} failed after "
                        f"{MAX_ATTEMPTS} attempts"
                    )

                    await send_response(writer, failure_response)

        finally:
            task_queue.task_done()


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
                    priority = message.get("priority", 3)

                    if (
                        not isinstance(priority, int)
                        or isinstance(priority, bool)
                        or not 1 <= priority <= 5
                    ):
                        response = create_message(
                            "TASK_FAILED",
                            "MeshWeaver-Node-1",
                            message["sender"],
                            "Invalid priority. Use values from 1 to 5."
                        )
                        await send_response(writer, response)
                        continue

                    await task_queue.put(
                        (
                            priority,
                            next(task_counter),
                            message,
                            writer,
                            1
                        )
                    )

                    print(
                        f"[Node 1] Task queued: "
                        f"{message['payload']} "
                        f"(Priority: {priority})"
                    )

                elif message_type == "PING":
                    response = create_message(
                        "PONG",
                        "MeshWeaver-Node-1",
                        message["sender"],
                        "Connection is active"
                    )

                    await send_response(writer, response)

            except (ValueError, KeyError, TypeError) as error:
                print(f"[Node 1] Invalid message: {error}")

    finally:
        writer.close()
        await writer.wait_closed()
        print("[Node 1] Client disconnected")


async def start_server():
    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT
    )

    workers = [
        asyncio.create_task(task_worker(i + 1))
        for i in range(NUM_WORKERS)
    ]

    print("\n[Node 1] MeshWeaver server started!")
    print(f"[Node 1] Listening on {HOST}:{PORT}")
    print(f"[Node 1] Started {NUM_WORKERS} task workers!")
    print(f"[Node 1] Maximum attempts per task: {MAX_ATTEMPTS}")

    try:
        async with server:
            await server.serve_forever()

    finally:
        for worker in workers:
            worker.cancel()

        await asyncio.gather(*workers, return_exceptions=True)


async def start_client():
    print("[Node 2] Connecting to Node 1...")

    reader, writer = await asyncio.open_connection(HOST, PORT)

    print("[Node 2] Connected successfully!")

    tasks = [
        ("Normal Task", 3),
        ("Retry Task", 1),
        ("Always Fail Task", 2)
    ]

    for payload, priority in tasks:
        message = create_message(
            "TASK",
            "MeshWeaver-Node-2",
            "MeshWeaver-Node-1",
            payload
        )

        message["priority"] = priority

        writer.write(encode_message(message) + b"\n")
        await writer.drain()

        print(
            f"[Node 2] Task sent: {payload} "
            f"(Priority: {priority})"
        )

    ping = create_message(
        "PING",
        "MeshWeaver-Node-2",
        "MeshWeaver-Node-1",
        "Check connection"
    )

    writer.write(encode_message(ping) + b"\n")
    await writer.drain()

    print("[Node 2] PING message sent!")

    try:
        response = decode_message(await reader.readline())

        print(
            f"[Node 2] Response received: {response['type']}"
        )
        print(f"[Node 2] {response['payload']}")

        for _ in range(len(tasks)):
            response = decode_message(await reader.readline())

            print(
                f"\n[Node 2] Response received: "
                f"{response['type']}"
            )
            print(f"[Node 2] {response['payload']}")

    finally:
        writer.close()
        await writer.wait_closed()

    print("\n[Node 2] Communication completed!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        asyncio.run(start_server())

    elif len(sys.argv) > 1 and sys.argv[1] == "client":
        asyncio.run(start_client())

    else:
        print("Usage:")
        print("python -m meshweaver.network_node server")
        print("python -m meshweaver.network_node client")
