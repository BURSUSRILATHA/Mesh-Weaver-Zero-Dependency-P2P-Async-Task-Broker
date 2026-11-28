
import asyncio
import sys
import itertools

HOST = "127.0.0.1"
PORT = 8888

NUM_WORKERS = 3
MAX_ATTEMPTS = 3
TASK_TIMEOUT_SECONDS = 2

task_queue = asyncio.PriorityQueue()
task_counter = itertools.count()


async def process_task(message, attempt):
    payload = message.get("payload", "")

    print(f"[Node 1] Processing: {payload} (Attempt {attempt})")

    # Simulate a task that takes too long
    if "Slow Task" in payload:
        await asyncio.sleep(5)

    # Simulate a temporary error
    elif "Retry Task" in payload and attempt < 3:
        await asyncio.sleep(1)
        raise Exception("Temporary task error")

    # Simulate a permanent error
    elif "Always Fail" in payload:
        await asyncio.sleep(1)
        raise Exception("Permanent task error")

    else:
        await asyncio.sleep(1)

    return {
        "type": "TASK_COMPLETED",
        "payload": f"{payload} completed successfully"
    }


async def task_worker(worker_id):
    print(f"[Worker {worker_id}] Worker started!")

    while True:
        priority, order, message, writer, attempt = await task_queue.get()

        payload = message.get("payload", "")

        try:
            print(
                f"[Worker {worker_id}] "
                f"Picked up {payload} (Attempt {attempt})"
            )

            response = await asyncio.wait_for(
                process_task(message, attempt),
                timeout=TASK_TIMEOUT_SECONDS
            )

            print(f"[Worker {worker_id}] {payload} completed!")

        except asyncio.TimeoutError:
            print(
                f"[Worker {worker_id}] {payload} timed out "
                f"on attempt {attempt}"
            )

            if attempt < MAX_ATTEMPTS:
                await task_queue.put(
                    (
                        priority,
                        next(task_counter),
                        message,
                        writer,
                        attempt + 1
                    )
                )
                print(f"[Worker {worker_id}] Requeued timed-out task")
                response = None
            else:
                response = {
                    "type": "TASK_TIMEOUT",
                    "payload": (
                        f"{payload} timed out after "
                        f"{MAX_ATTEMPTS} attempts"
                    )
                }

        except Exception as error:
            print(f"[Worker {worker_id}] Task failed: {error}")

            if attempt < MAX_ATTEMPTS:
                await task_queue.put(
                    (
                        priority,
                        next(task_counter),
                        message,
                        writer,
                        attempt + 1
                    )
                )
                print(f"[Worker {worker_id}] Task requeued")
                response = None
            else:
                response = {
                    "type": "TASK_FAILED",
                    "payload": (
                        f"{payload} failed after "
                        f"{MAX_ATTEMPTS} attempts"
                    )
                }

        if response is not None:
            try:
                import json
                writer.write(
                    (json.dumps(response) + "\n").encode()
                )
                await writer.drain()
            except (ConnectionError, OSError):
                print("[Worker] Client disconnected before response")

        task_queue.task_done()


async def handle_client(reader, writer):
    import json

    address = writer.get_extra_info("peername")
    print(f"[Node 1] Connected to {address}")

    try:
        while True:
            data = await reader.readline()

            if not data:
                break

            message = json.loads(data.decode())
            message_type = message.get("type")

            if message_type == "TASK":
                priority = message.get("priority", 3)

                if not isinstance(priority, int) or not 1 <= priority <= 5:
                    response = {
                        "type": "ERROR",
                        "payload": "Priority must be between 1 and 5"
                    }
                    writer.write((json.dumps(response) + "\n").encode())
                    await writer.drain()
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
                    f"[Node 1] Task received: "
                    f"{message.get('payload')} (Priority {priority})"
                )

            elif message_type == "PING":
                response = {
                    "type": "PONG",
                    "payload": "Connection is active"
                }
                writer.write((json.dumps(response) + "\n").encode())
                await writer.drain()

    except (ConnectionError, OSError, json.JSONDecodeError) as error:
        print(f"[Node 1] Client communication error: {error}")

    finally:
        writer.close()
        await writer.wait_closed()
        print("[Node 1] Client disconnected")


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
    print(f"[Node 1] Started {NUM_WORKERS} workers!")
    print(f"[Node 1] Task timeout: {TASK_TIMEOUT_SECONDS} seconds")
    print(f"[Node 1] Maximum attempts: {MAX_ATTEMPTS}\n")

    async with server:
        try:
            await server.serve_forever()
        finally:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)


async def start_client():
    import json

    print("[Node 2] Connecting to Node 1...")

    reader, writer = await asyncio.open_connection(HOST, PORT)

    print("[Node 2] Connected successfully!\n")

    tasks = [
        ("Normal Task", 3),
        ("Retry Task", 1),
        ("Always Fail Task", 2),
        ("Slow Task", 1)
    ]

    for payload, priority in tasks:
        message = {
            "type": "TASK",
            "payload": payload,
            "priority": priority
        }

        writer.write((json.dumps(message) + "\n").encode())
        await writer.drain()

        print(f"[Node 2] Task sent: {payload} (Priority {priority})")

    ping = {"type": "PING"}
    writer.write((json.dumps(ping) + "\n").encode())
    await writer.drain()

    print("[Node 2] PING message sent!\n")

    # Read the PONG response first
    response = json.loads((await reader.readline()).decode())
    print(f"[Node 2] Response received: {response['type']}")
    print(response["payload"], "\n")

    # Read one final response for each submitted task
    for _ in tasks:
        data = await reader.readline()

        if not data:
            print("[Node 2] Connection closed unexpectedly")
            break

        response = json.loads(data.decode())

        print(f"[Node 2] Response received: {response['type']}")
        print(response["payload"], "\n")

    print("[Node 2] Communication completed!")

    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        asyncio.run(start_server())
    elif len(sys.argv) > 1 and sys.argv[1] == "client":
        asyncio.run(start_client())
    else:
        print("Usage:")
        print("python -m meshweaver.network_node server")
        print("python -m meshweaver.network_node client")
