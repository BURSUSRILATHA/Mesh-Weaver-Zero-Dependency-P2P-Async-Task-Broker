
import asyncio
import time


class MeshWeaverNode:
    def __init__(self, node_name):
        self.node_name = node_name
        self.tasks = []

    async def process_task(self, task_id):
        print(f"[{self.node_name}] Processing Task {task_id}")

        await asyncio.sleep(2)

        print(f"[{self.node_name}] Task {task_id} completed")

        return f"Task {task_id} successful"

    async def start(self):
        print(f"\n{self.node_name} started successfully!")

        self.tasks = [
            self.process_task(1),
            self.process_task(2),
            self.process_task(3)
        ]

        results = await asyncio.gather(*self.tasks)

        print("\nAll tasks completed!")
        for result in results:
            print(result)


async def main():
    node = MeshWeaverNode("MeshWeaver-Node-1")
    await node.start()


if __name__ == "__main__":
    asyncio.run(main())
