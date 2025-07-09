import asyncio
from datetime import datetime
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Type, Literal, Optional, Iterable

from danielutils import AsyncWorkerPool, atomic, AsyncRetryExecutor, ExponentialBackOffStrategy

from jobs import BaseJob, LongRunningJob, Shooker, Prowler

JOBS_JSON_PATH: str = "./jobs.json"
OUTPUT_FILE_NAME: str = "tasks.log"


@dataclass
class Task:
    job_name: str  # can be enum
    payload: Dict


JOB_MAPPING: Dict[str, Type[BaseJob]] = {
    cls.__name__: cls for cls in [
        Prowler, LongRunningJob, Shooker
    ]
}


class MyAsyncWorkerPool(AsyncWorkerPool):
    def __init__(self, pool_name: str, num_workers: int = 5, show_pbar: bool = False) -> None:
        super().__init__(pool_name, num_workers, show_pbar)

    def __enter__(self):
        self.output_file_steam = open(OUTPUT_FILE_NAME, "w")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.output_file_steam.close()

    @atomic
    def print_to_file(self, task: Task, result: str):
        print(f"{datetime.now()} | {task.job_name} | {task.payload} | {result}", file=self.output_file_steam)

    async def worker(self, worker_id) -> None:
        """Worker coroutine that continuously fetches and executes tasks from the queue."""
        while True:
            task = await self._queue.get()
            if task is None:  # Sentinel value to shut down the worker
                break
            func, args, kwargs, name = task
            res = None
            exceptions = []
            # quicker than AsyncRetryExecutor
            for timeout in [1000, 2000, 4000]:
                try:
                    res = await func(*args, **kwargs)
                    break
                except Exception as e:
                    exceptions.append(e)
                    await asyncio.sleep(timeout)

            if self._pbar:
                self._pbar.update(1)
            self._queue.task_done()


async def main() -> None:
    with open(JOBS_JSON_PATH, "r") as f:
        raw_tasks: List[Dict] = json.load(f).get("tasks", [])
    job_name_dct: Dict[str, int] = defaultdict(int)
    processed_tasks: List[Task] = [Task(**task) for task in raw_tasks]
    with MyAsyncWorkerPool("zest", num_workers=os.cpu_count(), show_pbar=True) as pool:
        for task in processed_tasks:
            cls = JOB_MAPPING.get(task.job_name, None)
            if not cls:
                continue

            await pool.submit(cls().execute, args=[task.payload])

        await pool.start()
        await pool.join()


if __name__ == "__main__":
    asyncio.run(main())
