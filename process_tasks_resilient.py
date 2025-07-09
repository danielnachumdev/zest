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
        # Fix: os.cpu_count() can return None, so default to 4 if None
        if num_workers is None:
            num_workers = 4
        super().__init__(pool_name, num_workers, show_pbar)
        self._log_lock = asyncio.Lock()
        self._results = []  # To store (task, result, success, exec_time)
        self._task_map = {}  # index -> Task

    def __enter__(self):
        self.output_file_steam = open(OUTPUT_FILE_NAME, "w")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.output_file_steam.close()

    async def _print_to_file(self, job_name: str, payload: dict = {}, message: dict = {}):
        async with self._log_lock:
            print(f"{datetime.now()} | {job_name} | {payload} | {str(message)}", file=self.output_file_steam)
            self.output_file_steam.flush()

    async def _handle_missing_handler(self, task_obj):
        job_name = getattr(task_obj, 'job_name', 'UNKNOWN')
        payload = getattr(task_obj, 'payload', {})
        msg = {"status": "failed", "msg": [f"No handler found for job name: '{job_name}'. Please check your job configuration."]}
        await self._print_to_file(job_name, payload, msg)
        self._results.append((task_obj, None, False, 0))
        if self._pbar:
            self._pbar.update(1)
        self._queue.task_done()

    async def _execute_with_retries(self, func, args, kwargs, task_obj):
        res = None
        attempt_msgs = []
        success = False
        exec_time = 0
        for i, timeout in enumerate([1, 2, 4]):
            start = datetime.now()
            try:
                res = await func(*args, **(kwargs or {}))
                exec_time = (datetime.now() - start).total_seconds()
                if isinstance(res, dict) and res.get("status") == "success":
                    msg_val = res.get("result")
                    attempt_msgs.append(msg_val)
                    success = True
                    break
                else:
                    msg_val = res.get("result") if isinstance(res, dict) else str(res)
                    attempt_msgs.append(msg_val)
            except MissingJobHandlerException:
                await self._handle_missing_handler(task_obj)
                return None, None, None, None, True  # handled, skip further processing
            except Exception as e:
                exec_time = (datetime.now() - start).total_seconds()
                attempt_msgs.append(str(e))
                if i < 2:
                    await asyncio.sleep(timeout)
        return res, attempt_msgs, success, exec_time, False

    async def _log_result(self, task_obj, res, attempt_msgs, success, exec_time):
        overall_status = "success" if success else "failed"
        msg = {"status": overall_status, "msg": attempt_msgs}
        job_name = getattr(task_obj, 'job_name', 'UNKNOWN')
        payload = getattr(task_obj, 'payload', {})
        await self._print_to_file(job_name, payload, msg)
        self._results.append((task_obj, res, success, exec_time))
        if self._pbar:
            self._pbar.update(1)
        self._queue.task_done()

    async def worker(self, worker_id) -> None:
        """Worker coroutine that continuously fetches and executes tasks from the queue."""
        while True:
            task = await self._queue.get()
            if task is None:  # Sentinel value to shut down the worker
                break
            func, args, kwargs, name = task
            task_obj = self._task_map.get(name)
            if func is None:
                await self._handle_missing_handler(task_obj)
                continue
            res, attempt_msgs, success, exec_time, handled = await self._execute_with_retries(func, args, kwargs, task_obj)
            if handled:
                continue
            await self._log_result(task_obj, res, attempt_msgs, success, exec_time)

class MissingJobHandlerException(Exception):
    pass

async def main() -> None:
    with open(JOBS_JSON_PATH, "r") as f:
        raw_tasks: List[Dict] = json.load(f).get("tasks", [])
    processed_tasks: List[Task] = [Task(**task) for task in raw_tasks]
    num_workers = os.cpu_count() or 4
    with MyAsyncWorkerPool("zest", num_workers=num_workers, show_pbar=True) as pool:
        for idx, task in enumerate(processed_tasks):
            pool._task_map[str(idx)] = task
            cls = JOB_MAPPING.get(task.job_name, None)
            async def job_func(payload, cls=cls):
                if cls is None:
                    raise MissingJobHandlerException()
                return await cls().execute(payload)
            await pool.submit(job_func, args=[task.payload], kwargs=None, name=str(idx))
        start_time = datetime.now()
        await pool.start()
        await pool.join()
    end_time = datetime.now()
    # Summary
    total_tasks = len(processed_tasks)
    successes = sum(1 for _, _, success, _ in pool._results if success)
    failures = total_tasks - successes
    total_runtime = (end_time - start_time).total_seconds()
    avg_exec_time = sum(exec_time for _, _, _, exec_time in pool._results) / total_tasks if total_tasks else 0
    print("\n--- SUMMARY ---")
    print(f"Total tasks: {total_tasks}")
    print(f"Successes: {successes}")
    print(f"Failures: {failures}")
    print(f"Total runtime: {total_runtime:.2f} seconds")
    print(f"Average execution time per task: {avg_exec_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
