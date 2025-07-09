import asyncio
from datetime import datetime
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Type, Optional, Any, Iterator

from danielutils import AsyncWorkerPool

from jobs import BaseJob, LongRunningJob, Shooker, Prowler

JOBS_JSON_PATH: str = "./jobs.json"
OUTPUT_FILE_NAME: str = "tasks.log"


@dataclass
class Task:
    job_name: str
    payload: Dict


def get_job_class_mapping() -> Dict[str, Type[BaseJob]]:
    return {
        cls.__name__: cls for cls in [Prowler, LongRunningJob, Shooker]
    }


class MissingJobHandlerException(Exception):
    pass


class BackoffStrategy:
    def __iter__(self) -> Iterator[float]:
        raise NotImplementedError


class FixedSecondsBackoffStrategy(BackoffStrategy):
    def __init__(self, delays: List[float]) -> None:
        self.delays = delays

    def __iter__(self) -> Iterator[float]:
        return iter(self.delays)


def build_log_message(status: str, messages: List[Any]) -> dict:
    if len(messages) == 1:
        return {"status": status, "msg": messages[0]}
    return {"status": status, "msg": messages}


class TaskWorkerPool(AsyncWorkerPool):
    def __init__(
            self,
            pool_name: str,
            num_workers: int = os.cpu_count() or 4,
            show_progress_bar: bool = False,
            backoff_strategy: Optional[BackoffStrategy] = None
    ) -> None:
        super().__init__(pool_name, num_workers, show_progress_bar)
        self._log_lock = asyncio.Lock()
        self._results: List[Any] = []
        self._task_lookup: Dict[str, Task] = {}
        self._backoff_strategy = backoff_strategy or FixedSecondsBackoffStrategy([1, 2, 4])

    def submit_task(
            self,
            func: Any,
            args: Optional[List[Any]] = None,
            kwargs: Optional[Dict[str, Any]] = None,
            task_id: Optional[str] = None,
            task_obj: Optional[Task] = None
    ) -> Any:
        if task_id is not None and task_obj is not None:
            self._task_lookup[task_id] = task_obj
        return super().submit(func, args=args, kwargs=kwargs, name=task_id)

    def __enter__(self) -> 'TaskWorkerPool':
        self.output_file_stream = open(OUTPUT_FILE_NAME, "w")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.output_file_stream.close()

    async def _log_to_file(self, job_name: str, payload: Dict, message: Dict) -> None:
        async with self._log_lock:
            print(f"{datetime.now()} | {job_name} | {payload} | {str(message)}", file=self.output_file_stream)
            self.output_file_stream.flush()

    async def _handle_missing_job(self, task_obj: Optional[Task]) -> None:
        job_name = getattr(task_obj, 'job_name', 'UNKNOWN')
        payload = getattr(task_obj, 'payload', {})
        msg = build_log_message("failed",
                                [f"No handler found for job name: '{job_name}'. Please check your job configuration."])
        await self._log_to_file(job_name, payload, msg)
        self._results.append((task_obj, None, False, 0))

    async def _run_with_retries(
            self,
            func: Any,
            args: List[Any],
            kwargs: Dict[str, Any],
            task_obj: Optional[Task]
    ) -> Any:
        result = None
        attempt_messages: List[Any] = []
        success = False
        execution_time = 0
        for delay in iter(self._backoff_strategy):
            start = datetime.now()
            try:
                result = await func(*args, **kwargs)
                execution_time = (datetime.now() - start).total_seconds()
                if isinstance(result, dict) and result.get("status") == "success":
                    attempt_messages.append(result.get("result"))
                    success = True
                    break
                else:
                    attempt_messages.append(result.get("result") if isinstance(result, dict) else str(result))
            except MissingJobHandlerException:
                await self._handle_missing_job(task_obj)
                self._task_done()
                return None, None, None, None, True
            except Exception as e:
                execution_time = (datetime.now() - start).total_seconds()
                attempt_messages.append(str(e))
                await asyncio.sleep(delay)
        return result, attempt_messages, success, execution_time, False

    async def _log_task_result(
            self,
            task_obj: Optional[Task],
            result: Any,
            attempt_messages: List[Any],
            success: bool,
            execution_time: float
    ) -> None:
        status = "success" if success else "failed"
        msg = build_log_message(status, attempt_messages)
        job_name = getattr(task_obj, 'job_name', 'UNKNOWN')
        payload = getattr(task_obj, 'payload', {})
        await self._log_to_file(job_name, payload, msg)
        self._results.append((task_obj, result, success, execution_time))

    def _task_done(self):
        if self._pbar:
            self._pbar.update(1)
        self._queue.task_done()

    async def worker(self, worker_id: int) -> None:
        while True:
            task = await self._queue.get()
            if task is None:  # sentinal value
                break
            func, args, kwargs, task_id = task
            task_key = str(task_id) if task_id is not None else ''
            task_obj = self._task_lookup.get(task_key)
            if func is None:
                await self._handle_missing_job(task_obj)
                self._task_done()
                continue
            ret = await self._run_with_retries(func, list(args or []), dict(kwargs or {}), task_obj)
            result, attempt_messages, success, execution_time, handled = ret
            if handled:
                continue
            await self._log_task_result(task_obj, result, attempt_messages, success, execution_time)
            self._task_done()


def load_tasks_from_json(path: str) -> List[Task]:
    with open(path, "r") as f:
        raw_tasks: List[Dict] = json.load(f).get("tasks", [])
    return [Task(**task) for task in raw_tasks]


def get_job_executor(job_class: Optional[Type[BaseJob]]) -> Any:
    async def job_func(payload: Dict) -> Any:
        if job_class is None:
            raise MissingJobHandlerException()
        return await job_class().execute(payload)

    return job_func


async def main() -> None:
    job_class_mapping = get_job_class_mapping()
    tasks = load_tasks_from_json(JOBS_JSON_PATH)
    with TaskWorkerPool("zest", show_progress_bar=True) as pool:
        for idx, task in enumerate(tasks):
            job_class = job_class_mapping.get(task.job_name, None)
            job_executor = get_job_executor(job_class)
            await pool.submit_task(job_executor, args=[task.payload], kwargs=None, task_id=str(idx), task_obj=task)
        start_time = datetime.now()
        await pool.start()
        await pool.join()
    end_time = datetime.now()
    total_tasks = len(tasks)
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
