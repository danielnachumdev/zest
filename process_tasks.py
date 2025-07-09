from datetime import datetime
import json
from dataclasses import dataclass
from typing import Dict, List, Type
from tqdm import tqdm
from jobs import BaseJob, Prowler, LongRunningJob, Shooker

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


def print_to_file(task: Task, result: str, fs):
    print(f"{datetime.now()} | {task.job_name} | {task.payload} | {result}", file=fs)


def main() -> None:
    with open(JOBS_JSON_PATH, "r") as f:
        raw_tasks: List[Dict] = json.load(f).get("tasks", [])

    # assuming valid input
    processed_tasks: List[Task] = [Task(**task) for task in raw_tasks]

    with open(OUTPUT_FILE_NAME, "w") as f:
        for task in tqdm(processed_tasks, total=len(processed_tasks), desc="Tasks"):
            cls = JOB_MAPPING.get(task.job_name, None)
            if not cls:
                print_to_file(task, f"Job type {task.job_name} is not supported", fs=f)
                tqdm.write(f"Skipped {task.job_name}")
                continue

            res = ""
            try:
                res = cls().execute(task.payload)
            except Exception as e:
                res = str(e)
            finally:
                f.write(f"{datetime.now()} | {task.job_name} | {task.payload} | {res}\n")
            tqdm.write(f"Processed {task.job_name}")


if __name__ == '__main__':
    main()
