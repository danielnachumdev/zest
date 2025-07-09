# ZEST Task

**Notes to Reviewer**

- In a production environment, I would have refactored the codebase into a more modular and maintainable structure, rather than consolidating all logic into a single file. This approach facilitates scalability, testing, and long-term maintainability.
- I have proactively started developing a more extensible and future-proof solution, as I believe it is important to demonstrate my ability to design for scalability and robustness, not just to deliver a minimal working solution. While it is straightforward to produce a basic implementation with AI assistance, my goal here is to showcase the level of quality and architectural thinking I can bring to a project when given the opportunity and time.
- The mock Job classes were adapted to support asynchronous execution. In a real-world scenario, job implementations would be designed with async compatibility from the outset, so this adjustment is both practical and realistic.
- For future extensibility, adopting Python's built-in `logging` module would enable more advanced log management, support for multiple outputs, and flexible formatting as project requirements evolve.
- For full transparency, I encourage you to review the commit history to distinguish between work completed independently and sections where AI assistance was utilized.


## Actual Strategy & Design

### Stage 1: Basic Task Dispatch
- **Task Loading:**
  - Tasks are loaded from a `jobs.json` file, each specifying a job name and a payload.
  - Each task is represented by a `Task` dataclass.
- **Job Dispatch:**
  - A fixed mapping associates job names to their corresponding classes.
  - For each task, the job class is instantiated and its `execute(payload)` method is called.
- **Logging:**
  - Each job's result, along with a timestamp, job name, payload, and result, is logged as a single line in `tasks.log`.

### Stage 2: Asynchronous Processing
- **Parallel Execution:**
  - Jobs are processed in parallel using an asynchronous worker pool (`AsyncWorkerPool` from `danielutils`).
  - The number of workers is set to the number of logical CPU cores (`os.cpu_count()`).
- **Task Queue:**
  - Tasks are submitted to the pool and distributed among workers.
- **Process-Safe Logging:**
  - An `asyncio.Lock` is used to ensure that only one worker writes to the log file at a time, preventing interleaved or corrupted log entries.

### Stage 3: Robust Error Handling & Retries
- **Retry Logic:**
  - If a job fails (by returning a non-success status or raising an exception), it is retried up to 3 times with exponential backoff delays (1s, 2s, 4s).
  - All retry attempts and their results are logged.
- **Failure Handling:**
  - If all retries fail, the failure is logged with a status of "failed" and the exception message.
- **Summary Reporting:**
  - After all tasks are processed, a summary is printed and logged, including total tasks, successes, failures, total runtime, and average execution time per task.

## Notable Design Choices
- **Explicit Job Mapping:**
  - Job classes are mapped by name in a dictionary, making it easy to add or remove job types.
- **Async Worker Pool:**
  - The use of an asynchronous worker pool allows for efficient parallel processing without manual thread/process management.
- **Structured Logging:**
  - Logging is consistent and process-safe, with each entry containing all relevant information for traceability.
- **Separation of Concerns:**
  - The code is modular, with clear separation between task loading, job execution, logging, and summary reporting.


**Author:** [danielnachumdev] 