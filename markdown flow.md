# 🧭 News Feed Task Execution Flow

## 1. Task Submission
- **Component**: `config_api.py`
- **Action**: User submits configuration
- **Storage**: `config_repo` via `save_config()`
- **Next**: Scheduler picks up the task

---

## 2. Task Scheduling (Every 30s)
- **Component**: `scheduler_service.py`
- **Checks**:
  - Not already scheduled/in-progress
  - Task is due based on frequency
  - Not recently queued (uses `queued_at`)
- **Actions**:
  - Set status → `scheduled`
  - Update `last_execution_time`, `queued_at`
  - Publish to `crawl_tasks` queue
  - Notify `/orchestrator/process_tasks`

---

## 3. Task Orchestration
- **Component**: `orchestrator.py`
- **Actions**:
  - Get task from RabbitMQ
  - Lock task in memory
  - Allocate `worker_id`, `proxy`
  - Set status → `assigned`
  - Record `allocated_at`
  - Launch worker with `TaskWorker.execute_task(...)`

---

## 4. Task Execution
- **Component**: `task_worker.py`
- **On Start**:
  - Set status → `in-progress`
  - Record `started_at`, `retry_count`
- **Work**:
  - Scrape data using Playwright
  - Save results via `save_crawl_items()` → `crawl_repo`
- **On Success**:
  - Set status → `completed`
  - Record `finished_at`, `retry_count`
- **On Failure**:
  - Set status → `failed`
  - Record `error_message`, `retry_count`
  - Raise error → task requeued

---

## 5. Execution Logging
- **Component**: `orchestrator.py → log_task_result()`
- **Storage**: `task_execution_log` via `save_task_result()`
- **Fields**:
  - `task_name`, `worker_id`, `proxy`, `status`, `error`, `timestamp`
