from flask import Blueprint, jsonify, jsonify, request, current_app
import logging
import time
from app.services.message_queue_service import MessageQueueService
from app.services.proxy_manager_service import ProxyManagerService
from app.services.data_storage_service import data_storage_service
from app.services.task_tracker_service import TaskTrackerService
from app.services.alert_service import AlertService
from app.services.monitoring_service import MonitoringService
from app.workers.task_worker import TaskWorker
from app.logging_config import get_logger
import asyncio
from threading import Lock
from datetime import datetime, timedelta, timezone

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = get_logger('orchestrator')

# Blueprint for Orchestrator API
orchestrator_bp = Blueprint('orchestrator', __name__)

# Initialize Services
message_queue_service = MessageQueueService(queue_name="crawl_tasks")
proxy_manager = ProxyManagerService()
task_tracker = TaskTrackerService()
alert_service = AlertService(
    webhook_url="https://hooks.slack.com/services/your/webhook/url")
monitoring_service = MonitoringService()

MAX_RETRIES = 3  # Maximum number of task retries
# In-memory lock (replace with Redis/Mongo for production)
task_locks = {}
task_locks_lock = Lock()


def allocate_worker():
    """
    Allocate an available worker dynamically using the MonitoringService.

    :return: str, worker ID of the allocated worker, or None if no workers are available
    """
    worker = monitoring_service.get_available_worker()
    if worker:
        logger.info("Worker allocated: %s", worker)
        return worker
    logger.warning("No available workers.")
    return None


def validate_proxy(proxy):
    """
    Validate the assigned proxy to ensure it is functional.

    :param proxy: str, proxy URL
    :return: bool, whether the proxy is valid
    """
    is_valid = proxy_manager.validate_proxy(proxy)
    if not is_valid:
        logger.warning(f"Proxy {proxy} validation failed.")
    return is_valid


def process_task_with_retry(task):
    """
    Process a task with retries, monitoring, and alerting on failure.

    :param task: dict, task to process
    """
    retries = task.get("retries", 0)
    logger.info(f"Processing task {task['task_name']} with {retries} retries.")
    try:
        # Allocate a worker and proxy for the task
        worker_id = allocate_worker()
        if not worker_id:
            logger.warning(
                "No available workers for task %s. Requeuing task.", task["task_name"])
            message_queue_service.publish_task(task)
            monitoring_service.release_worker(worker_id)
            return

        proxy = proxy_manager.get_proxy()
        if not proxy:
            logger.warning(
                "No available proxies for task %s. Requeuing task.", task["task_name"])
            message_queue_service.publish_task(task)
            monitoring_service.release_worker(worker_id)
            return

        # Update task metadata
        task["allocated_worker"] = worker_id
        task["allocated_proxy"] = proxy
        logger.info(
            f"Task {task['task_name']} allocated to worker {worker_id} with proxy {proxy}")

        # Update task status to 'assigned'
        task_tracker.update_task_status(task["task_name"], "assigned", worker_id, proxy,
                                        extra_fields={"allocated_at": datetime.now(timezone.utc).isoformat()})

        # Execute the task
        logger.info(
            f"Executing task {task['task_name']} with worker {worker_id} and proxy {proxy}")
        logger.info(
            f"worker: {worker_id}, orchestrator_url: {current_app.config['DOMAIN'] + 'orchestrator'}")
        worker_instance = TaskWorker(
            worker_id=worker_id,
            orchestrator_url=current_app.config["DOMAIN"] + "orchestrator"
        )
        asyncio.run(worker_instance.execute_task(task))

        logger.info(
            f"Worker completed task {task['task_name']} — no need to update status from orchestrator.")

        data_storage_service.log_task_execution_status(task["task_name"], task.get(
            "allocated_worker"), task.get("allocated_proxy"), "success")
        monitoring_service.release_worker(worker_id)

    except Exception as e:
        retries += 1
        task["retries"] = retries
        logger.error(
            f"Task {task['task_name']} failed on attempt {retries}: {str(e)}")

        if retries <= MAX_RETRIES:
            # Retry task
            task_tracker.update_task_status(
                task["task_name"], f"retrying ({retries}/{MAX_RETRIES})", worker_id, proxy)
            message_queue_service.publish_task(task)
        else:
            # Mark task as failed and send alert
            data_storage_service.log_task_execution_status(task["task_name"], task.get(
                "allocated_worker"), task.get("allocated_proxy"), "failure", error=str(e))
            task_tracker.update_task_status(
                task["task_name"], "failed", worker_id, proxy)
            alert_service.send_alert(
                f"Task {task['task_name']} failed after {MAX_RETRIES} retries.")


def log_task_result(task, status, error=None):
    """
    Log the result of task execution.

    :param task: dict, task metadata
    :param status: str, task execution status
    :param error: str, error message if the task failed
    """
    data_storage_service.log_task_execution_status(task["task_name"], task.get(
        "allocated_worker"), task.get("allocated_proxy"), status, error)


@orchestrator_bp.route('/process_tasks', methods=['GET'])
def process_tasks():
    """
    API to continuously process tasks from the message queue.
    Implements task locking to prevent duplicate processing.
    """
    logger.info("Starting task processing loop...")

    try:
        while True:
            task = message_queue_service.get_task()
            if not task:
                logger.info("No tasks found. Sleeping for 5 seconds...")
                time.sleep(5)
                continue

            task_id = str(task["_id"])
            now = datetime.utcnow()

            # Acquire lock
            with task_locks_lock:
                lock_entry = task_locks.get(task_id)
                if lock_entry and lock_entry > now:
                    logger.warning(
                        f"Task {task_id} is already being processed. Skipping.")
                    continue
                # Set lock with 2-minute timeout
                task_locks[task_id] = now + timedelta(minutes=2)

            # Process the task
            try:
                process_task_with_retry(task)
            except Exception as e:
                logger.exception(
                    f"Unexpected error during processing task {task_id}: {e}")
            finally:
                # Clean up lock after execution
                with task_locks_lock:
                    if task_id in task_locks:
                        del task_locks[task_id]

    except KeyboardInterrupt:
        logger.info("Task processing loop interrupted by user.")

    return jsonify({"status": "stopped", "message": "Task processing stopped."}), 200


@orchestrator_bp.route('/failed_tasks', methods=['GET'])
def get_failed_tasks():
    """
    API to retrieve failed tasks.
    """
    failed_tasks = data_storage_service.get_all_failed_tasks()
    return jsonify({"status": "success", "failed_tasks": failed_tasks}), 200


@orchestrator_bp.route('/workers_status', methods=['GET'])
def get_workers_status():
    """
    API to retrieve the status of all workers.
    """
    try:
        worker_statuses = data_storage_service.get_all_worker_statuses()
        return jsonify({"status": "success", "workers": worker_statuses}), 200
    except Exception as e:
        logger.error(f"Error retrieving worker statuses: {str(e)}")
        return jsonify({"status": "error", "message": "Failed to retrieve worker statuses."}), 500


@orchestrator_bp.route('/update_worker_status', methods=['POST'])
def update_worker_status():
    """
    API to update the status of a specific worker.
    """
    try:
        data = request.get_json()
        worker_id = data.get("worker_id")
        status = data.get("status")

        if not worker_id or not status:
            return jsonify({"status": "error", "message": "Worker ID and status are required."}), 400

         # Save the status to MongoDB
        data_storage_service.save_worker_status(worker_id, status)
        logger.info(f"Worker {worker_id} status updated to {status}.")
        return jsonify({"status": "success", "message": f"Worker {worker_id} status updated to {status}."}), 200
    except Exception as e:
        logger.error(f"Error updating worker status: {str(e)}")
        return jsonify({"status": "error", "message": "Failed to update worker status."}), 500
