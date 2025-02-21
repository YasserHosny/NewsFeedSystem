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

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = get_logger('orchestrator')

# Blueprint for Orchestrator API
orchestrator_bp = Blueprint('orchestrator', __name__)

# Initialize Services
mq_service = MessageQueueService()
proxy_manager = ProxyManagerService()
task_tracker = TaskTrackerService()
alert_service = AlertService(webhook_url="https://hooks.slack.com/services/your/webhook/url")
monitoring_service = MonitoringService()

MAX_RETRIES = 3  # Maximum number of task retries
# Internal store for worker statuses
worker_status_store = {
    "worker-1": "available",
    "worker-2": "busy",
    "worker-3": "available"
}
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

def release_worker(worker):
    """
    Release a worker back to the available pool.

    :param worker: str, worker ID to release
    """
    if worker:
        logger.info("Worker released: %s", worker)
    else:
        logger.warning("Attempted to release a worker that is not allocated.")

def process_task_with_retry(task):
    """
    Process a task with retries, monitoring, and alerting on failure.

    :param task: dict, task to process
    """
    retries = task.get("retries", 0)

    try:
        # Allocate a worker and proxy for the task
        worker = allocate_worker()
        if not worker:
            logger.warning("No available workers for task %s. Requeuing task.", task["task_name"])
            mq_service.publish_task(task)
            return

        proxy = proxy_manager.get_proxy()
        if not proxy:
            logger.warning("No available proxies for task %s. Requeuing task.", task["task_name"])
            mq_service.publish_task(task)
            release_worker(worker)
            return

        # Update task metadata
        task["allocated_worker"] = worker
        task["allocated_proxy"] = proxy

        # Update task status to 'in-progress'
        task_tracker.update_task_status(task["task_name"], "in-progress")

        # Execute the task
        logger.info(f"Executing task {task['task_name']} with worker {worker} and proxy {proxy}")
        worker_instance = TaskWorker(
            worker_id=worker.id,
            orchestrator_url=current_app.config["DOMAIN"] + "/orchestrator"
        )
        worker_instance.execute_task(task)

        # Log success and update task status
        log_task_result(task, "success")
        task_tracker.update_task_status(task["task_name"], "completed")
        release_worker(worker)

    except Exception as e:
        retries += 1
        task["retries"] = retries
        logger.error(f"Task {task['task_name']} failed on attempt {retries}: {str(e)}")

        if retries <= MAX_RETRIES:
            # Retry task
            task_tracker.update_task_status(task["task_name"], f"retrying ({retries}/{MAX_RETRIES})")
            mq_service.publish_task(task)
        else:
            # Mark task as failed and send alert
            log_task_result(task, "failure", error=str(e))
            task_tracker.update_task_status(task["task_name"], "failed")
            alert_service.send_alert(f"Task {task['task_name']} failed after {MAX_RETRIES} retries.")

def log_task_result(task, status, error=None):
    """
    Log the result of task execution.

    :param task: dict, task metadata
    :param status: str, task execution status
    :param error: str, error message if the task failed
    """
    result = {
        "task_name": task["task_name"],
        "status": status,
        "allocated_worker": task.get("allocated_worker"),
        "allocated_proxy": task.get("allocated_proxy"),
        "error": error,
        "timestamp": time.time()
    }
    data_storage_service.save_task_result(result)  # Save result to MongoDB
    logger.info("Task result logged: %s", result)

@orchestrator_bp.route('/process_tasks', methods=['POST'])
def process_tasks():
    """
    API to process tasks from the message queue.
    """
    logger.info("Starting task processing...")
    while True:
        task = mq_service.get_task()  # Retrieve a task from the message queue
        if not task:
            logger.info("No more tasks in the queue.")
            break

        process_task_with_retry(task)

    return jsonify({"status": "success", "message": "All tasks processed."}), 200

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