from flask import Blueprint, jsonify, request
import logging
from random import random, choice
import time
from app.services.message_queue_service import MessageQueueService
from app.services.proxy_manager_service import ProxyManagerService
from app.services.validation_service import validate_frequency, validate_task_fields


# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Blueprint for Scheduler API
scheduler_bp = Blueprint('scheduler', __name__)

# Initialize Message Queue Service
mq_service = MessageQueueService()

# Initialize Proxy Manager
proxy_manager = ProxyManagerService()
# Load additional proxies dynamically if needed
additional_proxies = [
    "http://185.199.229.156:80",
    "http://49.0.2.242:8090"
]
proxy_manager.load_proxies(additional_proxies)
logger.info(f"All proxies: {proxy_manager.get_all_proxies()}")

# Mock task queue and executed task log
task_queue = []
executed_tasks = []

# Mock resources
available_workers = ["Worker-1", "Worker-2", "Worker-3"]
available_proxies = ["Proxy-1", "Proxy-2", "Proxy-3"]

def validate_task(task):
    """
    Validate task details.

    :param task: dict, task details
    :return: tuple (is_valid: bool, message: str)
    """
    is_valid, message = validate_task_fields(task, ["task_name", "url", "frequency"])
    if not is_valid:
        return False, message

    return validate_frequency(task["frequency"])

def prioritize_tasks():
    """
    Prioritize tasks in the queue by converting frequency to minutes and sorting in ascending order.
    """
    def convert_to_minutes(frequency):
        """
        Convert frequency to minutes.

        :param frequency: str, frequency in the format '<number> <unit>'
        :return: int, equivalent frequency in minutes
        """
        unit_to_minutes = {"mins": 1, "hours": 60, "days": 1440, "weeks": 10080}  # Conversion factors

        is_valid, error_message = validate_frequency(frequency)
        if not is_valid:
            logger.error(f"Invalid frequency: {error_message}")
            return float('inf')  # Assign lowest priority for invalid frequencies

        value, unit = frequency.split()
        return int(value) * unit_to_minutes[unit]

    try:
        # Sort tasks based on their converted frequency in minutes
        task_queue.sort(key=lambda task: convert_to_minutes(task.get("frequency", "")))
        logger.info("Tasks prioritized successfully.")
    except Exception as e:
        logger.error(f"Error prioritizing tasks: {str(e)}")

def monitor_and_adjust(task):
    """
    Monitor task execution and make adjustments if necessary.

    :param task: dict, the task being monitored
    """
    logger.info("Monitoring task execution for task: %s", task)

    # Example adjustment logic: Retry failed tasks
    if task.get("status") == "failure":
        retries = task.get("retries", 0)
        if retries < 3:  # Retry up to 3 times
            logger.warning("Retrying failed task: %s (Retry #%d)", task["task_name"], retries + 1)
            task["retries"] = retries + 1
            task_queue.append(task)  # Requeue the task
            prioritize_tasks()  # Reprioritize after requeuing
        else:
            logger.error("Max retries reached for task: %s", task["task_name"])
    else:
        logger.info("Task executed successfully, no adjustments needed.")

def log_results(task, status):
    """
    Log the results of task execution.

    :param task: dict, the task being logged
    :param status: str, the status of the task execution (e.g., 'success', 'failure')
    """
    if status == "success":
        logger.info(
            "Task Result - Name: %s, URL: %s, Status: %s, Execution Time: %.2f seconds",
            task["task_name"],
            task["url"],
            status,
            task.get("execution_time", 0),
        )
    elif status == "failure":
        logger.error(
            "Task Failed - Name: %s, URL: %s, Error: %s",
            task["task_name"],
            task["url"],
            task.get("error", "Unknown error"),
        )

@scheduler_bp.route('/schedule_task', methods=['POST'])
def schedule_task():
    """API endpoint to receive and schedule a task."""
    task = request.json
    logger.info("Received task for scheduling: %s", task)

    # Step 1: Validate Task
    is_valid, message = validate_task(task)
    if not is_valid:
        logger.error("Task validation failed: %s", message)
        return jsonify({"status": "error", "message": message}), 400

    # Step 2: Add Task to Queue
    task_queue.append(task)
    logger.info("Task added to queue: %s", task)

    # Step 3: Prioritize Tasks
    prioritize_tasks()

    # Step 4: Publish the Task to RabbitMQ
    try:
        mq_service.publish_task(task)
        logger.info("Task published to RabbitMQ: %s", task)
        return jsonify({"status": "success", "message": "Task scheduled successfully.", "task": task}), 200
    except Exception as e:
        logger.error("Failed to schedule task: %s", str(e))
        return jsonify({"status": "error", "message": "Failed to schedule task."}), 500

@scheduler_bp.route('/get_task_queue', methods=['GET'])
def get_task_queue():
    """API endpoint to retrieve all tasks in the queue."""
    logger.info("Fetching all tasks in the queue. Total: %d", len(task_queue))
    return jsonify({"status": "success", "data": task_queue}), 200

@scheduler_bp.route('/get_executed_tasks', methods=['GET'])
def get_executed_tasks():
    """
    API endpoint to retrieve all executed tasks.

    :return: JSON response containing a list of executed tasks
    """
    logger.info("Fetching all executed tasks. Total: %d", len(executed_tasks))
    
    # Return all executed tasks with success and failure details
    return jsonify({
        "status": "success",
        "total_executed": len(executed_tasks),
        "executed_tasks": executed_tasks
    }), 200

@scheduler_bp.route('/clear_queue', methods=['POST'])
def clear_task_queue():
    """API endpoint to clear the task queue."""
    global task_queue
    task_queue = []
    logger.info("Task queue cleared.")
    return jsonify({"status": "success", "message": "Task queue cleared successfully."}), 200

@scheduler_bp.route('/clear_executed', methods=['POST'])
def clear_executed_tasks():
    """API endpoint to clear the executed tasks log."""
    global executed_tasks
    executed_tasks = []
    logger.info("Executed tasks log cleared.")
    return jsonify({"status": "success", "message": "Executed tasks log cleared successfully."}), 200
