from apscheduler.schedulers.background import BackgroundScheduler
from .monitoring_service import MonitoringService
from .message_queue_service import MessageQueueService
from .alert_service import AlertService
import logging
import requests
from flask import current_app
from .data_storage_service import data_storage_service
from app.services.validation_service import convert_to_minutes
import time
from datetime import datetime, timedelta, timezone
from app.services.task_tracker_service import TaskTrackerService

# Initialize Logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Services
alert_service = AlertService(
    webhook_url="https://hooks.slack.com/services/your/webhook/url")
message_queue_service = MessageQueueService(queue_name="crawl_tasks")
monitoring_service = MonitoringService()
task_tracker = TaskTrackerService()


def start_scheduler(app):
    """
    Start the APScheduler background scheduler for periodic health checks and task scheduling.
    Ensures Flask application context is available for all scheduled jobs.
    """
    logger.info("Starting APScheduler for health monitoring tasks...")

    scheduler = BackgroundScheduler()
    # Clear the crawl queue on startup
    result = message_queue_service.purge_crawl_queue()
    logger.info(f"Cleared {result} tasks from the crawl queue.")

    # Add periodic jobs
    scheduler.add_job(
        func=lambda: run_in_app_context(app, enhanced_worker_health_check),
        trigger="interval",
        seconds=30,
        id="worker_health_check",
        misfire_grace_time=15
    )
    scheduler.add_job(
        func=lambda: run_in_app_context(
            app, lambda: enhanced_proxy_health_check()),
        trigger="interval",
        seconds=60,
        id="proxy_health_check",
        misfire_grace_time=30
    )
    scheduler.add_job(
        func=lambda: run_in_app_context(
            app, lambda: enhanced_queue_health_check("crawl_tasks")),
        trigger="interval",
        seconds=60,
        id="queue_health_check",
        misfire_grace_time=30
    )
    scheduler.add_job(
        func=lambda: run_in_app_context(app, schedule_tasks),
        trigger="interval",
        seconds=30,
        id="task_scheduling",
        misfire_grace_time=15
    )

    # Start the scheduler
    scheduler.start()
    logger.info(
        "Scheduler started with periodic health monitoring and task scheduling jobs.")


def run_in_app_context(app, func):
    """
    Helper function to run a job within the Flask application context.

    :param app: Flask application instance
    :param func: Function to run within the application context
    """
    with app.app_context():
        func()


def schedule_tasks():
    """ 
    Schedule tasks based on their defined frequency and push only due tasks to the queue.
    Avoids repetitive DB calls by pre-filtering tasks that are already in 'scheduled' or 'in-progress'.
    """
    logger.info("Checking tasks for execution based on frequency...")

    try:
        # Fetch all tasks NOT already in 'scheduled' or 'in-progress' status
        eligible_tasks_cursor = data_storage_service.get_eligible_configs_for_scheduling()

        tasks = sorted(
            [{**doc, "_id": str(doc["_id"])} for doc in eligible_tasks_cursor],
            key=lambda t: convert_to_minutes(t["frequency"])
        )

        logger.info(f"Number of eligible tasks: {len(tasks)}")
        current_time = datetime.now(timezone.utc)

        for task in tasks:
            task_name = task["task_name"]
            frequency = task.get("frequency", "30 mins")
            last_execution_time = task.get("last_execution_time")

            if last_execution_time:
                if isinstance(last_execution_time, str):
                    last_execution_time = datetime.fromisoformat(
                        last_execution_time).replace(tzinfo=timezone.utc)
                else:
                    last_execution_time = last_execution_time.replace(
                        tzinfo=timezone.utc)
            else:
                last_execution_time = datetime.min.replace(tzinfo=timezone.utc)

            interval = convert_to_minutes(frequency)
            next_execution_time = last_execution_time + \
                timedelta(minutes=interval)

            logger.info(
                f"Evaluating task: {task_name}, Frequency: {frequency}, "
                f"Last Execution: {last_execution_time}, Next Execution: {next_execution_time}"
            )

            # Skip if task is not due
            if current_time < next_execution_time:
                logger.info(f"Task {task_name} is not due yet.")
                continue

            # Check for available workers
            available_worker = monitoring_service.get_available_worker()
            if not available_worker:
                logger.warning(
                    f"No available workers. Task {task_name} is delayed.")
                continue

            # Check if task was recently queued, Deduplication by queued_at
            queued_at = task.get("queued_at")
            if queued_at:
                # Handle if queued_at is string (from DB) or datetime (in-memory)
                if isinstance(queued_at, str):
                    try:
                        queued_at = datetime.fromisoformat(queued_at)
                    except ValueError:
                        logger.warning(
                            f"Invalid queued_at format for task {task_name}. Skipping deduplication.")
                        queued_at = None

            if queued_at and (current_time - queued_at) < timedelta(seconds=30):
                logger.warning(
                    f"Task {task_name} was queued recently at {queued_at}. Skipping requeue.")
                continue

            # Mark as scheduled
            task_tracker.update_task_status(
                task_name=task_name,
                status="scheduled",
                last_execution_time=current_time.isoformat(),
                worker_id=None,
                proxy=None,
                extra_fields={"queued_at": current_time.isoformat()}
            )

            # Push to message queue
            logger.info(
                f"Pushing task {task_name} to message queue. Task: {task}")
            message_queue_service.publish_task(task)

            # Notify orchestrator
            try:
                response = requests.get(
                    current_app.config["DOMAIN"] + "/orchestrator/process_tasks", timeout=10
                )
                if response.status_code == 200:
                    logger.info(
                        f"Task {task_name} successfully dispatched to orchestrator.")
                    data_storage_service.update_task_config(
                        task_name,
                        {"last_orchestrated_at": datetime.now(
                            timezone.utc).isoformat()}
                    )
                else:
                    logger.warning(
                        f"Orchestrator responded with status {response.status_code}")
                    if should_requeue_task(task_name):
                        logger.warning(
                            f"Requeuing task {task_name} due to orchestrator error.")
                        task_tracker.update_task_status(
                            task_name=task_name,
                            status="scheduled",
                            last_execution_time=current_time,
                            worker_id=None,
                            proxy=None,
                            extra_fields={
                                "queued_at": datetime.now(timezone.utc).isoformat()}
                        )
                        time.sleep(5)
                        message_queue_service.publish_task(task)

            except requests.exceptions.RequestException as e:
                logger.error(f"Orchestrator unreachable: {str(e)}")
                if should_requeue_task(task_name):
                    logger.warning(
                        f"Requeuing task {task_name} due to orchestrator timeout.")
                    task_tracker.update_task_status(
                        task_name=task_name,
                        status="scheduled",
                        last_execution_time=current_time,
                        worker_id=None,
                        proxy=None,
                        extra_fields={
                            "queued_at": datetime.now(timezone.utc).isoformat()}
                    )
                    time.sleep(5)
                    message_queue_service.publish_task(task)

    except Exception as e:
        logger.error(f"Error during task scheduling: {str(e)}")

def should_requeue_task(task_name, threshold_seconds=60):
        """Check task status and queued_at to decide if it should be requeued."""
        try:
            task_status = data_storage_service.get_task_config_fields(task_name, fields=["status", "queued_at"])
            current_status = task_status.get("status")
            last_queued = task_status.get("queued_at")

            if last_queued and isinstance(last_queued, str):
                last_queued = datetime.fromisoformat(last_queued)

            already_queued = (
                last_queued and (
                    datetime.now(timezone.utc) - last_queued) < timedelta(seconds=threshold_seconds)
            )

            if current_status in ["in-progress", "assigned", "completed"] or already_queued:
                logger.warning(
                    f"Skipping requeue: Task {task_name} is {current_status} or recently queued at {last_queued}.")
                return False
            return True

        except Exception as e:
            logger.error(
                f"Error during requeue check for task {task_name}: {e}")
            return False  # Failsafe: better skip than duplicate


def enhanced_worker_health_check():
    """
    Enhanced worker health check with dynamic monitoring and alerting.
    """
    try:
        available_worker = monitoring_service.get_available_worker()
        if available_worker:
            logger.info(f"Available worker found: {available_worker}.")
        else:
            logger.warning("No available workers detected.")
            alert_service.send_alert("No available workers detected.")
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Error fetching worker statuses from the Orchestrator: {str(e)}")
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during worker health check: {str(e)}")


def enhanced_proxy_health_check():
    """
    Enhanced proxy health check with metrics logging and alerting.
    """
    try:
        proxies = monitoring_service.check_proxy_health()
        for proxy, metrics in proxies.items():
            status = "healthy" if metrics["healthy"] else "unhealthy"
            logger.info(
                f"Proxy {proxy} is {status} with latency {metrics['latency']}ms.")
    except Exception as e:
        logger.error(f"Error during proxy health check: {str(e)}")


def enhanced_queue_health_check(queue_name):
    """
    Enhanced queue health check with stuck task detection and alerting.
    """
    try:
        queue_metrics = monitoring_service.check_queue_health(queue_name)
        if queue_metrics and queue_metrics.get("stuck_tasks", 0) > 0:
            alert_service.send_alert(
                f"Queue {queue_name} has {queue_metrics['stuck_tasks']} stuck tasks.")
    except Exception as e:
        logger.error(f"Error during queue health check: {str(e)}")
