from apscheduler.schedulers.background import BackgroundScheduler
from .monitoring_service import MonitoringService
from .message_queue_service import MessageQueueService
from .alert_service import AlertService
import logging
import requests
from flask import current_app

# Initialize Logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Services
alert_service = AlertService(
    webhook_url="https://hooks.slack.com/services/your/webhook/url")
message_queue_service = MessageQueueService(queue_name="crawl_tasks")
monitoring_service = MonitoringService()


def start_scheduler(app):
    """
    Start the APScheduler background scheduler for periodic health checks and task scheduling.
    Ensures Flask application context is available for all scheduled jobs.
    """
    logger.info("Starting APScheduler for health monitoring tasks...")

    scheduler = BackgroundScheduler()

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
    Pull tasks from the message queue and dispatch them to the Crawl Orchestrator.
    """
    logger.info("Scheduling tasks from the message queue...")

    try:
        # Pull a task from the message queue
        task = message_queue_service.get_task()
        logger.info("Task pulled from queue: %s", task)
        if not task:
            logger.info("No tasks available in the message queue.")
            return

        # Use MonitoringService to check worker availability
        available_worker = monitoring_service.get_available_worker()
        if not available_worker:
            logger.warning(
                "No available workers detected. Requeuing the task.")
            message_queue_service.publish_task(task)
            alert_service.send_alert("No available workers to process tasks.")
            return

        # Forward task to Orchestrator
        response = requests.post(current_app.config["DOMAIN"] + "orchestrator/process_tasks", json=task)

        if response.status_code == 200:
            logger.info(
                f"Task {task['task_name']} dispatched to the orchestrator successfully.")
        else:
            logger.error(
                f"Failed to dispatch task {task['task_name']} to the orchestrator. Requeuing task.")
            message_queue_service.publish_task(task)

    except Exception as e:
        logger.error(f"Error during task scheduling: {str(e)}")
        message_queue_service.publish_task(task)  # Requeue task on failure


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
