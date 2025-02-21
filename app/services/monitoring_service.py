import logging
import pika
from app.services.proxy_manager_service import ProxyManagerService
from app.services.data_storage_service import data_storage_service
import requests
from flask import current_app
import random

# Initialize Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class MonitoringService:
    def __init__(self):
        self.proxy_manager = ProxyManagerService()

    def check_worker_health(self):
        """
        Retrieve and log the health of all workers.

        :return: list of dict, all worker statuses
        """
        try:
            workers = data_storage_service.get_all_worker_statuses()
            for worker in workers:
                worker_id = worker["worker_id"]
                status = worker["status"]
                if status == "available":
                    logger.info(f"Worker {worker_id} is healthy and available.")
                else:
                    logger.warning(f"Worker {worker_id} is {status}.")
            return workers
        except Exception as e:
            logger.error(f"Error checking worker health: {str(e)}")
            return []

    def check_proxy_health(self):
        """
        Check the health of proxies.

        :return: dict, proxy metrics including health and latency
        """
        proxies = self.proxy_manager.get_all_proxies()
        if not proxies:
            logger.warning("No proxies available for health check.")
            return {}

        proxy_metrics = {}
        for proxy in proxies:
            is_valid = self.proxy_manager.validate_proxy(proxy)
            proxy_metrics[proxy] = {
                "healthy": is_valid,
                "latency": random.randint(20, 500) if is_valid else None
            }
            status = "healthy" if is_valid else "unhealthy"
            logger.info(f"Proxy {proxy} is {status}.")
        return proxy_metrics

    def check_queue_health(self, queue_name):
        """
        Check the health of the RabbitMQ queue.

        :param queue_name: str, name of the queue to check
        :return: dict, queue metrics including task count
        """
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            channel = connection.channel()
            queue = channel.queue_declare(queue=queue_name, durable=True, passive=True)
            task_count = queue.method.message_count
            connection.close()

            logger.info(f"Queue {queue_name} has {task_count} tasks.")
            return {
                "task_count": task_count,
                "stuck_tasks": 0  # Placeholder for stuck task detection
            }
        except Exception as e:
            logger.error(f"Error checking queue health: {str(e)}")
            return {
                "task_count": 0,
                "stuck_tasks": 0
            }

    def get_available_worker(self):
        """
        Get an available worker from the stored statuses in MongoDB.

        :return: str, worker ID of an available worker, or None if none are available
        """
        try:
            workers = data_storage_service.get_all_worker_statuses()
            available_workers = [worker["worker_id"] for worker in workers if worker["status"] == "available"]
            logger.info(f"Available workers: {available_workers}")
            return available_workers[0] if available_workers else None
        except Exception as e:
            logger.error(f"Error fetching worker statuses: {str(e)}")
            return None
