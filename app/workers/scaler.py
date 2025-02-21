import os
import subprocess
import time
import pika
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
MAX_WORKERS = 10
MIN_WORKERS = 1
QUEUE_NAME = 'tasks'

def get_task_count():
    """Retrieve the number of tasks in the RabbitMQ queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    queue_state = channel.queue_declare(queue=QUEUE_NAME, durable=True, passive=True)
    task_count = queue_state.method.message_count
    connection.close()
    return task_count

def scale_workers():
    """Scale workers dynamically based on queue length."""
    task_count = get_task_count()
    desired_workers = min(max(task_count // 5, MIN_WORKERS), MAX_WORKERS)  # Scale between MIN_WORKERS and MAX_WORKERS
    current_workers = int(os.environ.get("WORKER_COUNT", MIN_WORKERS))

    logger.info("Task count: %d, Current workers: %d, Desired workers: %d", task_count, current_workers, desired_workers)

    if desired_workers > current_workers:
        # Start additional workers
        for _ in range(desired_workers - current_workers):
            subprocess.Popen(["python", "workers/task_worker.py"])
        os.environ["WORKER_COUNT"] = str(desired_workers)
        logger.info("Scaled up to %d workers.", desired_workers)

    elif desired_workers < current_workers:
        # Stop excess workers (not implemented here, relies on external process management)
        logger.info("Scale-down required, but stopping workers is manual in this implementation.")

if __name__ == "__main__":
    while True:
        scale_workers()
        time.sleep(10)  # Check every 10 seconds
