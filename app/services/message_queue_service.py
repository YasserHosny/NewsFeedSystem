import pika
import json
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MessageQueueService:
    """Service to interact with RabbitMQ."""

    def __init__(self, queue_name='crawl_tasks'):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        # Declare a queue with durable settings
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        logger.info(f"Message queue '{self.queue_name}' initialized.")


    def publish_task(self, task):
        """
        Publish a task to the RabbitMQ queue.

        :param task: dict, the task to publish
        """
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(task),
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make message persistent
                )
            )
            logger.info("Task published to queue: %s", task)
            # List all available tasks in the queue
            logger.info("Available tasks in the queue: %s", self.channel.queue_declare(queue=self.queue_name, durable=True).method.message_count) 
        except Exception as e:
            logger.error("Failed to publish task: %s", str(e))

    def get_task(self):
        """
        Retrieve a task from the RabbitMQ queue.

        :return: dict, the retrieved task or None if no task is available
        """
        logger.info("Retrieving task from queue named: %s", self.queue_name)
        # List all available tasks in the queue
        logger.info("Available tasks in the queue: %s", self.channel.queue_declare(queue=self.queue_name, durable=True).method.message_count) 
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=False)
        if method_frame:
            task = json.loads(body)
            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            logger.info("Task retrieved from queue: %s", task)
            return task
        else:
            logger.info("No task available in the queue.")
            return None

    def close_connection(self):
        """Close the RabbitMQ connection."""
        self.connection.close()
        logger.info("Message queue connection closed.")
