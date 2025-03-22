from pymongo import MongoClient
import os
import logging
from datetime import datetime, timezone

# Initialize logging with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log', encoding='utf-8')  # Ensure file handler uses UTF-8
    ]
)
logger = logging.getLogger(__name__)

class DataStorageService:
    """
    A service to handle data storage operations for configurations using MongoDB.
    """

    def __init__(self):
        # MongoDB connection details
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        database_name = os.getenv("MONGO_DB_NAME", "crawl_system")
        config_collection_name = "config_repo"  # Collection name directly defined in the code
        worker_status_collection_name = "worker_status"  # Collection for worker statuses
        crawl_collection_name = "crawl_repo"  # New collection for crawled items
        task_execution_collection_name = "task_execution_log"  # Collection for task execution logs
        # Initialize MongoDB client
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
        self.config_collection = self.db[config_collection_name]
        self.worker_status_collection = self.db[worker_status_collection_name]
        self.crawl_collection = self.db[crawl_collection_name]  # Initialize the new collection
        self.task_execution_collection = self.db[task_execution_collection_name]  # Initialize the new collection

    def save_config(self, config):
        """
        Save a configuration to the MongoDB collection.

        :param config: dict, the configuration data to save
        :return: dict, the inserted configuration with its MongoDB ID
        """
        result = self.config_collection.insert_one(config)
        config["_id"] = str(result.inserted_id)
        return config

    def get_all_configs(self):
        """
        Retrieve all configurations from the MongoDB collection.

        :return: list of dict, all saved configurations
        """
        configs = self.config_collection.find()
        return [{**doc, "_id": str(doc["_id"])} for doc in configs]

    def clear_storage(self):
        """
        Clear all configurations in the MongoDB collection.

        :return: dict, the result of the delete operation
        """
        result = self.config_collection.delete_many({})
        return {"deleted_count": result.deleted_count}
    
    def save_task_result(self, task_name, worker_id, proxy, status, error=None):
        """
        Save task execution result to the `task_execution_log` collection.
        
        :param task_name: Name of the executed task
        :param worker_id: ID of the worker that executed the task
        :param proxy: Proxy used during execution
        :param status: Execution status (success, failed, retrying)
        :param error: Optional error message if the task failed
        """
        task_result = {
            "task_name": task_name,
            "worker_id": worker_id,
            "proxy_used": proxy,
            "execution_status": status,
            "error_message": error,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.task_execution_collection.insert_one(task_result)
        logger.info("Task execution result saved to MongoDB: %s", task_result)

    def save_failed_task(self, task_name, worker_id, proxy, error, retry_count):
        """
        Save failed task execution in `task_execution_log`.

        :param task_name: Task name
        :param worker_id: Worker ID
        :param proxy: Proxy used
        :param error: Error message
        :param retry_count: Number of retry attempts
        """
        failed_task = {
            "task_name": task_name,
            "worker_id": worker_id,
            "proxy_used": proxy,
            "status": "failed",
            "error_message": error,
            "retry_count": retry_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.task_execution_collection.insert_one(failed_task)
        logger.info("Failed task logged: %s", failed_task)

    def save_worker_status(self, worker_id, status):
            """
            Save or update the status of a worker in the MongoDB collection.

            :param worker_id: str, the ID of the worker
            :param status: str, the status of the worker (e.g., "available", "busy")
            """
            logger.info(f"Saving worker {worker_id} status: {status}")
            self.worker_status_collection.update_one(
                {"worker_id": worker_id},
                {"$set": {"status": status, "updated_at": self._current_timestamp()}},
                upsert=True
            )
            logger.info(f"Worker {worker_id} status updated to {status}.")

    def get_worker_status(self, worker_id):
            """
            Retrieve the status of a specific worker.

            :param worker_id: str, the ID of the worker
            :return: dict or None, the worker's status record
            """
            return self.worker_status_collection.find_one({"worker_id": worker_id}, {"_id": 0})

    def get_all_worker_statuses(self):
        """
        Retrieve the statuses of all workers.

        :return: list of dict, all worker statuses
        """
        workers = list(self.worker_status_collection.find({}, {"_id": 0}))
        logger.info(f"Retrieved {len(workers)} worker statuses.")
        return workers

    def clear_worker_statuses(self):
        """
        Clear all worker statuses from the MongoDB collection.

        :return: dict, the result of the delete operation
        """
        result = self.worker_status_collection.delete_many({})
        logger.info(f"Cleared {result.deleted_count} worker statuses.")
        return {"deleted_count": result.deleted_count}

    def _current_timestamp(self):
        """
        Helper method to generate the current timestamp.

        :return: str, the current timestamp in ISO format
        """
        from datetime import datetime
        return datetime.now(timezone.utc).isoformat()

    def save_crawl_items(self, result):
        """
        Save a single task execution result to the MongoDB collection.

        :param result: dict, the task execution result data to save
        :return: dict, the inserted result with its MongoDB ID
        """
        if not isinstance(result, dict):
            raise ValueError("The result parameter must be a dictionary.")
        
        inserted_result = self.crawl_collection.insert_one(result)
        result["_id"] = str(inserted_result.inserted_id)
        
        logger.info("Task execution result saved to MongoDB: %s", result)
        return result

# Singleton instance of the data storage service
data_storage_service = DataStorageService()
