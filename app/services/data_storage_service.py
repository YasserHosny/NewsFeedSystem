from pymongo import MongoClient
import os
import logging
from datetime import datetime, timezone
import re

# Initialize logging with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # Ensure file handler uses UTF-8
        logging.FileHandler('app.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('data_storage_service')


class DataStorageService:
    """
    A service to handle data storage operations for configurations using MongoDB.
    """

    def __init__(self):
        # MongoDB connection details
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        database_name = os.getenv("MONGO_DB_NAME", "crawl_system")
        # Collection name directly defined in the code
        config_collection_name = "config_repo"
        worker_status_collection_name = "worker_status"  # Collection for worker statuses
        crawl_collection_name = "crawl_repo"  # New collection for crawled items
        # Collection for task execution logs
        task_execution_collection_name = "task_execution_log"
        # Initialize MongoDB client
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
        self.config_collection = self.db[config_collection_name]
        self.worker_status_collection = self.db[worker_status_collection_name]
        # Initialize the new collection
        self.crawl_collection = self.db[crawl_collection_name]
        # Initialize the new collection
        self.task_execution_collection = self.db[task_execution_collection_name]

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

    def get_eligible_configs_for_scheduling(self):
        """
        Retrieve configurations that are not already scheduled or in-progress.

        :return: list of dict, eligible tasks for scheduling
        """
        configs = self.config_collection.find(
            {"$or": [{"status": None}, {"status": "available"}]}
        )
        return [{**doc, "_id": str(doc["_id"])} for doc in configs]

    def clear_storage(self):
        """
        Clear all configurations in the MongoDB collection.

        :return: dict, the result of the delete operation
        """
        result = self.config_collection.delete_many({})
        return {"deleted_count": result.deleted_count}

    def log_task_execution_status(self, task_name, worker_id, proxy, status, error=None):
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

    def save_updated_items(self, result):
        """
        Save the entire result only if crawled_items contain new or updated entries.
        """
        try:
            all_items = result["crawled_items"]
            task_name = result["task_name"]
            fields_to_compare = result.get(
                "compare_fields", ["title", "price"])
            updated_items = []

            for item in all_items:
                product_url = item.get("product_url", "")
                item_id = self.extract_item_id(product_url)
                if not item_id:
                    logger.info(
                        'No item_id found in product_url. Skipping item.')
                    continue

                existing_doc = self.crawl_collection.find_one(
                    {
                        "task_name": task_name,
                        "crawled_items": {
                            "$elemMatch": {"item_id": item_id}
                        }
                    },
                    {
                        "crawled_items.$": 1
                    }
                )

                existing = existing_doc.get("crawled_items", [None])[0] if existing_doc else None

                # Compare only selected fields
                if not existing or any(item.get(k) != existing.get(k) for k in fields_to_compare):
                    # Add update metadata only AFTER confirming it's different
                    item["item_id"] = item_id
                    item["task_name"] = task_name
                    item["last_seen"] = datetime.now(timezone.utc).isoformat()
                    updated_items.append(item)

            if updated_items:
                result["crawled_items"] = updated_items
                result["total_items"] = len(updated_items)
                result["timestamp"] = datetime.now(timezone.utc).isoformat()

                inserted = self.crawl_collection.insert_one(result)
                result["_id"] = str(inserted.inserted_id)
                logger.info(
                    f"[{task_name}] Inserted {len(updated_items)} new/updated items.")
                return result
            else:
                logger.info(
                    f"[{task_name}] No updated items detected. Skipped DB insert.")
                return None

        except Exception as e:
            logger.error(
                f"Error while saving updated items for task {result.get('task_name', 'UNKNOWN')}: {e}", exc_info=True)
            return None

    def extract_item_id(self, product_url):
        match = re.search(r"/dp/([A-Z0-9]{10})", product_url)
        return match.group(1) if match else None

    def update_task_config(self, task_name, update_fields):
        """Update fields for a specific task config."""
        return self.config_collection.update_one(
            {"task_name": task_name},
            {"$set": update_fields}
        )

    def get_task_config_fields(self, task_name, fields=None):
        """Retrieve selected fields from a task config document."""
        projection = {field: 1 for field in fields} if fields else None
        return self.config_collection.find_one({"task_name": task_name}, projection)


# Singleton instance of the data storage service
data_storage_service = DataStorageService()
