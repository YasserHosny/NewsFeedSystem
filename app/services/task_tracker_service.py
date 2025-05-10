import logging
from app.logging_config import get_logger
# Use MongoDB instead of Redis
from app.services.data_storage_service import data_storage_service

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = get_logger("task_tracker_service")


class TaskTrackerService:
    """
    Service to track task progress and statuses using MongoDB instead of Redis.
    """

    def update_task_status(self, task_name, status, worker_id=None, proxy=None, last_execution_time=None, extra_fields=None):
        try:
            update_fields = {
                "status": status
            }
            if worker_id is not None:
                update_fields["worker_id"] = worker_id
            if proxy is not None:
                update_fields["proxy"] = proxy
            if last_execution_time is not None:
                update_fields["last_execution_time"] = last_execution_time
            if extra_fields:
                update_fields.update(extra_fields)

            result = data_storage_service.update_task_config(
                task_name=task_name,
                update_fields=update_fields
            )

            if result.modified_count:
                logger.info(
                    f"Updated task {task_name} status to {status} with fields: {update_fields}")
            else:
                logger.warning(
                    f"No documents matched for task {task_name}. Status update skipped.")

        except Exception as e:
            logger.error(
                f"Failed to update task status for {task_name}: {str(e)}")

    def get_task_status(self, task_name):
        """
        Get the status of a task from MongoDB.
        """
        try:
            task = data_storage_service.config_collection.find_one(
                {"task_name": task_name}, {"status": 1})
            if task:
                return task.get("status", "unknown")
            else:
                return "not found"
        except Exception as e:
            logger.error(
                f"Failed to retrieve task status for {task_name}: {str(e)}")
            return "error"
