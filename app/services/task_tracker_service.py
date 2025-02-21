import redis

class TaskTrackerService:
    """
    Service to track task progress and statuses using Redis.
    """
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)

    def update_task_status(self, task_name, status):
        """
        Update the status of a task.
        """
        self.redis.set(f"task:{task_name}:status", status)

    def get_task_status(self, task_name):
        """
        Get the status of a task.
        """
        return self.redis.get(f"task:{task_name}:status").decode("utf-8")
