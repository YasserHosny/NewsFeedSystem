import json
from task_worker import TaskWorker
from config import TASK_CONFIG
from app.logging_config import get_logger

# Initialize logger
logger = get_logger('message_queue')

class MessageQueue:
    async def process_message(self, message):
        worker = None
        try:
            task = json.loads(message.body)
            worker = TaskWorker()
            
            try:
                result = await worker.execute_task(task)
                # Handle successful result
                await self.handle_success(task, result)
            except Exception as e:
                if hasattr(worker, 'current_retries') and worker.current_retries >= TASK_CONFIG['retries']['max_attempts']:
                    # Handle final failure after max retries
                    await self.handle_failure(task, str(e))
                else:
                    # Don't requeue if we're still within retry attempts
                    raise
                    
        except json.JSONDecodeError as e:
            logger.error(f"Invalid message format: {str(e)}")
            # Don't requeue invalid messages
            return
        except ImportError as e:
            logger.error(f"Missing dependency: {str(e)}")
            # Don't requeue if there's a missing dependency
            return
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Only requeue if it's a new task or hasn't reached max retries
            if not worker or not hasattr(worker, 'current_retries') or worker.current_retries < TASK_CONFIG['retries']['max_attempts']:
                await self.requeue_message(message) 