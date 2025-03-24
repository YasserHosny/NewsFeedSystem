import asyncio
import logging
import time
from threading import Thread
from playwright.sync_api import sync_playwright
from app.services.data_storage_service import data_storage_service
from app.services.proxy_manager_service import ProxyManagerService
from app.services.task_executor_service import TaskExecutor
from app.logging_config import get_logger
import requests
import pika
import json
from app.config import Config
import uuid
from functools import wraps
from playwright.async_api import async_playwright
from datetime import datetime, timezone
from app.services.task_tracker_service import TaskTrackerService

# Initialize logger first
logger = get_logger('worker')
task_tracker = TaskTrackerService()

try:
    import backoff
    HAS_BACKOFF = True
except ImportError:
    HAS_BACKOFF = False
    logger.warning(
        "backoff package not found. Falling back to simple retry logic.")


class TaskWorker:
    """
    Class representing a Task Worker responsible for consuming tasks,
    executing them, and maintaining worker status via heartbeats.
    """

    def __init__(self, worker_id, orchestrator_url, screenshot_dir="screenshots/"):
        self.worker_id = worker_id
        self.orchestrator_url = orchestrator_url
        self.screenshot_dir = screenshot_dir
        self.retry_limit = Config.RETRY_LIMIT if hasattr(
            Config, 'RETRY_LIMIT') else 3
        self.rate_limit_delay = Config.RATE_LIMIT_DELAY if hasattr(
            Config, 'RATE_LIMIT_DELAY') else 5
        self.proxy_manager = ProxyManagerService()
        self.task_executor = TaskExecutor()
        self.current_retries = 0

        # Initialize logger
        self.logger = logger
        self.logger.info(f"TaskWorker initialized with ID: {self.worker_id}")

    def _retry_with_backoff(self, func):
        """Decorator to implement retry logic with or without backoff package"""
        if HAS_BACKOFF:
            return backoff.on_exception(
                backoff.expo,
                Exception,
                max_tries=self.retry_limit,
                max_time=300,
                on_backoff=lambda details: logger.warning(
                    f"Retrying {func.__name__} (attempt {details['tries']}) after {details['wait']} seconds"
                )
            )(func)
        else:
            @wraps(func)
            async def wrapper(*args, **kwargs):
                retries = 0
                while retries < self.retry_limit:
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        retries += 1
                        if retries >= self.retry_limit:
                            logger.error(
                                f"Max retries ({self.retry_limit}) reached. Giving up.")
                            raise
                        wait_time = 2 ** retries  # Simple exponential backoff
                        logger.warning(
                            f"Retrying {func.__name__} (attempt {retries}) after {wait_time} seconds")
                        await asyncio.sleep(wait_time)
            return wrapper

    def send_heartbeat(self):
        """
        Periodically send worker status to the Orchestrator.
        """
        while True:
            try:
                status = "available"
                self.logger.info(
                    f"Sending heartbeat for worker: {self.worker_id} with status: {status}")
                response = requests.post(
                    f"{self.orchestrator_url}/update_worker_status",
                    json={"worker_id": self.worker_id, "status": status}
                )
                if response.status_code == 200:
                    self.logger.info(
                        f"Heartbeat sent successfully: {self.worker_id} - {status}")
                else:
                    self.logger.error(
                        f"Failed to send heartbeat: {response.status_code}")
            except Exception as e:
                self.logger.error(f"Error sending heartbeat: {e}")
            time.sleep(30)  # Send heartbeat every 30 seconds

    async def execute_task(self, task):
        """
        Execute the given task using Playwright for web scraping across multiple pages.
        """
        self.logger.info(f"Starting execution for task: {task}")
        retries = task.get("retries", 0)
        all_items = []
        started_at = datetime.now(timezone.utc).isoformat()

        try:
            # Update task status to in-progress
            task_tracker.update_task_status(
                task_name=task["task_name"],
                status="in-progress",
                worker_id=self.worker_id,
                proxy=task.get("proxy"),
                extra_fields={
                    "started_at": started_at,
                    "retry_count": retries
                }
            )
            self.logger.info("Starting Playwright")
            playwright = await async_playwright().start()
            self.logger.info("Launching browser")
            browser = await playwright.chromium.launch(headless=True)

            self.logger.info("Creating new browser context")
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )

            context.set_default_timeout(60000)
            self.logger.info("Opening new page")
            page = await context.new_page()

            crawl_settings = task.get("crawl_settings", {})
            selectors = crawl_settings.get("selectors", {})
            content_selector = selectors.get("content")
            specific_selector = selectors.get("specific")
            next_page_selector = selectors.get(
                "next")  # Support for pagination

            current_url = task["url"]
            page_num = 1

            while current_url:
                self.logger.info(
                    f"Navigating to page {page_num}: {current_url}")
                response = await page.goto(current_url, wait_until='domcontentloaded', timeout=60000)

                if not response or response.status >= 400:
                    raise Exception(
                        f"Failed to load page: {response.status if response else 'No response'}")

                self.logger.info(
                    f"Waiting for content selector: {content_selector}")
                await page.wait_for_selector(content_selector, timeout=10000)
                content_elements = page.locator(content_selector)
                content_count = await content_elements.count()

                self.logger.info(
                    f"Found {content_count} content blocks on page {page_num}")

                for i in range(content_count):
                    content_element = content_elements.nth(i)
                    specific_elements = content_element.locator(
                        specific_selector)
                    specific_count = await specific_elements.count()

                    for j in range(specific_count):
                        text = await specific_elements.nth(j).inner_text()
                        all_items.append({
                            "page": page_num,
                            "content_index": i,
                            "specific_index": j,
                            "content": text.strip()
                        })

                # Take screenshot of this page
                screenshot_path = f"{self.screenshot_dir}{task['task_name']}_page{page_num}_{int(time.time())}.png"
                self.logger.info(f"Taking screenshot: {screenshot_path}")
                await page.screenshot(path=screenshot_path)
                self.logger.info(
                    f"Screenshot for page {page_num} saved: {screenshot_path}")

                # Go to next page if available
                if next_page_selector:
                    next_button = page.locator(next_page_selector)
                    if await next_button.count() > 0 and await next_button.is_enabled():
                        self.logger.info("Clicking next page button")
                        await next_button.click()
                        # Add delay between pages
                        await page.wait_for_timeout(2000)
                        current_url = page.url
                        page_num += 1
                    else:
                        self.logger.info("No next page found. Finishing.")
                        break
                else:
                    self.logger.info(
                        "No next page selector provided. Finishing.")
                    break

            title = await page.title()
            # Final Result
            result = {
                "task_name": task["task_name"],
                "url": task["url"],
                "status": "success",
                "title": title,
                "crawled_items": all_items,
                "total_items": len(all_items),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            self.logger.info("Saving crawled items")
            data_storage_service.save_crawl_items(result)

            # Update task status to completed
            task_tracker.update_task_status(
                task_name=task["task_name"],
                status="completed",
                worker_id=self.worker_id,
                proxy=task.get("proxy"),
                extra_fields={
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "retry_count": retries
                }
            )
            self.logger.info(
                f"Task completed successfully. {len(all_items)} items saved.")

            self.logger.info("Closing context and browser")
            await context.close()
            await browser.close()
            await playwright.stop()

        except Exception as e:
            self.logger.error(f"Task execution failed: {str(e)}")
            retries += 1
            task["retries"] = retries

            task_tracker.update_task_status(
                task_name=task["task_name"],
                status="failed",
                worker_id=self.worker_id,
                proxy=task.get("proxy"),
                extra_fields={
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "retry_count": retries,
                    "error_message": str(e)
                }
            )
            raise

    def worker_process(self):
        """
        Worker process that consumes tasks from the RabbitMQ queue.
        """
        self.logger.info(
            f"Worker process started for worker: {self.worker_id}")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='crawl_tasks', durable=True)

        async def async_callback(ch, method, properties, body):
            task = json.loads(body)
            self.logger.info(f"Task received: {task}")

            try:
                await self.execute_task(task)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                self.logger.error(
                    f"Task execution failed: {e}. Requeuing task.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            finally:
                time.sleep(self.rate_limit_delay)

        def callback(ch, method, properties, body):
            asyncio.run(async_callback(ch, method, properties, body))

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='crawl_tasks',
                              on_message_callback=callback)
        self.logger.info(
            f"Worker {self.worker_id} ready and waiting for tasks...")
        channel.start_consuming()

    def start(self):
        """
        Start the worker: Heartbeat thread and task processing.
        """
        self.logger.info(f"Starting worker: {self.worker_id}")
        heartbeat_thread = Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()
        self.worker_process()


if __name__ == "__main__":
    worker = TaskWorker(
        worker_id=str(uuid.uuid4()),  # Generate a unique worker ID
        orchestrator_url=Config.DOMAIN + "orchestrator"
    )
    worker.start()
