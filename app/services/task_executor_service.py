import time
from playwright.sync_api import sync_playwright
from app.services.proxy_manager_service import ProxyManagerService
from app.services.data_storage_service import data_storage_service
from firecrawl import FirecrawlApp

class TaskExecutor:
    """
    Service to execute tasks using Playwright for web scraping.
    """

    def __init__(self, task=None):
        self.task = task
        self.proxy_manager = ProxyManagerService()
        self.retry_limit = 3
        self.screenshot_dir = "screenshots/"

    def fetch_crawl_configuration(self):
        """
        Fetch crawl configuration for the given task from the database.
        :return: dict, crawl configuration settings
        """
        config = data_storage_service.collection.find_one({"task_name": self.task["task_name"]})
        if not config:
            return {
                "selectors": {"content": "body", "specific": "div.content"},
                "headers": {},
                "action_delay": 1  # Default delay between actions in seconds
            }
        return config.get("crawl_settings", {
            "selectors": {"content": "body", "specific": "div.content"},
            "headers": {},
            "action_delay": 1
        })

    def _execute_with_playwright(self, crawl_config, selectors):
        """
        Execute the scraping task using Playwright.
        Returns the scraped data, list of items, and screenshot path.
        """
        proxy = self.task.get("allocated_proxy")
        
        with sync_playwright() as p:
            # Launch browser with proxy
            browser = p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
            page = browser.new_page()
            
            # Set headers if provided
            page.set_extra_http_headers(crawl_config.get("headers", {}))

            # Navigate to the URL
            page.goto(self.task["url"], timeout=30000)

            # Extract content using configured selectors
            title = page.title()
            content = page.query_selector(selectors["content"]).inner_text()
            specific_content = page.query_selector(selectors["specific"])
            specific_content_text = specific_content.inner_text() if specific_content else "Content not found"

            # Extract list of items
            items = []
            item_selector = selectors.get("items", ".item")  # Default to .item if not specified
            item_elements = page.query_selector_all(item_selector)
            
            for item in item_elements:
                item_data = {}
                
                # Extract data from each item based on configured selectors
                for field, selector in selectors.get("item_fields", {}).items():
                    element = item.query_selector(selector)
                    item_data[field] = element.inner_text() if element else ""
                
                # If no item_fields configured, get full item text
                if not selectors.get("item_fields"):
                    item_data["text"] = item.inner_text()
                    
                items.append(item_data)

            # Save screenshot for debugging or analysis
            screenshot_path = f"{self.screenshot_dir}{self.task['task_name']}.png"
            page.screenshot(path=screenshot_path)

            # Close the browser
            browser.close()

            return {
                "title": title,
                "content": content,
                "specific_content_text": specific_content_text,
                "items": items,
                "screenshot_path": screenshot_path
            }
    def _execute_with_firecrawl(self, crawl_config):
        """
        Execute the scraping task using Firecrawl.
        Returns the scraped data.
        """
        # Initialize Firecrawl with your API key
        firecrawl = FirecrawlApp(api_key="YOUR_FIRECRAWL_API_KEY")

        # Set up the parameters for Firecrawl
        params = {
            "pageOptions": {
                "onlyMainContent": True  # Adjust based on your needs
            }
        }

        # Perform the crawl
        try:
            result = firecrawl.scrape_url(self.task["url"], params=params)
            content = result.get("data", {}).get("content", "Content not found")
            title = result.get("data", {}).get("metadata", {}).get("title", "Title not found")

            return {
                "title": title,
                "content": content,
                "specific_content_text": content,  # Firecrawl may not separate specific content
                "screenshot_path": None  # Firecrawl does not provide screenshots
            }
        except Exception as e:
            raise Exception(f"Firecrawl failed: {str(e)}")

    def execute(self):
        """
        Execute the task using Playwright.
        """
        start_time = time.time()  # Start timing

        try:
            # Update worker status to busy
            data_storage_service.save_worker_status(self.task["allocated_worker"], "busy")

            crawl_config = self.fetch_crawl_configuration()
            selectors = crawl_config.get("selectors", {"content": "body", "specific": "div.content"})

            # Allocate proxy if not already set
            if "allocated_proxy" not in self.task:
                self.task["allocated_proxy"] = self.proxy_manager.get_proxy()

            retries = self.task.get("retries", 0)

            # Execute scraping with Playwright
            scraped_data = self._execute_with_playwright(crawl_config, selectors)

            # Save task result to MongoDB
            result = {
                "task_name": self.task["task_name"],
                "url": self.task["url"],
                "status": "success",
                "title": scraped_data["title"],
                "content_preview": scraped_data["content"][:200],
                "specific_content": scraped_data["specific_content_text"][:200],
                "selectors": selectors,
                "screenshot_path": scraped_data["screenshot_path"],
                "timestamp": time.time(),
            }
            data_storage_service.save_config(result)

            # Update worker status to available
            data_storage_service.save_worker_status(self.task["allocated_worker"], "available")
        except Exception as e:
            if retries < self.retry_limit:
                self.task["retries"] = retries + 1
                raise
            else:
                result = {
                    "task_name": self.task["task_name"],
                    "url": self.task["url"],
                    "status": "failure",
                    "error_message": str(e),
                    "timestamp": time.time(),
                }
                data_storage_service.save_config(result)
                # Update status to available on failure
                data_storage_service.save_worker_status(self.task["allocated_worker"], "available")
                raise