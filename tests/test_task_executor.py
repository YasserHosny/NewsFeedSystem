import unittest
from unittest.mock import patch, MagicMock
from app.services.task_executor_service import TaskExecutor

class TestTaskExecutor(unittest.TestCase):
    def setUp(self):
        self.task = {
            "task_name": "Test Task",
            "url": "https://example.com",
            "allocated_proxy": "http://proxy.example:8080",
        }
        self.executor = TaskExecutor(self.task)

    @patch("app.services.data_storage_service.data_storage_service.collection.find_one")
    def test_fetch_crawl_configuration(self, mock_find_one):
        # Mock database response
        mock_find_one.return_value = {
            "task_name": "Test Task",
            "crawl_settings": {"selectors": {"content": "body", "specific": "div.test"}}
        }
        config = self.executor.fetch_crawl_configuration()
        self.assertEqual(config["selectors"]["specific"], "div.test")

    @patch("app.services.task_executor_service.sync_playwright")
    def test_execute_task_success(self, mock_playwright):
        # Mock Playwright browser and page behavior
        mock_browser = MagicMock()
        mock_page = MagicMock()
        mock_browser.new_page.return_value = mock_page
        mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = mock_browser

        # Mock Playwright page interactions
        mock_page.title.return_value = "Example Title"
        mock_page.query_selector.return_value.inner_text.return_value = "Example Content"

        # Execute task
        self.executor.execute()
        mock_page.goto.assert_called_once_with("https://example.com", timeout=30000)
        mock_page.screenshot.assert_called()

    @patch("app.services.task_executor_service.sync_playwright")
    def test_execute_task_failure(self, mock_playwright):
        # Mock Playwright to raise an exception
        mock_playwright.return_value.__enter__.side_effect = Exception("Playwright Error")

        with self.assertRaises(Exception):
            self.executor.execute()

if __name__ == "__main__":
    unittest.main()
