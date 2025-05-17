import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from app.workers.task_worker import TaskWorker
import asyncio


class TestTaskWorker(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.worker = TaskWorker("worker-1", "http://localhost")

    @patch("app.workers.task_worker.requests.post")
    def test_send_heartbeat(self, mock_post):
        mock_post.return_value.status_code = 200
        with patch("time.sleep", return_value=None), patch.object(self.worker.logger, 'info'):
            self.worker.send_heartbeat()
            mock_post.assert_called_with(
                "http://localhost/update_worker_status",
                json={"worker_id": "worker-1", "status": "available"}
            )

    @patch("app.workers.task_worker.async_playwright")
    @patch("app.workers.task_worker.data_storage_service.save_crawl_items")
    @patch("app.workers.task_worker.task_tracker.update_task_status")
    async def test_execute_task_success(self, mock_update_status, mock_save_data, mock_async_playwright):
        # Mock feature element
        feature_locator = MagicMock()
        feature_locator.get_attribute = AsyncMock(return_value="https://image.jpg")
        feature_locator.inner_text = AsyncMock(return_value="Mocked Text")

        # Mock specific element
        specific_element = MagicMock()
        specific_element.locator.return_value = feature_locator

        # Mock specific locator
        specific_locator = MagicMock()
        specific_locator.count = AsyncMock(return_value=1)
        specific_locator.nth.return_value = specific_element

        # Mock content element
        content_element_mock = MagicMock()
        content_element_mock.locator.return_value = specific_locator

        # Mock content locator
        content_locator = MagicMock()
        content_locator.count = AsyncMock(return_value=1)
        content_locator.nth.return_value = content_element_mock

        # Page mock
        page_mock = MagicMock()
        page_mock.locator.return_value = content_locator
        page_mock.goto = AsyncMock(return_value=MagicMock(status=200))
        page_mock.title = AsyncMock(return_value="Test Page")
        page_mock.screenshot = AsyncMock()
        page_mock.url = "http://example.com"
        page_mock.wait_for_selector = AsyncMock()
        page_mock.wait_for_timeout = AsyncMock()

        # Context and browser mocks
        context_mock = MagicMock()
        context_mock.new_page = AsyncMock(return_value=page_mock)
        context_mock.set_default_timeout = MagicMock()
        context_mock.close = AsyncMock()

        browser_mock = MagicMock()
        browser_mock.new_context = AsyncMock(return_value=context_mock)
        browser_mock.close = AsyncMock()

        chromium_mock = MagicMock()
        chromium_mock.launch = AsyncMock(return_value=browser_mock)

        playwright_instance = AsyncMock()
        playwright_instance.chromium = chromium_mock
        playwright_instance.stop = AsyncMock()

        mock_async_playwright.return_value.start = AsyncMock(return_value=playwright_instance)

        # Task definition
        task = {
            "task_name": "test_task",
            "url": "https://www.amazon.eg/l/21864643031/ref=s9_acss_bw_cg_gaming_5a1_w?...",
            "crawl_settings": {
                "selectors": {
                    "content": "div.a-column.a-span12.aok-float-right...",
                    "specific": "li.octopus-pc-item.octopus-pc-item-v3",
                    "next": "#apb-desktop-browse-search-see-all",
                    "features": {
                        "title": { "selector": ".octopus-pc-asin-title span" },
                        "price": { "selector": ".a-price .a-offscreen" },
                        "old_price": { "selector": ".octopus-pc-asin-strike-price span" },
                        "rating": { "selector": ".a-icon-alt" },
                        "review_count": { "selector": ".a-size-mini.a-color-tertiary" },
                        "image_url": { "selector": "img.octopus-pc-item-image", "attribute": "src" },
                        "product_url": { "selector": "a.octopus-pc-item-link", "attribute": "href" }
                    }
                },
                "headers": { "User-Agent": "Mozilla/5.0" },
                "action_delay": 1
            }
        }

        # Execute task
        await self.worker.execute_task(task)

        # Validate expected calls
        mock_save_data.assert_called()
        mock_update_status.assert_any_call(task_name="test_task", status="in-progress",
                                        worker_id="worker-1", proxy=None, extra_fields=unittest.mock.ANY)
        mock_update_status.assert_any_call(task_name="test_task", status="completed",
                                        worker_id="worker-1", proxy=None, extra_fields=unittest.mock.ANY)

    async def test_extract_item_data(self):
        mock_content_element = AsyncMock()
        mock_specific = AsyncMock()
        mock_content_element.locator.return_value = mock_specific
        mock_specific.count.return_value = 1
        mock_specific.nth.return_value.locator.return_value.inner_text.return_value = "Title"

        task_url = "http://example.com"
        selectors = {"title": {"selector": ".title"}}
        data = await self.worker.extract_item_data(task_url, mock_content_element, ".specific", 1, 0, selectors)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["page"], 1)


if __name__ == '__main__':
    unittest.main()
