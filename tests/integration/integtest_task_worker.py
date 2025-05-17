import sys
import os
import pytest
import uuid
import asyncio
from unittest.mock import patch
from app.workers.task_worker import TaskWorker
import logging
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../")))

# ✅ Add project root to Python path
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../")))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("integration_test")


@pytest.mark.asyncio
@patch("app.workers.task_worker.task_tracker.update_task_status")
async def test_execute_task_real_almas4perfumes(mock_status):
    logger.info("Initializing TaskWorker...")
    worker = TaskWorker(
        worker_id=str(uuid.uuid4()),
        orchestrator_url="http://localhost"
    )

    task = {
        "task_name": "ExampleCrawl3",
        "url": "https://almas4perfumes.com/ar/%D9%85%D8%B9%D8%B7%D8%B1%D8%A7%D8%AA-%D8%A7%D9%84%D9%85%D9%86%D8%B2%D9%84/c1583151528",
        "crawl_settings": {
            "selectors": {
                "content": "div.flex.min-h-screen",
                "specific": "custom-salla-product-card.s-product-card-entry",
                "next": "#apb-desktop-browse-search-see-all",
                "features": {
                    "title": {
                        "selector": "h3.s-product-card-content-title a",
                        "attribute": None  # text_content will be used
                    },
                    "price": {
                        "selector": "h4.s-product-card-price",
                        "attribute": None
                    },
                    "product_url": {
                        "selector": "h3.s-product-card-content-title a",
                        "attribute": "href"
                    },
                    "image_url": {
                        "selector": "img.s-product-card-image-cover",
                        "attribute": "src"
                    },
                    "rating": {
                        "selector": "div.s-product-card-rating span",
                        "attribute": None
                    }
                },
                "compare_fields": [
                    "title",
                    "price"
                ]
            },
            "headers": {
                "User-Agent": "Mozilla/5.0"
            },
            "action_delay": 1
        },
        "proxy": [
            "http://49.0.2.242:8090",
            "http://185.199.229.156:80"
        ]
    }

    logger.info("Starting task execution for task: %s", task["task_name"])
    try:
        await worker.execute_task(task)
        logger.info("Task executed successfully.")
    except Exception as e:
        logger.error("Exception occurred during task execution: %s", e)
        raise

    # If no exception occurred, the test passes
    assert True

if __name__ == "__main__":
    import pytest
    pytest.main([__file__])
