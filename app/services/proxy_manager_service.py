import random
import logging
import aiohttp
from app.logging_config import get_logger

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = get_logger('proxy_manager')

class ProxyManagerService:
    """Service to manage proxies for task execution."""

    def __init__(self):
        self.proxies = [
            "http://23.82.137.162:80",
            "http://116.107.176.31:1080",
            "http://128.199.64.85:80",
            "http://45.91.93.166:27030",
            "http://36.90.50.9:5678"
        ]  # Proxy pool (list of proxy strings)

    def load_proxies(self, proxy_list):
        """
        Load proxies into the pool.

        :param proxy_list: list of proxy strings (e.g., ["http://proxy1:8080", "http://proxy2:8080"])
        """
        self.proxies.extend(proxy_list)
        logger.info("Loaded %d proxies into the pool.", len(proxy_list))

    def get_proxy(self):
        """
        Get a random proxy from the pool.

        :return: str, a proxy URL or None if no proxies are available
        """
        if not self.proxies:
            logger.warning("No proxies available in the pool.")
            return None
        return random.choice(self.proxies)

    def remove_proxy(self, proxy):
        """
        Remove a proxy from the pool.

        :param proxy: str, the proxy URL to remove
        """
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            logger.info("Removed proxy: %s", proxy)
        else:
            logger.warning("Attempted to remove a proxy that does not exist: %s", proxy)

    async def validate_proxy(self, proxy):
        """
        Validate a proxy by sending a test request.

        :param proxy: str, the proxy URL to validate
        :return: bool, True if the proxy is valid, False otherwise
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    'https://www.amazon.eg',
                    proxy=proxy,
                    timeout=10,
                    ssl=False
                ) as response:
                    return response.status == 200
        except Exception as e:
            logger.warning(f"Proxy validation failed for {proxy}: {str(e)}")
            return False

    def validate_all_proxies(self):
        """
        Validate all proxies in the pool and remove invalid ones.
        """
        logger.info("Validating all proxies...")
        valid_proxies = [proxy for proxy in self.proxies if self.validate_proxy(proxy)]
        invalid_count = len(self.proxies) - len(valid_proxies)
        self.proxies = valid_proxies
        logger.info("Proxy validation completed. Removed %d invalid proxies.", invalid_count)

    def get_all_proxies(self):
        """
        Retrieve all proxies in the pool.
        :return: list of proxy strings
        """
        return self.proxies

    async def get_validated_proxy(self):
        """Get a working proxy with validation"""
        for _ in range(3):  # Try up to 3 different proxies
            proxy = self.get_proxy()
            if proxy and await self.validate_proxy(proxy):
                return proxy
            await self.mark_proxy_failed(proxy)
        return None
