import aiohttp
import asyncio
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class ProxyValidator:
    def __init__(self, test_url: str = "https://www.google.com"):
        self.test_url = test_url
        self.timeout = aiohttp.ClientTimeout(total=10)  # 10 second timeout
        
    async def validate_proxy(self, proxy: str) -> bool:
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(
                    self.test_url,
                    proxy=proxy,
                    ssl=False
                ) as response:
                    return response.status == 200
        except Exception as e:
            logger.warning(f"Proxy validation failed for {proxy}: {str(e)}")
            return False
            
    async def filter_working_proxies(self, proxies: List[str]) -> List[str]:
        tasks = [self.validate_proxy(proxy) for proxy in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        working_proxies = [
            proxy for proxy, is_working in zip(proxies, results) 
            if isinstance(is_working, bool) and is_working
        ]
        
        return working_proxies 