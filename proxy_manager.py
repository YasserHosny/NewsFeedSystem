from typing import Optional, List
import random
import time
from proxy_validator import ProxyValidator
from app.logging_config import get_logger
from config import TASK_CONFIG

# Initialize logger
logger = get_logger('proxy_manager')

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.validator = ProxyValidator(test_url="https://www.amazon.eg")  # Use actual target site
        self.last_validation = 0
        self.validation_interval = TASK_CONFIG['retries']['proxy_refresh_interval']
        self.current_index = 0
        self.failed_proxies = {}  # Track failed proxies and their failure count
        
    async def get_proxy(self) -> Optional[str]:
        """Get next working proxy using round-robin"""
        if not self.proxies:
            return None
            
        if time.time() - self.last_validation > self.validation_interval:
            await self.refresh_proxies(self.proxies)
            
        if TASK_CONFIG['proxy']['rotation_strategy'] == 'round_robin':
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
        else:
            proxy = random.choice(self.proxies)
            
        return proxy
        
    async def refresh_proxies(self, proxy_list: List[str]):
        """Refresh and validate proxy list"""
        working_proxies = await self.validator.filter_working_proxies(proxy_list)
        
        # Keep track of working vs total proxies
        success_rate = len(working_proxies) / len(proxy_list) if proxy_list else 0
        logger.info(f"Proxy validation success rate: {success_rate:.2%}")
        
        if not working_proxies:
            logger.error("No working proxies found!")
            return False
            
        self.proxies = working_proxies
        self.last_validation = time.time()
        self.current_index = 0
        
        return True 

    async def mark_proxy_failed(self, proxy: str):
        """Mark a proxy as failed and remove it if it fails too often"""
        if proxy not in self.failed_proxies:
            self.failed_proxies[proxy] = 1
        else:
            self.failed_proxies[proxy] += 1
            
        # Remove proxy if it fails too many times
        if self.failed_proxies[proxy] >= TASK_CONFIG['proxy'].get('max_failures', 3):
            if proxy in self.proxies:
                self.proxies.remove(proxy)
                logger.warning(f"Removed failing proxy: {proxy}")
                
        # If too few proxies remain, trigger refresh
        if len(self.proxies) < TASK_CONFIG['proxy'].get('min_proxies', 2):
            await self.refresh_proxies(self.proxies + TASK_CONFIG['proxy']['backup_proxies']) 