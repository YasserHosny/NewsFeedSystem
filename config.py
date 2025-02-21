TASK_CONFIG = {
    "timeouts": {
        "page_load": 60000,  # 60 seconds
        "navigation": 30000,  # 30 seconds
        "proxy_validation": 10000  # 10 seconds
    },
    "retries": {
        "max_attempts": 3,
        "delay_between": 5,
        "proxy_refresh_interval": 300  # 5 minutes
    },
    "proxy": {
        "validation_url": "https://www.amazon.eg",
        "rotation_strategy": "round_robin",
        "validation_timeout": 10,
        "max_concurrent_validations": 10,
        "max_failures": 3,  # Maximum failures before removing proxy
        "min_proxies": 2,   # Minimum number of working proxies before refresh
        "backup_proxies": [
            "http://backup-proxy-1:8080",
            "http://backup-proxy-2:8080"
        ]
    }
} 