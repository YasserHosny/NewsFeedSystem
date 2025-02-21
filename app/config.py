class Config:
    """Base configuration settings."""
    DEBUG = False
    TESTING = False
    SECRET_KEY = "your_secret_key"
    DOMAIN = "http://127.0.0.1:5200/"

class DevelopmentConfig(Config):
    """Development configuration settings."""
    DEBUG = True

class TestingConfig(Config):
    """Testing configuration settings."""
    TESTING = True

class ProductionConfig(Config):
    """Production configuration settings."""
    SECRET_KEY = "your_production_secret_key"
