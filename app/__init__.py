from flask import Flask
from app.config import DevelopmentConfig
from app.routes.proxy_manager import proxy_manager_bp
from app.routes.dashboard import dashboard_bp
from app.routes.scheduler import scheduler_bp
from app.routes.orchestrator import orchestrator_bp  # Import Orchestrator Blueprint
from app.logging_config import get_logger

logger = get_logger('app')

def create_app(config_class=DevelopmentConfig):
    """Create and configure the Flask app."""
    app = Flask(__name__)
    # Load the appropriate configuration
    app.config.from_object(config_class)
        
    # Register blueprints
    from .routes.config_api import config_api_bp
    app.register_blueprint(config_api_bp, url_prefix='/config_api')
    app.register_blueprint(scheduler_bp, url_prefix='/scheduler')  # Correct scheduler prefix
    app.register_blueprint(orchestrator_bp, url_prefix='/orchestrator')  # Register orchestrator blueprint
    app.register_blueprint(proxy_manager_bp, url_prefix='/proxy_manager')
    app.register_blueprint(dashboard_bp, url_prefix='/dashboard')

    return app
