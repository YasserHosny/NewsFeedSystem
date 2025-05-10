from app import create_app
from app.config import DevelopmentConfig, ProductionConfig
from app.logging_config import get_logger
from app.services.scheduler_service import start_scheduler

# Initialize logger
logger = get_logger('app')

# Create the Flask app
app = create_app(config_class=DevelopmentConfig)

# Configure logging for run.py
get_logger(name="app")

# Start the scheduler for health monitoring
start_scheduler(app)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5200)
