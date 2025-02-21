from flask import Blueprint, request, jsonify, current_app
from app.services.data_storage_service import data_storage_service
from app.services.validation_service import validate_frequency, validate_task_fields
import logging
import requests

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Blueprint for the Configuration API
config_api_bp = Blueprint('config_api', __name__)

# Mock Scheduler Queue (can be replaced with actual task queue like Celery)
task_queue = []

def validate_input(config):
    """Validate basic input structure and required fields."""
    required_fields = ["task_name", "url", "frequency"]
    for field in required_fields:
        if field not in config:
            return False, f"Missing required field: {field}"
    return True, None

def validate_config_logic(config):
    """
    Validate configuration logic (e.g., frequency limits).

    :param config: dict, configuration details
    :return: tuple (is_valid: bool, message: str)
    """
    is_valid, message = validate_task_fields(config, ["frequency"])
    if not is_valid:
        return False, message

    return validate_frequency(config["frequency"])

def queue_task(config):
    """Queue the task in the scheduler."""
    task_queue.append(config)
    logger.info("Task queued successfully: %s", config)

@config_api_bp.route('/submit_config', methods=['POST'])
def submit_configuration():
    """API endpoint to submit a new configuration."""
    config = request.json
    logger.info("Received configuration submission: %s", config)

    # Step 1: Validate Input
    is_valid, message = validate_input(config)
    if not is_valid:
        logger.error("Input validation failed: %s", message)
        return jsonify({"status": "error", "message": message}), 400

    # Step 2: Validate Configuration Logic
    is_valid_logic, message = validate_config_logic(config)
    if not is_valid_logic:
        logger.error("Configuration logic validation failed: %s", message)
        return jsonify({"status": "error", "message": message}), 400

    try:
        # Step 3: Save to Configuration Repository
        saved_config = data_storage_service.save_config(config)
        logger.info("Configuration saved successfully: %s", saved_config)

        # Step 4: Queue Task in Scheduler
        scheduler_response = requests.post(current_app.config["DOMAIN"]+"scheduler/schedule_task", json=saved_config)
        if scheduler_response.status_code != 200:
            logger.error("Failed to queue task in Scheduler: %s", scheduler_response.text)
            return jsonify({"status": "error", "message": "Failed to queue task in Scheduler."}), 500

        return jsonify({
            "status": "success",
            "message": "Configuration saved and task queued successfully.",
            "data": saved_config
        }), 200
    except Exception as e:
        logger.error("Failed to process configuration: %s", str(e))
        return jsonify({"status": "error", "message": "Failed to process configuration."}), 500

@config_api_bp.route('/get_configs', methods=['GET'])
def get_all_configs():
    """API endpoint to retrieve all configurations."""
    try:
        configs = data_storage_service.get_all_configs()
        logger.info("Fetching all configurations. Total: %d", len(configs))
        return jsonify({
            "status": "success",
            "data": configs
        }), 200
    except Exception as e:
        logger.error("Failed to fetch configurations: %s", str(e))
        return jsonify({"status": "error", "message": "Failed to fetch configurations."}), 500

@config_api_bp.route('/get_tasks', methods=['GET'])
def get_queued_tasks():
    """API endpoint to retrieve all queued tasks."""
    try:
        logger.info("Fetching all queued tasks. Total: %d", len(task_queue))
        return jsonify({
            "status": "success",
            "data": task_queue
        }), 200
    except Exception as e:
        logger.error("Failed to fetch queued tasks: %s", str(e))
        return jsonify({"status": "error", "message": "Failed to fetch queued tasks."}), 500
