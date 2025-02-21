from flask import Blueprint, jsonify
from app.services.data_storage_service import data_storage_service
from app.services.proxy_manager_service import ProxyManagerService

# Initialize Blueprint
dashboard_bp = Blueprint('dashboard', __name__)

# Proxy Manager
proxy_manager = ProxyManagerService()

@dashboard_bp.route('/task_progress', methods=['GET'])
def task_progress():
    """API to fetch task progress and status."""
    tasks = list(data_storage_service.collection.find({}, {"_id": 0}))
    return jsonify({"status": "success", "tasks": tasks}), 200

@dashboard_bp.route('/worker_status', methods=['GET'])
def worker_status():
    """API to fetch worker status."""
    workers = {
        "Worker-1": "available",
        "Worker-2": "busy",
        "Worker-3": "available"
    }  # Replace with actual worker health logic
    return jsonify({"status": "success", "workers": workers}), 200

@dashboard_bp.route('/proxy_status', methods=['GET'])
def proxy_status():
    """API to fetch proxy health and usage."""
    proxies = proxy_manager.get_all_proxies()  # Replace with actual logic
    return jsonify({"status": "success", "proxies": proxies}), 200
