from flask import Blueprint, jsonify, request
from app.services.proxy_manager_service import ProxyManagerService

# Initialize Proxy Manager
proxy_manager_bp = Blueprint('proxy_manager', __name__)
proxy_manager = ProxyManagerService()

@proxy_manager_bp.route('/add_proxies', methods=['POST'])
def add_proxies():
    """API to add proxies to the pool."""
    proxy_list = request.json.get('proxies', [])
    proxy_manager.load_proxies(proxy_list)
    return jsonify({"status": "success", "message": f"Added {len(proxy_list)} proxies."}), 200

@proxy_manager_bp.route('/list_proxies', methods=['GET'])
def list_proxies():
    """API to list all proxies in the pool."""
    return jsonify({"status": "success", "proxies": proxy_manager.proxies}), 200

@proxy_manager_bp.route('/validate_proxies', methods=['POST'])
def validate_proxies():
    """API to validate all proxies in the pool."""
    proxy_manager.validate_all_proxies()
    return jsonify({"status": "success", "message": "Proxies validated."}), 200
