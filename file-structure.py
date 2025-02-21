import os

def create_file_structure(base_path, structure):
    for key, value in structure.items():
        path = os.path.join(base_path, key)
        if isinstance(value, dict):
            # Create directory and recurse
            os.makedirs(path, exist_ok=True)
            create_file_structure(path, value)
        elif isinstance(value, list):
            # Create directory and add files
            os.makedirs(path, exist_ok=True)
            for file in value:
                file_path = os.path.join(path, file)
                with open(file_path, 'w') as f:
                    f.write('')

if __name__ == "__main__":
    project_structure = {
        "project": {
            "app": {
                "__init__.py": None,
                "config.py": None,
                "routes": ["__init__.py", "config_api.py", "scheduler.py", "orchestrator.py", "proxy_manager.py", "workers.py", "logging_monitoring.py"],
                "models": ["__init__.py", "config_model.py", "task_model.py", "log_model.py"],
                "services": ["__init__.py", "validation_service.py", "task_service.py", "proxy_service.py", "monitoring_service.py", "data_storage_service.py"]
            },
            "tests": ["__init__.py", "test_config_api.py", "test_scheduler.py", "test_orchestrator.py", "test_proxy_manager.py", "test_workers.py", "test_logging_monitoring.py"],
            "requirements.txt": None,
            "run.py": None,
            "README.md": None,
            ".gitignore": None
        }
    }

    base_directory = os.getcwd()  # You can change this to a specific path
    create_file_structure(base_directory, project_structure)
    print("Project structure created successfully!")
