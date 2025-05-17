## Create venv:
python -m venv .venv
## Activate venv:
.\.venv\Scripts\activate
## Install requirements:
pip install -r requirements.txt
## Run task_worker.py:
python -m app.workers.task_worker
## Run integtest_task_worker.py
py -m tests.integration.integtest_task_worker
## Run run.py:
python -m run