import requests

class AlertService:
    """
    Service to send alerts via Slack.
    """
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_alert(self, message):
        """
        Send an alert message to Slack.
        """
        payload = {"text": message}
        requests.post(self.webhook_url, json=payload)
