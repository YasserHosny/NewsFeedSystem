import re

def validate_frequency(frequency):
    """
    Validate the frequency value in the format '<number> <unit>'.

    :param frequency: str, frequency to validate
    :return: tuple (is_valid: bool, message: str)
    """
    valid_units = {"mins", "hours", "days", "weeks"}
    frequency_pattern = r"^\d+\s+(mins|hours|days|weeks)$"

    if isinstance(frequency, str):
        if re.match(frequency_pattern, frequency):
            value, unit = frequency.split()
            if unit in valid_units and int(value) > 0:
                return True, None
            return False, f"Invalid frequency unit or value. Must be a positive integer followed by one of {valid_units}."
        return False, f"Invalid frequency format. Expected format: '<number> <unit>', e.g., '1 mins', '2 hours', '3 days', '4 weeks'."
    return False, "Invalid frequency type. Must be a string in the format '<number> <unit>'."

def validate_task_fields(task, required_fields):
    """
    Validate required fields in a task or configuration.

    :param task: dict, task or configuration details
    :param required_fields: list of str, fields that must be present
    :return: tuple (is_valid: bool, message: str)
    """
    for field in required_fields:
        if field not in task:
            return False, f"Missing required field: {field}"
    return True, None

def convert_to_minutes(frequency):
    """
    Convert frequency to minutes.

    :param frequency: str, frequency in the format '<number> <unit>'
    :return: int, equivalent frequency in minutes or float('inf') for invalid values
    """
    unit_to_minutes = {"mins": 1, "hours": 60, "days": 1440, "weeks": 10080}

    if not isinstance(frequency, str) or " " not in frequency:
        logger.error(f"Invalid frequency format: {frequency}")
        return float('inf')  # Invalid format

    value, unit = frequency.split()
    if unit not in unit_to_minutes:
        logger.error(f"Invalid frequency unit: {unit}")
        return float('inf')  # Invalid unit

    try:
        return int(value) * unit_to_minutes[unit]
    except ValueError:
        logger.error(f"Invalid numeric value in frequency: {value}")
        return float('inf')  # Invalid numeric value
