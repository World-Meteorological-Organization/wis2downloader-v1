import requests
import logging

LOGGER = logging.getLogger(__name__)

def validate_post_config(config: dict) -> tuple[bool, str]:
    """
    Validates the post configuration dictionary to ensure it's proper.

    Args:
        config: The configuration dictionary to validate.

    Returns:
        A tuple containing a boolean (True if valid) and a message.
    """
    if not isinstance(config, dict):
        return False, "Validation failed: post_config must be a dictionary."

    required_keys = ["post_url", "post_body_type"]
    for key in required_keys:
        if key not in config:
            return False, f"Validation failed: Missing required key '{key}'"

    post_type = config["post_body_type"]
    if post_type not in ["json", "binary"]:
        return False, f"Validation failed: 'post_body_type' must be 'json' or 'binary', not '{post_type}'"

    if post_type == "binary":
        if "binary_content_type" not in config:
            return False, "Validation failed: Missing 'binary_content_type' for post_body_type 'binary'"

    return True, "Configuration is valid."


def post_data(config: dict, json_payload: dict = None, raw_bytes: bytes = None) -> requests.Response | None:
    """
    Posts data to a URL based on the provided configuration.

    Args:
        config: The configuration dictionary for the POST request.
        json_payload: The data to send if the type is 'json'.
        raw_bytes: The data to send if the type is 'binary'.
    """
    is_valid, message = validate_post_config(config)
    if not is_valid:
        LOGGER.error(f"Invalid post configuration: {message}")
        return None

    post_type = config.get("post_body_type")
    url = config.get("post_url")
    headers = config.get("post_headers", {})
    timeout = config.get("post_timeout", 10)

    try:
        if post_type == "json":
            response = requests.post(url, json=json_payload, headers=headers, timeout=timeout)
        elif post_type == "binary":
            headers["Content-Type"] = config.get("binary_content_type", "application/octet-stream")
            response = requests.post(url, data=raw_bytes, headers=headers, timeout=timeout)
        
        response.raise_for_status()
        LOGGER.info(f"Successfully posted data to {url}. Status: {response.status_code}")
        return response

    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Failed to post data to {url}: {e}")
        return None
