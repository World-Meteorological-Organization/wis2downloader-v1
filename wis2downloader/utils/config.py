import json
import os
from pathlib import Path

from wis2downloader.log import LOGGER


def load_config():

    try:
        config_file = os.getenv('WIS2DOWNLOADER_CONFIG')
    except Exception as e:
        LOGGER.error("No config file specified, please set WIS2DOWNLOADER_CONFIG before running") # noqa
        raise RuntimeError(e)

    if config_file is None:
        raise ValueError(
            "WIS2DOWNLOADER_CONFIG must be set to an existing config file")

    config_file = Path(config_file)

    if not config_file.is_file():
        LOGGER.error(f"Config file {str(config_file)} not found")
        raise FileNotFoundError(f"Config file {str(config_file)} not found")

    try:
        with open(config_file) as fh:
            _config = json.load(fh)
    except Exception as e:
        LOGGER.error("Error loading config file")
        raise RuntimeError(e)

    return _config

CONFIG = load_config()