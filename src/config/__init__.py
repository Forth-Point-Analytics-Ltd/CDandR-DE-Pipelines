"""Script to initialise logger from yml file

Attributes:
    LOGGER_CONFIG_PATH (str): The relative path to the logging yml file.
"""
import logging
import logging.config
import os
import yaml
from yaml.loader import SafeLoader

LOGGER_CONFIG_PATH = (
    os.path.join(os.environ["CONFIG_FILE_PATH"], "logging.yaml")
    if "CONFIG_FILE_PATH" in os.environ
    else "./src/config/logging.yaml"
)

with open(str(os.path.abspath(LOGGER_CONFIG_PATH))) as file_handler:
    logging_config = yaml.load(file_handler, Loader=SafeLoader)
    logging.config.dictConfig(logging_config)
logger = logging.getLogger("consoleLogger")
