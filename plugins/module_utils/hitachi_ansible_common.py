


import os
from logging.handlers import RotatingFileHandler
import logging
import subprocess
import sys

from ansible.module_utils.hitachi_ansible_common_constant import (
    LoggingConstants,
)

def get_log_file():
    # Extract directory path from log file
    log_dir = os.path.dirname(LoggingConstants.RUN_LOG_FILE)
    
    # Check if directory exists
    if not os.path.exists(log_dir):
        # Create the directory if it doesn't exist
        os.makedirs(log_dir)
        
    return LoggingConstants.RUN_LOG_FILE

def initialize_filehandler_logger(logger):
    # Define log message format
    log_format = LoggingConstants.RUN_LOG_FORMAT
    formatter = logging.Formatter(log_format)
    log_level = LoggingConstants.get_log_level()
    
    file_handler = RotatingFileHandler(get_log_file(), maxBytes=LoggingConstants.MAX_BYTES, backupCount=LoggingConstants.BACKUP_COUNT)  # Rotates after 10MB, keeps 5 backups
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)

    if log_level > logging.DEBUG:
        sys.tracebacklimit = 0

    #Set the ANSIBLE_LOG_PATH
    
    # command = f'export ANSIBLE_LOG_PATH={LoggingConstants.INTERNAL_LOG_FILE}'
    # subprocess.run(command, shell=True)

    