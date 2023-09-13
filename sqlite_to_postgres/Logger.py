import logging
from logging.handlers import RotatingFileHandler

# Set up logging
log_file = './sqlite_to_postgres.log'
max_file_size = 1024 * 1024  # 1 MB
backup_count = 5

# Create a named logger for your application
logger = logging.getLogger('sqlite_to_postgres')
logger.setLevel(logging.INFO)

# Configure the file handler with log rotation
file_handler = RotatingFileHandler(log_file, maxBytes=max_file_size, backupCount=backup_count)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)