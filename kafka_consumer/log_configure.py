import logging

# Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler("./kafka_consumer.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

# Get a logger instance
logger = logging.getLogger(__name__)
