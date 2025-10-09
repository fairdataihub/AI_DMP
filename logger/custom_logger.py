import os  # for file and directory handling
import logging  # built-in Python logging module
from datetime import datetime  # to create timestamped log filenames
import structlog  # for structured (JSON-style) logging


class CustomLogger:
    
    def __init__(self, log_dir="logs"):  # initialize logger with optional log directory
        # Create the logs directory if it doesn't already exist
        self.logs_dir = os.path.join(os.getcwd(), log_dir)  # make full path for logs folder
        os.makedirs(self.logs_dir, exist_ok=True)  # ensure folder exists

        # Create a log filename based on current date and time
        log_file = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"  # e.g., 10_09_2025_12_45_00.log
        self.log_file_path = os.path.join(self.logs_dir, log_file)  # complete file path for saving logs

    def get_logger(self, name=__file__):  # method to get a logger object
        logger_name = os.path.basename(name)  # use current filename as logger name

        # --- File Handler: writes logs to a file ---
        file_handler = logging.FileHandler(self.log_file_path)  # where logs are saved
        file_handler.setLevel(logging.INFO)  # minimum level of messages to log
        file_handler.setFormatter(logging.Formatter("%(message)s"))  # plain text format

        # --- Console Handler: prints logs to terminal ---
        console_handler = logging.StreamHandler()  # log output to console
        console_handler.setLevel(logging.INFO)  # only INFO and above levels
        console_handler.setFormatter(logging.Formatter("%(message)s"))  # plain format (no timestamp here)

        # --- Configure Python's logging module ---
        logging.basicConfig(
            level=logging.INFO,  # overall minimum level
            format="%(message)s",  # structlog will handle JSON formatting
            handlers=[console_handler, file_handler]  # apply both console and file outputs
        )

        # --- Configure structlog for structured JSON output ---
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),  # add ISO timestamp
                structlog.processors.add_log_level,  # include log level (info, error, etc.)
                structlog.processors.EventRenamer(to="event"),  # rename the main message key to "event"
                structlog.processors.JSONRenderer()  # render final log as JSON
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),  # use standard library logging backend
            cache_logger_on_first_use=True,  # performance optimization
        )

        return structlog.get_logger(logger_name)  # return configured logger


# --- Example usage section ---
#if __name__ == "__main__":  # runs only if the file is executed directly
   # logger = CustomLogger().get_logger(__file__)  # create a logger instance for this file
  #  logger.info("User uploaded a file", user_id=123, filename="report.pdf")  # info log example
 #   logger.error("Failed to process PDF", error="File not found", user_id=123)  # error log example
