import logging
import os

class Logger:
    def __init__(self, log_name=None,log_level=logging.DEBUG, log_file=None, log_file_name="app.log", console_log=True):
        """
        Initializes the logger with a console handler and an optional file handler.

        Args:
            log_level (int): The logging level (e.g., logging.DEBUG, logging.INFO). Defaults to logging.INFO.
            log_file (str, optional): The path to the log file. If None, no file handler is added. Defaults to None.
            log_file_name (str, optional): The name of the log file. Defaults to "app.log".
        """
        if log_name:
            self.logger = logging.getLogger(log_name)
        else:
            self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        if console_log:
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        # File handler (optional)
        if log_file:
            # Ensure the directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            file_handler = logging.FileHandler(os.path.join(log_file, log_file_name))
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)
