import logging


# Setup logging
def start_logging(name: str) -> logging.Logger:
    logger = logging.getLogger(f'{name}.log')
    logger.setLevel(logging.INFO)
    # Add a FileHandler to write logs to a file
    fh = logging.FileHandler(f'{name}.log')
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(fh)
    return logger


# Log messages
def log_message(logger: logging.Logger, type: str, message: str):
    if type == 'info':
        logger.info(message)
    elif type == 'warning':
        logger.warning(message)
    elif type == 'error':
        logger.error(message)
    elif type == 'debug':
        logger.debug(message)
    else:
        logger.info(f'Unknown log type: {type}. Message: {message}')
