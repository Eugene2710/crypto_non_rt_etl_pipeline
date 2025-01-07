import logging


def setup_logging(logger: logging.Logger):
    """
    achieves 3 main purposes
    - set log level to INFO; logs only INFO and above (default is WARNING)
    - direct the logs to a file -> FileHandler
    - for each log, customize the log to
        - specify the time logged - asctime
        - func name responsible - funcName
        - levelname - DEBUG / INFO / WARNING / ERROR / CRITICAL
        - message - actual logs
    """
    # set logging level
    logger.setLevel(logging.INFO)

    # format logs
    logging_formatter = logging.Formatter(
        "(%(asctime)s %(funcName)s %(levelname)s) %(message)s"
    )

    file_handler = logging.FileHandler("logs.txt")
    file_handler.setFormatter(logging_formatter)

    logger.addHandler(file_handler)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    setup_logging(logger)
    logger.info("Hello World")
