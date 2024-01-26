import logging


def add_logging_level(level_name, level_num, method_name=None):
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError(f"{level_name} already defined in logging module")
    if hasattr(logging, method_name):
        raise AttributeError(f"{method_name} already defined in logging module")
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError(f"{method_name} already defined in logger class")

    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)


SUCCESS_LEVEL_NUM = 25


def get_custom_logger(name, level=logging.INFO):
    if not hasattr(logging, "SUCCESS"):
        add_logging_level("SUCCESS", SUCCESS_LEVEL_NUM)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(ColoredFormatter())
    logger.addHandler(ch)

    return logger


class ColoredFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    green = "\x1b[32;21m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    white = "\x1b[37m"

    prefix_format = "%(levelname)s:%(name)s:%(filename)s:%(lineno)d:"
    message_format = " %(message)s"

    FORMATS = {
        logging.DEBUG: grey + prefix_format + reset + message_format,
        logging.INFO: white + prefix_format + reset + message_format,
        logging.WARNING: yellow + prefix_format + reset + message_format,
        logging.ERROR: red + prefix_format + reset + message_format,
        logging.CRITICAL: bold_red + prefix_format + reset + message_format,
        SUCCESS_LEVEL_NUM: green + prefix_format + reset + message_format,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, "%Y-%m-%d %H:%M:%S")
        return formatter.format(record)
