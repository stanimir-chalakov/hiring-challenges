import logging
import re
from colorlog import ColoredFormatter
from colorlog.escape_codes import parse_colors  # correct import

def setup_logger(name="app", level=logging.INFO):
    """Simple color logger for consistent colorful output."""
    formatter = ColoredFormatter(
        "%(log_color)s[%(asctime)s] [%(levelname)s]%(reset)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def colorize_message(message: str, color: str = None) -> str:
    """Automatically add colors for numbers, filenames, or apply a custom color."""
    reset_code = parse_colors("reset")  # reset code
    if color:
        color_code = parse_colors(color)
        return f"{color_code}{message}{reset_code}"

    # Highlight numbers (counts, etc.)
    message = re.sub(r"\b\d+\b", f"{parse_colors('cyan')}\\g<0>{reset_code}", message)
    # Highlight filenames like msgs_20251107_1515.json
    message = re.sub(
        r"\b[\w\-]+\.json\b",
        f"{parse_colors('yellow')}\\g<0>{reset_code}",
        message,
    )

    return message