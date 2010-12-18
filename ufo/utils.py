import logging, pprint, copy

# This sets up the print logger, system will be used throughout whenever we need
# to print something to the screen. For more complex logging, use a
# class-specific or file-specific logger.
logger = logging.getLogger("logger")

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"))
logger.addHandler(handler)


def group(lst, n):
    return [lst[i:i+n:] for i in range(0, len(lst), n)]
