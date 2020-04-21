import logging

def prepare_logging(module, level):
    """
    Generic function to prepare logging used on functions

    :param module: module name ot be use in logger
    :param level: level of logging
    :return: logger object
    """

    logger = logging.getLogger(module)
    logger_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(logger_handler)
    logger.propagate = False
    return logger
