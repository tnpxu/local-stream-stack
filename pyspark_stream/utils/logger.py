import logging
import sys

# Define a default logger name for the framework
DEFAULT_LOGGER_NAME = 'pyspark_stream_logger'

def get_logger(name: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns a logger instance.

    If 'name' is provided, it returns a child logger of the default framework logger.
    Otherwise, it returns the default framework logger.

    Args:
        name (str, optional): The name of the logger. If None, the default
                              framework logger name is used. Defaults to None.
        level (int, optional): The logging level for this logger.
                               Defaults to logging.INFO.

    Returns:
        logging.Logger: A configured logger instance.
    """
    if name:
        logger_name = f"{DEFAULT_LOGGER_NAME}.{name}"
    else:
        logger_name = DEFAULT_LOGGER_NAME

    logger = logging.getLogger(logger_name)

    # Set the level for this specific logger.
    # This ensures that if a child logger is requested with a specific level,
    # it's respected, otherwise it inherits from the parent or default.
    logger.setLevel(level)

    # Configure the handler and formatter only if the main logger hasn't been configured yet
    # or if we want to ensure handlers are present for child loggers too (though typically they propagate)
    # For simplicity here, we check handlers on the root of our logger namespace (DEFAULT_LOGGER_NAME)
    # to avoid adding multiple handlers if get_logger is called multiple times for the same base logger.

    # Get the base logger to check/add handlers
    base_logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    if not base_logger.handlers: # Configure only if no handlers are present on the base logger
        # Create a console handler
        console_handler = logging.StreamHandler(sys.stdout)

        # Create a formatter and set it for the handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)

        # Add the handler to the base logger
        # Child loggers will propagate messages to this handler by default
        base_logger.addHandler(console_handler)

        # Set the base logger level. Child loggers can have their own more specific levels.
        base_logger.setLevel(logging.INFO) # Default level for the base logger

    # If a specific name was given, we are getting a child logger.
    # Its level was set above. It will use handlers from the base_logger.
    # If no name was given, we are getting the base_logger itself.
    return logger

# Example usage (optional, can be removed or commented out)
if __name__ == '__main__':
    # Get the default logger
    main_logger = get_logger()
    main_logger.info("This is an info message from the main logger.")
    main_logger.warning("This is a warning message from the main logger.")

    # Get a child logger
    child_logger_1 = get_logger("MyModule")
    child_logger_1.info("This is an info message from MyModule.")
    child_logger_1.debug("This debug message from MyModule might not be visible if main logger level is INFO.") # Won't show if base is INFO

    # Get another child logger with a more specific level
    child_logger_2 = get_logger("AnotherModule", level=logging.DEBUG)
    child_logger_2.debug("This is a debug message from AnotherModule and should be visible.")
    child_logger_2.info("This is an info message from AnotherModule.")

    # Test that handlers are not duplicated
    main_logger_again = get_logger()
    main_logger_again.info("Testing main logger again. Handler should not be duplicated.")

    # Test direct instantiation of default logger
    default_logger_direct = logging.getLogger(DEFAULT_LOGGER_NAME)
    default_logger_direct.info("Message from directly getting the default logger.")

    # Test logging from a grandchild logger
    grandchild_logger = get_logger("MyModule.MySubModule")
    grandchild_logger.info("This is an info message from MySubModule.")
    grandchild_logger.debug("This is a debug message from MySubModule, level inherited.") # Will show if MyModule was DEBUG

    child_logger_1.setLevel(logging.DEBUG) # Change level of child_logger_1
    child_logger_1.debug("This is a debug message from MyModule after changing its level.")
    grandchild_logger.debug("This is a debug message from MySubModule, should now be visible.")

    # Logger for a specific task with a high level
    critical_task_logger = get_logger("CriticalTask", level=logging.CRITICAL)
    critical_task_logger.critical("This is a critical message from a critical task.")
