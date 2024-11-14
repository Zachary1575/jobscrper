import bootstrap_init
from global_state import global_instance

def initBootstrap() -> None:
    """
    Initalizes variables in package __init__.py

    @Parameters:
    None

    @Returns:
    None
    """
    bootstrap_init.logger = global_instance.get_data("logger")

    if (not bootstrap_init.logger):
        raise Exception("Logger could not be intitalized in bootstrap_init")
    return