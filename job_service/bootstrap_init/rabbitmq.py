import socket
import bootstrap_init

from tasks import dummy_task

def verifyRabbitMQ(host: str, port: int) -> None:
    """
    Verifies if RabbitMQ Broker is up in running (Docker).

    @Paramters:
    host [string] - specified host to ping the server at.
    port [int] - specified port to ping the server at.

    @Return: 
    None
    """
    logger = bootstrap_init.logger
    if (host == None or port == None):
        logger.exception("No Host or Port was given!")
        raise Exception("No Host or Port was given!")
    
    try:
        logger.debug(f"Testing Connection String -> {host}:{port}")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))

        if (result == 0):
            logger.debug(f"Listener responded on port {port}, verifying if the port is RabbitMQ...")
        else:
            logger.error(f"Port {port} is closed or not responding")
            raise Exception(f"Port {port} is closed or not responding")
        
        result = dummy_task.delay()
        logger.debug(f"Task result: {result.get(timeout=5)}")
        
        logger.success(f"RabbitMQ is live on port {port}!")
        return
    except socket.error as e:
        logger.exception(f"Socket Error: {e}")
        raise Exception(f"Socket Error: {e}")
    except Exception as e:
        logger.error("Verifying RabbitMQ ran into an Exception!")
        raise Exception(f"{e}")
    finally:
        sock.close

    return