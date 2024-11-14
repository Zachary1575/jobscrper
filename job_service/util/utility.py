import time
import bootstrap_init

def divide_pages_into_three_parts(total_pages):
    """
    DOC STRING
    """
    part_size = total_pages // 3
    remainder = total_pages % 3

    parts = []
    start_page = 1

    for i in range(3):
        end_page = start_page + part_size - 1
        if remainder > 0:
            end_page += 1
            remainder -= 1
        
        parts.append((start_page, end_page))
        start_page = end_page + 1

    return parts

def estimate_current_job_time(previous_predictions, completion_times, alpha = 0.5):
    """
    Update the Exponential Moving Average (EMA) based on the latest completion time
    and the previous EMA prediction.

    Parameters:
    - completion_times: list of float or int, the history of completion times, 
                        with the last entry being the most recent completion time.
    - previous_predictions: list of float, the history of previous EMA predictions, 
                            with the last entry being the most recent EMA.
    - alpha: float, the smoothing factor, where 0 < alpha <= 1. Defaults to 0.1.

    Returns:
    - float: the updated EMA value.
    """
    ema = alpha * completion_times[len(completion_times) - 1] + (1 - alpha) * previous_predictions[len(previous_predictions) - 1]
    return ema

def time_monitoring_task(duration, stop_event):
    """
    DOC STRING
    """
    logger = bootstrap_init.logger
    start_time = time.time()
    
    while (time.time() - start_time < duration and not stop_event.is_set()):
        time.sleep(0.1)
        
    if (time.time() - start_time < duration):
        logger.debug("Terminating Timing Thread as host process finished early!")
    else:
        logger.warning("Time limit reached for current job. Signaling processing task to stop.")
        stop_event.set()
    return