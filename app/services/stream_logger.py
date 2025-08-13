# app/services/stream_logger.py
import logging

class QueueHandler(logging.Handler):
    """
    A logging handler that puts records into a specific queue instance.
    This handler does not create its own queue; it's given one.
    """
    def __init__(self, log_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_queue = log_queue

    def emit(self, record):
        # We only want to stream the formatted message
        msg = self.format(record)
        self.log_queue.put(msg)