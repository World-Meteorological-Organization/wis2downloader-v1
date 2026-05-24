from abc import ABC, abstractmethod
from queue import Queue
import time

from wis2downloader import stop_event
from wis2downloader.log import LOGGER
from wis2downloader.metrics import QUEUE_SIZE


class BaseQueue(ABC):

    @abstractmethod
    def enqueue(self, item):
        """Add an item to the queue"""
        pass

    @abstractmethod
    def dequeue(self):
        """Remove and return an item from the queue"""
        pass

    @abstractmethod
    def size(self) -> int:
        """Return the number of items in the queue"""
        pass

    @abstractmethod
    def task_done(self):
        """Indicate that a formerly enqueued task is complete"""
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """Check if the queue is empty"""
        pass

    @abstractmethod
    def toggle_active(self):
        """Toggle the active state of the queue"""
        pass

    @abstractmethod
    def is_active(self) -> bool:
        """
        Function that returns whether the queue is active
        """
        pass


class QMonitor:
    def __init__(self, _queue: BaseQueue, period: int = 60):
        while not stop_event.is_set():
            LOGGER.info(f"Queue size: {_queue.size()}")
            time.sleep(period)


class SimpleQueue(BaseQueue):
    def __init__(self, maxsize=10000):
        """
        Initialize SimpleQueue with a size limit to prevent unbounded memory growth.

        Args:
            maxsize: Maximum number of items allowed in the queue.
                     Default is 10000. Set to 0 for unlimited (not recommended).
        """
        self._queue = Queue(maxsize=maxsize)
        self.active = True
        self.maxsize = maxsize
        LOGGER.info(f"SimpleQueue initialized with maxsize={maxsize}")

    def enqueue(self, item):
        self._queue.put(item)
        # Note queue size for the metric
        QUEUE_SIZE.set(self.size())

    def dequeue(self):
        # Note queue size for the metric
        QUEUE_SIZE.set(self.size())
        return self._queue.get()

    def size(self) -> int:
        return self._queue.qsize()

    def task_done(self):
        self._queue.task_done()

    def is_empty(self) -> bool:
        return self._queue.empty()

    def toggle_active(self):
        self.active = not self.active

    def is_active(self):
        return self.active


class RedisQueue(BaseQueue):
    pass
