import threading
from abc import ABC, abstractmethod


class LockClass(ABC):

    def __init__(self, resource: str):
        self.resource = resource
        self.lock = None

    @abstractmethod
    def acquire(self) -> bool:
        pass

    @abstractmethod
    def release():
        pass


class LocalLock(LockClass):

    def __init__(self, resource: str):
        self.lock = threading.Lock()
        self.resource = resource

    def acquire(self, blocking=True) -> bool:
        return self.lock.acquire(blocking=blocking)

    def release(self) -> bool:
        return self.lock.release()
