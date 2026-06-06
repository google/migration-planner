import threading
from sortedcontainers import SortedSet

# To-Do: Use finer grained locking
class AtomicInt():
    def __init__(self, value):
        self.value = value
        self.lock = threading.Lock()

    def increment(self, count: int = 1):
        with self.lock:
            self.value += count

    def decrement(self, count: int = 1):
        with self.lock:
            self.value -= count

    def get_value(self):
        with self.lock:
            return self.value

class ThreadSafeSortedSet():
    def __init__(self):
        self.sorted_set = SortedSet()
        self.lock = threading.Lock()
    
    def add(self, value):
        with self.lock:
            self.sorted_set.add(value)

    def remove(self, value):
        with self.lock:
            self.sorted_set.remove(value)


class ThreadSafeMap():
    def __init__(self):
        self.map = {}
        self.lock = threading.Lock()
    
    def update(self, key, value):
        with self.lock:
            self.map[key] = value
    
    def get(self, key, default_value):
        if self.contains(key):
            with self.lock:
                return self.map.get(key)
        return default_value
    
    def contains(self, key):
        with self.lock:
            return key in self.map

    def get_all(self):
        with self.lock:
            return self.map.copy()