import threading

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