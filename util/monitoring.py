import psutil
import threading
import time
from typing import Tuple

class ResourceMonitor(threading.Thread):
  """Monitors CPU and RAM usage in a separate thread.

  Attributes:
      interval: Time in seconds between measurements.
      stop_event: Event to signal the thread to stop.
      cpu_readings: List of CPU usage percentages.
      ram_readings: List of RAM usage percentages.
  """

  def __init__(self, interval: float = 1.0):
    super().__init__()
    self.interval = interval
    self.stop_event = threading.Event()
    self.cpu_readings: List[float] = []
    self.ram_readings: List[float] = []
    self.daemon = True

  def run(self) -> None:
    """Continuously records system metrics until stopped."""
    while not self.stop_event.is_set():
      self.cpu_readings.append(psutil.cpu_percent(interval=None))
      self.ram_readings.append(psutil.virtual_memory().percent)
      time.sleep(self.interval)

  def stop(self) -> None:
    """Signals the monitor to stop recording."""
    self.stop_event.set()

  def get_stats(self) -> Tuple[float, float, float, float]:
    """Calculates average and maximum CPU and RAM usage.

    Returns:
        A tuple containing (avg_cpu, max_cpu, avg_ram, max_ram).
    """
    if not self.cpu_readings:
      return 0.0, 0.0, 0.0, 0.0
    avg_cpu = sum(self.cpu_readings) / len(self.cpu_readings)
    max_cpu = max(self.cpu_readings)
    avg_ram = sum(self.ram_readings) / len(self.ram_readings)
    max_ram = max(self.ram_readings)
    return avg_cpu, max_cpu, avg_ram, max_ram
