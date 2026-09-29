# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Crash-resistant, per-run file logger shared across all migration planners.

Every record is written and flushed to `logs/{planner_type}_{ts}.log` as soon
as it is logged, so the log survives application crashes and force-quits.

If `logs/` cannot be written, the run falls back to stderr (the terminal), so
records are only dropped when stderr is unavailable too (e.g. `pythonw`).

Typical usage from a planner UI:

  run_logger.install_exception_hooks()      # once, at app start-up
  run_logger.start_run("files")             # on "Start scan"
  run_logger.log("Scanning sites...")       # from log_msg
  run_logger.export_to(dest_path)           # from "Export logs"

Future features can log into the active run file without being handed a
callback:

  logger = run_logger.get_logger("pdf_export")
  logger.info("PDF generated")
"""

from datetime import datetime
import atexit
import logging
import os
import re
import sys
import threading
import types
from typing import Any

LOGGER_NAME = "migration_planner.run"
LOGS_DIR = "logs"
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_FILE_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
_EXPORT_CHUNK_BYTES = 1024 * 1024
# Planner types become part of a file path, so restrict them to a safe charset.
_PLANNER_TYPE_PATTERN = re.compile(r"^[a-z0-9_]+$")

ExcInfo = tuple[type[BaseException], BaseException, types.TracebackType | None]


class RunFormatter(logging.Formatter):
  """Formats records as '[YYYY-MM-DD HH:MM:SS] message'.

  Leading newlines in the message are hoisted before the timestamp so that
  separator blocks (e.g. "\\n" + "=" * 40) render as blank lines followed by
  the stamped separator, as they did with the legacy in-memory buffer.
  """

  def format(self, record: logging.LogRecord) -> str:
    message = record.getMessage()
    stripped = message.lstrip("\n")
    prefix = "\n" * (len(message) - len(stripped))
    ts = datetime.fromtimestamp(record.created).strftime(_TIMESTAMP_FORMAT)
    line = f"{prefix}[{ts}] {stripped}"
    if record.exc_info:
      line += "\n" + self.formatException(record.exc_info)
    return line


class _RunFileHandler(logging.StreamHandler):
  """Append-mode UTF-8 file handler that flushes after every record.

  Unlike logging.FileHandler, a closed handler never re-opens its file, so a
  record racing with end_run() is dropped instead of leaking a file handle.
  """

  def __init__(self, path: str) -> None:
    super().__init__(open(path, "a", encoding="utf-8"))
    self.path = path

  def emit(self, record: logging.LogRecord) -> None:
    # Handler.handle() holds self.lock here, so this check cannot race close().
    if self.stream is None:
      return
    super().emit(record)  # StreamHandler.emit writes and then flushes.

  def snapshot_size(self) -> int:
    """Returns the file size at a record boundary.

    The handler lock is held only long enough to read the size, so no record
    can be half-written at that offset and logging threads are not blocked
    for the duration of a copy.
    """
    self.acquire()
    try:
      if self.stream is None:
        return os.path.getsize(self.path)
      self.stream.flush()
      return os.fstat(self.stream.fileno()).st_size
    finally:
      self.release()

  def close(self) -> None:
    self.acquire()
    try:
      if self.stream is not None:
        try:
          self.stream.flush()
        finally:
          self.stream.close()
          self.stream = None
    finally:
      self.release()
      super().close()


_logger = logging.getLogger(LOGGER_NAME)
_logger.setLevel(logging.INFO)
_logger.propagate = False
# With no handlers attached, logging falls back to `logging.lastResort`, which
# prints WARNING+ records to stderr. The NullHandler makes "no active run"
# explicitly mean "discard", so e.g. uncaught exceptions are not printed twice
# (once by lastResort and once by the chained default excepthook).
_null_handler = logging.NullHandler()
_logger.addHandler(_null_handler)

_state_lock = threading.Lock()
_active_handler: logging.Handler | None = None
_hooks_installed = False


def _unique_log_path(logs_dir: str, planner_type: str) -> str:
  """Returns {logs_dir}/{planner_type}_{ts}.log, suffixed if the name is taken."""
  ts = datetime.now().strftime(_FILE_TIMESTAMP_FORMAT)
  base = os.path.join(logs_dir, f"{planner_type}_{ts}")
  path = f"{base}.log"
  suffix = 1
  while os.path.exists(path):
    path = f"{base}_{suffix}.log"
    suffix += 1
  return path


def _open_file_handler(logs_dir: str, planner_type: str) -> _RunFileHandler:
  os.makedirs(logs_dir, exist_ok=True)
  handler = _RunFileHandler(_unique_log_path(logs_dir, planner_type))
  handler.setFormatter(RunFormatter())
  return handler


def _create_run_handler(planner_type: str) -> tuple[logging.Handler | None, list[str]]:
  """Creates the run handler: a file in ./logs, else stderr (the terminal).

  Returns:
    The handler (None only if the file fails and stderr is unavailable) and a
    list of warnings describing any fallback taken.
  """
  try:
    return _open_file_handler(LOGS_DIR, planner_type), []
  except OSError as e:
    warnings = [f"Could not create log file in '{os.path.abspath(LOGS_DIR)}': {e}"]

  if sys.stderr is not None:
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(RunFormatter())
    warnings.append("Logging to the terminal (stderr) instead of a file.")
    return stderr_handler, warnings

  warnings.append("No terminal available either; logs are discarded.")
  return None, warnings


def start_run(planner_type: str) -> str | None:
  """Closes any active run handler and opens a new one for this run.

  Args:
    planner_type: Short planner identifier, e.g. "files", "exchange", "chat".

  Returns:
    Path of the new log file, or None if it could not be created (in which
    case records go to stderr, or are discarded if stderr is unavailable).
  """
  global _active_handler
  if not _PLANNER_TYPE_PATTERN.match(planner_type or ""):
    raise ValueError(f"Invalid planner type for log file name: {planner_type!r}")

  with _state_lock:
    old_handler = _active_handler
    new_handler, warnings = _create_run_handler(planner_type)

    # Attach the new handler before detaching the old one so no record is
    # dropped during the swap.
    if new_handler is not None:
      _logger.addHandler(new_handler)
      _logger.removeHandler(_null_handler)
    else:
      _logger.addHandler(_null_handler)
    if old_handler is not None:
      _logger.removeHandler(old_handler)
      old_handler.close()  # Never closes sys.stderr: StreamHandler leaves it open.
    _active_handler = new_handler

  # On fallback the active handler is stderr, so these reach the terminal.
  for warning in warnings:
    _logger.warning(warning)
  return getattr(new_handler, "path", None)


def end_run() -> None:
  """Closes the active run handler (if any). Subsequent records are discarded."""
  global _active_handler
  with _state_lock:
    if _active_handler is None:
      return
    # Re-attach the NullHandler first so logging never falls back to lastResort.
    _logger.addHandler(_null_handler)
    _logger.removeHandler(_active_handler)
    _active_handler.close()
    _active_handler = None


def current_log_path() -> str | None:
  """Returns the path of the active run file, or None if not logging to a file."""
  return getattr(_active_handler, "path", None)


def log(text: Any) -> None:
  """Logs a line at INFO to the active run. None is ignored; other values are str()-ed."""
  if text is None:
    return
  _logger.info(str(text))


def log_exception(message: str, exc_info: ExcInfo) -> None:
  """Logs a message at CRITICAL followed by the full traceback of the exception."""
  _logger.critical(message, exc_info=exc_info)


def export_to(dest_path: str) -> None:
  """Copies the active run file to dest_path.

  No module-level lock is needed: the handler reference is read once
  (atomic), and the copy is bounded by a size snapshot taken at a record
  boundary, so the export never ends in a half-written line even while other
  threads keep logging. If a new run starts mid-export, the previous run's
  (complete, closed) file is copied, which is still a valid log.
  This performs disk I/O; call it off the UI thread.

  Raises:
    FileNotFoundError: If there is no active run file.
  """
  handler = _active_handler
  if not isinstance(handler, _RunFileHandler):
    raise FileNotFoundError("No active log file to export.")

  remaining = handler.snapshot_size()
  with open(handler.path, "rb") as src, open(dest_path, "wb") as dest:
    while remaining > 0:
      chunk = src.read(min(_EXPORT_CHUNK_BYTES, remaining))
      if not chunk:
        break
      dest.write(chunk)
      remaining -= len(chunk)


def get_logger(feature: str | None = None) -> logging.Logger:
  """Returns the run logger, or a child of it for the given feature.

  Child loggers propagate into the run logger, so their records land in the
  active run file with the same format.
  """
  return _logger.getChild(feature) if feature else _logger


def install_exception_hooks() -> None:
  """Routes uncaught exceptions (main and worker threads) into the run file.

  Previously installed hooks (by default Python's own, which print the
  traceback to stderr) are still invoked afterwards, so crashes keep appearing
  in the terminal as before. Safe to call multiple times.
  """
  global _hooks_installed
  with _state_lock:
    if _hooks_installed:
      return
    _hooks_installed = True

  previous_sys_hook = sys.excepthook
  previous_thread_hook = threading.excepthook

  def _sys_hook(exc_type, exc_value, exc_traceback):
    if not issubclass(exc_type, KeyboardInterrupt):
      log_exception("Unhandled exception", (exc_type, exc_value, exc_traceback))
    # Hooks default to sys.__excepthook__, but third-party code may set None.
    if previous_sys_hook is not None:
      previous_sys_hook(exc_type, exc_value, exc_traceback)

  def _thread_hook(args):
    if args.exc_type is not SystemExit and args.exc_value is not None:
      thread_name = args.thread.name if args.thread else "unknown"
      log_exception(
          f"Unhandled exception in thread '{thread_name}'",
          (args.exc_type, args.exc_value, args.exc_traceback),
      )
    if previous_thread_hook is not None:
      previous_thread_hook(args)

  sys.excepthook = _sys_hook
  threading.excepthook = _thread_hook


atexit.register(end_run)
