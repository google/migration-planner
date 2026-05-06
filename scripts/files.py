from ui.files_ui import FileMigrationEstimatorTool
import urllib3

def main():
  """Application entry point."""
  # Suppress SSL warnings for cleaner console output
  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

  # Configure High DPI scaling if necessary (Windows)
  try:
    from ctypes import windll

    windll.shcore.SetProcessDpiAwareness(1)
  except Exception:
    pass

  app = FileMigrationEstimatorTool()
  app.mainloop()


if __name__ == "__main__":
  main()