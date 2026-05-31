# Copyright 2026 Google LLC
"""Selector app to choose between Exchange, Chat, and Telemetry migration planners."""

import os
import subprocess
import sys
import customtkinter as ctk

COLOR_BACKGROUND = "#F0F2F5"
COLOR_SURFACE = "#FFFFFF"
COLOR_PRIMARY = "#0B57D0"
COLOR_PRIMARY_HOVER = "#0842a0"
COLOR_TEXT_MAIN = "#1F1F1F"
FONT_HEADER_LARGE = ("Roboto", 24, "bold")
FONT_BODY_BOLD = ("Roboto", 14, "bold")


class SelectorApp(ctk.CTk):
  """Main application for workload selection."""

  def __init__(self):
    super().__init__()
    self.title("Migration Planner Selector")
    self.geometry("400x250")
    self.configure(fg_color=COLOR_BACKGROUND)

    ctk.CTkLabel(
        self,
        text="Select Workload",
        font=FONT_HEADER_LARGE,
        text_color=COLOR_TEXT_MAIN,
    ).pack(pady=(30, 20))

    self.options = ["Exchange", "Chat", "Telemetry"]
    self.combobox = ctk.CTkComboBox(
        self,
        values=self.options,
        width=250,
        height=40,
        corner_radius=20,
        font=FONT_BODY_BOLD,
    )
    self.combobox.set("Exchange")  # Set default
    self.combobox.pack(pady=10)

    self.btn_launch = ctk.CTkButton(
        self,
        text="Launch",
        width=250,
        height=40,
        corner_radius=20,
        font=FONT_BODY_BOLD,
        fg_color=COLOR_PRIMARY,
        hover_color=COLOR_PRIMARY_HOVER,
        command=self.launch_selected,
    )
    self.btn_launch.pack(pady=10)

  def launch_selected(self):
    selection = self.combobox.get()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if selection == "Exchange":
      script_path = os.path.join(current_dir, "migration_planner_exchange.py")
    elif selection == "Chat":
      script_path = os.path.join(current_dir, "migration_planner_chat.py")
    elif selection == "Telemetry":
      script_path = os.path.join(current_dir, "migration_planner_telemetry.py")
    else:
      return
    subprocess.Popen([sys.executable, script_path])

    # FIX: Force an immediate OS-level exit to prevent CustomTkinter 
    # from throwing 'after script' errors during its garbage collection phase.
    os._exit(0)


if __name__ == "__main__":
  app = SelectorApp()
  app.mainloop()
