# Copyright 2026 Google LLC
"""Selector app to choose between Exchange, Files and Chat migration planners."""

import os
import subprocess
import sys
import customtkinter as ctk
from util.constants import *

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

    self.options = ["Exchange", "Chat", "Files"]
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
      module_name = "scripts.exchange_online"
    elif selection == "Chat":
      module_name = "scripts.chats"
    elif selection == "Files":
      module_name = "scripts.files"
    else:
      return
    subprocess.Popen([sys.executable, "-m", module_name])
    self.destroy()


if __name__ == "__main__":
  app = SelectorApp()
  app.mainloop()
