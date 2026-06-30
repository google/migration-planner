from util.constants import *

def build_configuration_view(self, ctk):
  """Builds the Configuration View."""
  self.view_config = ctk.CTkFrame(self.view_container, fg_color="transparent")
  self.view_config.grid_columnconfigure(0, weight=1)
  self.view_config.grid_rowconfigure(2, weight=1)

def build_header(self, ctk):
  # Header
  header_container = ctk.CTkFrame(self.view_config, fg_color="transparent")
  header_container.grid(row=0, column=0, sticky="w", padx=25, pady=(20, 5))
  ctk.CTkLabel(
      header_container,
      text="How would you like to provide data?",
      font=FONT_HEADER_MEDIUM,
      text_color=COLOR_TEXT_MAIN,
  ).pack(anchor="w")

def build_status_line(self, ctk):
  status_container = ctk.CTkFrame(self.view_config, fg_color="transparent")
  status_container.grid(row=1, column=0, sticky="w", padx=25, pady=(0, 20))
  ctk.CTkLabel(
      status_container,
      text="✔",
      text_color=COLOR_SUCCESS,
      font=FONT_BODY_MEDIUM,
  ).pack(side="left")
  ctk.CTkLabel(
      status_container,
      text=(
          " Data stays on your device. We never transmit credentials or data"
          " externally."
      ),
      font=FONT_BODY_MEDIUM,
      text_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=(5, 0))

def build_mail_input_frame(self, ctk):
  self.scroll_connect = ctk.CTkScrollableFrame(
    self.view_config,
    fg_color="transparent",
    scrollbar_button_color="white",
    scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
  )
  self.scroll_connect.grid(row=2, column=0, sticky="nsew", padx=15)

  ctk.CTkLabel(
      self.scroll_connect,
      text="Connect your Microsoft Azure account to fetch the data.",
      font=("Roboto", 13),
      text_color=COLOR_TEXT_SUB,
  ).pack(anchor="w", pady=(0, 15))

  # Input Container
  inputs_frame = ctk.CTkFrame(
      self.scroll_connect,
      fg_color=COLOR_SURFACE,
      border_color=COLOR_OUTLINE_LIGHT,
      border_width=1,
      corner_radius=8,
  )
  inputs_frame.pack(fill="x", pady=5)

  inner_pad = ctk.CTkFrame(inputs_frame, fg_color="transparent")
  inner_pad.pack(fill="x", padx=15, pady=15)

  self.create_entry(inner_pad, "Tenant ID", self.tenant_id)
  self.create_entry(inner_pad, "Client ID", self.client_ids)
  self.create_entry(inner_pad, "Client Secret", self.client_secrets, show="*")

def build_eo_resource_checkbox_list(self, ctk):
  ctk.CTkLabel(
      self.adv_frame,
      text="Scan Settings",
      font=FONT_BODY_BOLD,
      text_color=COLOR_TEXT_MAIN,
  ).pack(anchor="w", padx=15, pady=(10, 15))
  scan_options_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
  scan_options_frame.pack(fill="x", padx=15)
  ctk.CTkCheckBox(
      scan_options_frame,
      text="Contacts",
      variable=self.scan_contact,
      corner_radius=4,
      fg_color=COLOR_PRIMARY,
      border_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=10)
  ctk.CTkCheckBox(
      scan_options_frame,
      text="Calendars",
      variable=self.scan_calendar,
      corner_radius=4,
      fg_color=COLOR_PRIMARY,
      border_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=10)
  ctk.CTkCheckBox(
      scan_options_frame,
      text="Emails",
      variable=self.scan_email,
      corner_radius=4,
      fg_color=COLOR_PRIMARY,
      border_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=10)
  ctk.CTkCheckBox(
      scan_options_frame,
      text="In-Place Archives",
      variable=self.scan_in_place_archives,
      corner_radius=4,
      fg_color=COLOR_PRIMARY,
      border_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=10)
  ctk.CTkCheckBox(
      scan_options_frame,
      text="Shared Mails",
      variable=self.scan_shared_mail_boxes,
      corner_radius=4,
      fg_color=COLOR_PRIMARY,
      border_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=10)
  ctk.CTkCheckBox(
      scan_options_frame,
      text="Group Mailboxes",
      variable=self.scan_group_mail_boxes,
      corner_radius=4,
      fg_color=COLOR_PRIMARY,
      border_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=10)
  ctk.CTkLabel(
      self.adv_frame,
      text="* Only root contacts are scanned.",
      font=FONT_BODY_SMALL,
      text_color=COLOR_TEXT_SUB,
  ).pack(anchor="w", padx=25, pady=(2, 5))


def build_concurrency_settings_slider(self, ctk, useConcurrencyHeading=False):
  if useConcurrencyHeading:
    ctk.CTkLabel(
      self.adv_frame,
      text="Concurrency Settings",
      font=FONT_BODY_BOLD,
      text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=15, pady=(15, 5))

  concurrency_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
  concurrency_frame.pack(fill="x", padx=15)

  ctk.CTkLabel(
      concurrency_frame, text="Concurrency:", text_color=COLOR_TEXT_SUB
  ).grid(row=0, column=0, sticky="w", padx=5, pady=5)
  slider = ctk.CTkSlider(
      concurrency_frame,
      from_=10,
      to=100,
      number_of_steps=9,
      variable=self.concurrency,
  )
  slider.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
  ctk.CTkLabel(
      concurrency_frame,
      textvariable=self.concurrency,
      text_color=COLOR_TEXT_MAIN,
      width=30,
  ).grid(row=0, column=2, sticky="w", padx=5)
  ctk.CTkLabel(
      self.adv_frame,
      text=(
          "* Reduce concurrency if your CPU is slowing down or you see"
          " throttling errors in your logs."
      ),
      font=FONT_BODY_SMALL,
      text_color=COLOR_TEXT_SUB,
  ).pack(anchor="w", padx=25, pady=(2, 5))

  if SHOW_LOAD_MULTIPLIER:
    self.create_grid_entry(
        concurrency_frame, 1, 0, "Load Multiplier:", self.load_multiplier
    )

def build_migration_plan_options(self, ctk):
  ctk.CTkLabel(
    self.adv_frame,
    text="Migration Plan Options",
    font=FONT_BODY_BOLD,
    text_color=COLOR_TEXT_MAIN,
  ).pack(anchor="w", padx=15, pady=(15, 5))

  eta_settings_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
  eta_settings_frame.pack(fill="x", padx=15)

  ctk.CTkLabel(
      eta_settings_frame, text="Max Batches:", text_color=COLOR_TEXT_SUB
  ).grid(row=1, column=3, sticky="w", padx=5, pady=5)
  slider_max_batches = ctk.CTkSlider(
      eta_settings_frame,
      from_=10,
      to=100,
      number_of_steps=18,
      variable=self.eta_max_batches,
  )
  slider_max_batches.grid(row=1, column=4, sticky="ew", padx=5, pady=5)
  ctk.CTkLabel(
      eta_settings_frame,
      textvariable=self.eta_max_batches,
      text_color=COLOR_TEXT_MAIN,
      width=40,
  ).grid(row=1, column=5, sticky="w", padx=5)

  ctk.CTkLabel(
      self.adv_frame,
      text=(
          "* The migration plan will try to keep the total number of batches"
          " below this number."
      ),
      font=FONT_BODY_SMALL,
      text_color=COLOR_TEXT_SUB,
  ).pack(anchor="w", padx=25, pady=(2, 15))

def build_advanced_settings_frame(self, ctk):
  self.adv_frame = ctk.CTkFrame(
      self.scroll_connect, fg_color=COLOR_SURFACE_VARIANT, corner_radius=12
  )
  self.adv_visible = False
  self.btn_adv = ctk.CTkButton(
      self.scroll_connect,
      text="Show Advanced Settings ▼",
      command=self.toggle_adv,
      fg_color="transparent",
      text_color=COLOR_PRIMARY,
      hover=False,
      anchor="w",
  )
  self.btn_adv.pack(anchor="w", pady=(15, 5))

def build_file_distribution_bucket_ranges(self, ctk):
  """Builds input fields for file distribution bucket ranges."""
  ctk.CTkLabel(
      self.adv_frame,
      text="File Distribution Bucket Ranges",
      font=FONT_BODY_BOLD,
      text_color=COLOR_TEXT_MAIN,
  ).pack(anchor="w", padx=15, pady=(15, 2))

  ctk.CTkLabel(
      self.adv_frame,
      text="* Define file size ranges (in KB) to analyze file distribution.",
      font=FONT_BODY_SMALL,
      text_color=COLOR_TEXT_SUB,
  ).pack(anchor="w", padx=15, pady=(0, 5))

  ranges_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
  ranges_frame.pack(fill="x", padx=15)

  self.file_bucket_ranges = []

  def remove_row(frame, l_var, u_var):
    frame.destroy()
    if (l_var, u_var) in self.file_bucket_ranges:
      self.file_bucket_ranges.remove((l_var, u_var))

  def add_row(lower="", upper=""):
    row_frame = ctk.CTkFrame(ranges_frame, fg_color="transparent")
    row_frame.pack(fill="x", pady=2)

    lower_var = ctk.StringVar(value=lower)
    upper_var = ctk.StringVar(value=upper)

    def on_trace(var):
        val = var.get()
        if val.isdigit() or val == "INF" or val == "":
            return
        if "INF".startswith(val.upper()):
            var.set(val.upper())
        else:
            var.set("".join(c for c in val if c.isdigit()))

    lower_var.trace_add("write", lambda *args, v=lower_var: on_trace(v))
    upper_var.trace_add("write", lambda *args, v=upper_var: on_trace(v))

    lower_entry = ctk.CTkEntry(row_frame, placeholder_text="0", textvariable=lower_var, width=100, font=FONT_BODY_MEDIUM)
    lower_entry.pack(side="left", padx=5)
    ctk.CTkLabel(row_frame, text="KB", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(side="left", padx=2)

    ctk.CTkLabel(row_frame, text="-", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(side="left", padx=5)

    upper_entry = ctk.CTkEntry(row_frame, placeholder_text="100", textvariable=upper_var, width=100, font=FONT_BODY_MEDIUM)
    upper_entry.pack(side="left", padx=5)
    ctk.CTkLabel(row_frame, text="KB", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(side="left", padx=2)

    btn_remove = ctk.CTkButton(
        row_frame,
        text="🗑",
        width=30,
        fg_color="transparent",
        text_color=COLOR_ERROR,
        hover_color=COLOR_SECONDARY_HOVER,
        font=FONT_ICON_MEDIUM,
        command=lambda f=row_frame, l=lower_var, u=upper_var: remove_row(f, l, u)
    )
    btn_remove.pack(side="left", padx=10)

    self.file_bucket_ranges.append((lower_var, upper_var))

  # Default rows
  add_row("0", "100")
  add_row("101", "1000")
  add_row("1001", "INF")

  btn_add = ctk.CTkButton(
      self.adv_frame,
      text="Add Row",
      command=lambda: add_row("", ""),
      width=80,
      fg_color="transparent",
      border_width=1,
      border_color=COLOR_OUTLINE,
      text_color=COLOR_PRIMARY,
      hover_color=COLOR_SECONDARY_HOVER,
      font=FONT_BODY_MEDIUM,
  )
  btn_add.pack(anchor="w", padx=15, pady=(5, 15))

def build_large_resource_limit_input(self, ctk, min_val=2, max_val=10, increment=1):
  """Builds slider for lower count limit for large resources."""
  limit_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
  limit_frame.pack(fill="x", padx=15, pady=(15, 2))

  ctk.CTkLabel(
      limit_frame,
      text="Lower count limit for Large Resources:",
      font=FONT_BODY_BOLD,
      text_color=COLOR_TEXT_MAIN,
  ).pack(side="left", padx=5)

  self.large_resource_limit_var = ctk.IntVar(value=min_val)
  
  steps = (max_val - min_val) // increment
  
  slider = ctk.CTkSlider(
      limit_frame,
      from_=min_val,
      to=max_val,
      number_of_steps=steps,
      variable=self.large_resource_limit_var,
      width=200
  )
  slider.pack(side="left", padx=5)

  ctk.CTkLabel(
      limit_frame,
      textvariable=self.large_resource_limit_var,
      font=FONT_BODY_MEDIUM,
      text_color=COLOR_TEXT_MAIN,
      width=50
  ).pack(side="left", padx=5)

  ctk.CTkLabel(
      self.adv_frame,
      text="* Flag resources (like folders, subsites) with item counts greater than this limit.",
      font=FONT_BODY_SMALL,
      text_color=COLOR_TEXT_SUB,
  ).pack(anchor="w", padx=20, pady=(0, 15))