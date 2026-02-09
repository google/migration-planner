# Copyright 2026 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import requests
import pandas as pd
import io
import threading
import time

class MigrationEstimatorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Migration ETA Estimator")
        self.root.geometry("600x750")

        # Styles
        style = ttk.Style()
        style.configure("TLabel", font=("Helvetica", 10))
        style.configure("TButton", font=("Helvetica", 10, "bold"))

        # --- Credentials Section ---
        creds_frame = ttk.LabelFrame(root, text="Azure App Credentials", padding=10)
        creds_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(creds_frame, text="Tenant ID:").grid(row=0, column=0, sticky="w", pady=2)
        self.tenant_id = ttk.Entry(creds_frame, width=40)
        self.tenant_id.grid(row=0, column=1, sticky="w", pady=2)

        ttk.Label(creds_frame, text="Client ID:").grid(row=1, column=0, sticky="w", pady=2)
        self.client_id = ttk.Entry(creds_frame, width=40)
        self.client_id.grid(row=1, column=1, sticky="w", pady=2)

        ttk.Label(creds_frame, text="Client Secret:").grid(row=2, column=0, sticky="w", pady=2)
        self.client_secret = ttk.Entry(creds_frame, width=40, show="*")
        self.client_secret.grid(row=2, column=1, sticky="w", pady=2)

        # --- Assumptions Section ---
        assump_frame = ttk.LabelFrame(root, text="Migration Assumptions", padding=10)
        assump_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(assump_frame, text="Avg Throughput (GB/Hour):").grid(row=0, column=0, sticky="w")
        self.speed_gb_hr = ttk.Entry(assump_frame, width=15)
        self.speed_gb_hr.insert(0, "10") # Default 10 GB/hr
        self.speed_gb_hr.grid(row=0, column=1, sticky="w", padx=5)

        ttk.Label(assump_frame, text="Avg Items/Second:").grid(row=0, column=2, sticky="w")
        self.speed_items_sec = ttk.Entry(assump_frame, width=15)
        self.speed_items_sec.insert(0, "15") # Default 15 items/sec
        self.speed_items_sec.grid(row=0, column=3, sticky="w", padx=5)

        # --- Controls ---
        btn_frame = ttk.Frame(root, padding=10)
        btn_frame.pack(fill="x")
        
        self.calc_btn = ttk.Button(btn_frame, text="Fetch Data & Calculate", command=self.start_calculation)
        self.calc_btn.pack(fill="x", ipady=5)

        # --- Output / Logs ---
        log_frame = ttk.LabelFrame(root, text="Logs & Status", padding=10)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, state='disabled', font=("Consolas", 9))
        self.log_text.pack(fill="both", expand=True)

        # --- Results Section ---
        res_frame = ttk.LabelFrame(root, text="Estimation Results", padding=10)
        res_frame.pack(fill="x", padx=10, pady=10)

        self.res_label_exch = ttk.Label(res_frame, text="Exchange: N/A")
        self.res_label_exch.pack(anchor="w")
 

        ttk.Separator(res_frame, orient='horizontal').pack(fill='x', pady=5)

        self.res_label_total = ttk.Label(res_frame, text="TOTAL SIZE: 0 GB | TOTAL ITEMS: 0", font=("Helvetica", 10, "bold"))
        self.res_label_total.pack(anchor="w")

        self.res_label_time = ttk.Label(res_frame, text="ESTIMATED MIGRATION TIME: -- hours", foreground="blue", font=("Helvetica", 11, "bold"))
        self.res_label_time.pack(anchor="w", pady=5)

    def log(self, message):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def start_calculation(self):
        # Validate inputs
        if not all([self.tenant_id.get(), self.client_id.get(), self.client_secret.get()]):
            messagebox.showerror("Error", "Please fill in all credential fields.")
            return

        # Run in thread to prevent freezing UI
        self.calc_btn.config(state='disabled')
        threading.Thread(target=self.run_process, daemon=True).start()

    def get_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id.get()}/oauth2/v2.0/token"
        data = {
            'client_id': self.client_id.get(),
            'scope': 'https://graph.microsoft.com/.default',
            'client_secret': self.client_secret.get(),
            'grant_type': 'client_credentials'
        }
        try:
            r = requests.post(url, data=data)
            r.raise_for_status()
            return r.json().get('access_token')
        except Exception as e:
            self.log(f"Error getting token: {e}")
            return None

    def get_report_data(self, token, report_endpoint):
        """Calls Graph API report endpoint and returns a DataFrame"""
        headers = {'Authorization': f'Bearer {token}'}
        # We use period='D7' to get the latest snapshot available
        url = f"https://graph.microsoft.com/v1.0/reports/{report_endpoint}(period='D7')"
        
        self.log(f"Requesting: {report_endpoint}...")
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            # The report API returns CSV data directly
            content = response.content.decode('utf-8')
            
            # Parse CSV
            df = pd.read_csv(io.StringIO(content))
            return df
        except Exception as e:
            self.log(f"Failed to fetch {report_endpoint}: {e}")
            return None

    def run_process(self):
        self.log("--- Starting Process ---")
        token = self.get_token()
        
        if not token:
            self.log("Authentication Failed. Stopping.")
            self.root.after(0, lambda: self.calc_btn.config(state='normal'))
            return

        self.log("Authentication Successful.")

        # 1. Fetch Exchange Data
        df_exch = self.get_report_data(token, "getMailboxUsageDetail")
        exch_items = 0
        exch_size_bytes = 0
        
        if df_exch is not None:
            # Graph CSV columns can vary, usually 'Item Count' and 'Storage Used (Byte)'
            # We clean column names to handle potential variations
            df_exch.columns = [c.strip().lower() for c in df_exch.columns]
            
            # Identify columns loosely
            item_col = next((c for c in df_exch.columns if 'item' in c and 'count' in c), None)
            size_col = next((c for c in df_exch.columns if 'storage' in c and 'byte' in c), None)

            if item_col and size_col:
                exch_items = df_exch[item_col].sum()
                exch_size_bytes = df_exch[size_col].sum()
                self.log(f"Exchange: {exch_items} items, {exch_size_bytes/1e9:.2f} GB")
            else:
                self.log("Exchange: Could not identify columns in CSV report.")

        # Calculations
        total_items = exch_items
        total_size_bytes = exch_size_bytes
        total_size_gb = total_size_bytes / (1024**3)

        try:
            throughput_gb_hr = float(self.speed_gb_hr.get())
            throughput_items_sec = float(self.speed_items_sec.get())
        except ValueError:
            throughput_gb_hr = 10.0
            throughput_items_sec = 15.0
            self.log("Invalid throughput inputs, using defaults.")

        # Logic: Calculate time based on Size and Time based on Item Count. Take the maximum.
        # Often migration is slow due to small file overhead (item count) rather than raw bandwidth.
        
        time_by_size = total_size_gb / throughput_gb_hr if throughput_gb_hr > 0 else 0
        
        items_per_hour = throughput_items_sec * 3600
        time_by_items = total_items / items_per_hour if items_per_hour > 0 else 0

        estimated_hours = max(time_by_size, time_by_items)

        # Update UI (Must be done on main thread usually, but simple text updates often work. 
        # Safest way is using after or thread-safe calls, but for this POC we update directly)
        self.root.after(0, self.update_results, 
                        exch_items, exch_size_bytes, 
                        total_items, total_size_gb, estimated_hours)

        self.root.after(0, lambda: self.calc_btn.config(state='normal'))
        self.log("--- Calculation Complete ---")

    def update_results(self, exch_i, exch_s, tot_i, tot_s_gb, est_hrs):
        def fmt_size(b): return f"{b/(1024**3):.2f} GB"
        
        self.res_label_exch.config(text=f"Exchange: {exch_i:,} items | {fmt_size(exch_s)}")
        
        self.res_label_total.config(text=f"TOTAL: {tot_s_gb:.2f} GB | {tot_i:,} Items")
        
        days = est_hrs / 24
        self.res_label_time.config(text=f"ESTIMATED TIME: {est_hrs:.1f} Hours (~{days:.1f} Days)")

if __name__ == "__main__":
    root = tk.Tk()
    app = MigrationEstimatorApp(root)
    root.mainloop()
