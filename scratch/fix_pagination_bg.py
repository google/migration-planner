import os
import re

def fix_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Match control_frame instantiation and change fg_color to COLOR_SURFACE
    content = re.sub(
        r'control_frame = ctk\.CTkFrame\(([^,]+), fg_color="transparent"\)',
        r'control_frame = ctk.CTkFrame(\1, fg_color=COLOR_SURFACE)',
        content
    )
    
    # Match control_frame.grid(...) and change pady to 0 (sometimes it's pady=(5, 10) or pady=10)
    # Be careful not to replace other things. We can find `control_frame.grid` specifically.
    content = re.sub(
        r'control_frame\.grid\(([^)]*?)pady=[^,)]+([^)]*)\)',
        r'control_frame.grid(\1pady=0\2)',
        content
    )

    # Match center_container.pack(...) and add pady=(5, 10)
    # Currently it's center_container.pack(pady=0) from the previous script
    content = re.sub(
        r'center_container\.pack\(pady=0\)',
        r'center_container.pack(pady=(5, 10))',
        content
    )

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

files_to_fix = [
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/active_users_usage.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/devices_apps_telemetry.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/intune_policies.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/exchange_connectors_ui.py"
]

for fp in files_to_fix:
    fix_file(fp)
    print(f"Fixed {fp}")
