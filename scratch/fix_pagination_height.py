import os
import re

def fix_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Match left_spacer creation and packing
    content = re.sub(
        r'        left_spacer = ctk\.CTkFrame\([^,]+, fg_color="transparent"\)\n        left_spacer\.pack\(side="left", fill="x", expand=True\)\n',
        '',
        content
    )
    
    # Match right_spacer creation and packing
    content = re.sub(
        r'        right_spacer = ctk\.CTkFrame\([^,]+, fg_color="transparent"\)\n        right_spacer\.pack\(side="right", fill="x", expand=True\)\n',
        '',
        content
    )

    # Change center_container packing
    content = content.replace('center_container.pack(side="left")', 'center_container.pack(pady=0)')

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
